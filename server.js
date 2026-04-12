/**
 * NSM PDF Renderer — Standalone Puppeteer Rendering Microservice
 *
 * Accepts an S3 HTML URL and renders it to PDF using headless Chromium.
 * Designed for files > 50MB that exceed Cloudflare Worker limits.
 *
 * Supports the same page sizing logic as the Cloudflare Worker:
 * - Auto-detect paper size from HTML content (viewport meta, body width, @page CSS)
 * - Accept explicit width/height/margins from the caller
 * - Return sizing metadata for the caller to persist
 *
 * S3 Upload: Uses POST + multipart/form-data (matching the Forge storage API).
 *
 * Endpoints:
 *   POST /render   — Render HTML URL to PDF, upload to S3
 *   GET  /health   — Health check
 *
 * Environment variables:
 *   RENDER_SECRET       — Shared secret for request authentication
 *   PORT                — Server port (default 3001)
 *   MAX_CONCURRENT      — Max simultaneous renders (default 1)
 *   BROWSER_RESTART_EVERY — Restart Chromium after N renders (default 5)
 */

const express = require("express");
const puppeteer = require("puppeteer");

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = process.env.PORT || 3001;
const RENDER_SECRET = process.env.RENDER_SECRET || "";
const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT || "1", 10);
const BROWSER_RESTART_EVERY = parseInt(process.env.BROWSER_RESTART_EVERY || "5", 10);

// ── Standard paper sizes (in CSS pixels at 96 DPI) ────────────────────────
const PAPER_SIZES = {
  A4:     { width: 794, height: 1123, ptWidth: 595, ptHeight: 842, label: "A4" },
  A3:     { width: 1123, height: 1587, ptWidth: 842, ptHeight: 1191, label: "A3" },
  Letter: { width: 816, height: 1056, ptWidth: 612, ptHeight: 792, label: "Letter" },
  Legal:  { width: 816, height: 1344, ptWidth: 612, ptHeight: 1008, label: "Legal" },
  Tabloid:{ width: 1056, height: 1632, ptWidth: 792, ptHeight: 1224, label: "Tabloid" },
};

// ── Browser management ──────────────────────────────────────────────────────

let browserInstance = null;
let renderCount = 0;

async function getBrowser() {
  // Restart browser periodically to reclaim memory
  if (browserInstance && renderCount > 0 && renderCount % BROWSER_RESTART_EVERY === 0) {
    console.log(`[Browser] Restarting after ${renderCount} renders for memory hygiene`);
    await browserInstance.close().catch(() => {});
    browserInstance = null;
  }

  if (browserInstance && browserInstance.connected) {
    return browserInstance;
  }

  console.log("[Browser] Launching new Chromium instance...");
  browserInstance = await puppeteer.launch({
    headless: true,
    args: [
      "--no-sandbox",
      "--disable-setuid-sandbox",
      "--disable-dev-shm-usage",
      "--disable-gpu",
      "--disable-web-security",
      "--font-render-hinting=none",
      "--js-flags=--max-old-space-size=2048",
    ],
  });

  browserInstance.on("disconnected", () => {
    console.log("[Browser] Disconnected — will relaunch on next request");
    browserInstance = null;
  });

  console.log("[Browser] Chromium launched successfully");
  return browserInstance;
}

// ── Concurrency semaphore ───────────────────────────────────────────────────

let activeRenders = 0;
const waitQueue = [];

function acquireSlot() {
  return new Promise((resolve) => {
    if (activeRenders < MAX_CONCURRENT) {
      activeRenders++;
      resolve();
    } else {
      waitQueue.push(resolve);
    }
  });
}

function releaseSlot() {
  activeRenders--;
  if (waitQueue.length > 0) {
    activeRenders++;
    const next = waitQueue.shift();
    next();
  }
}

// ── S3 Upload helper ────────────────────────────────────────────────────────
// The Forge storage API expects POST + multipart/form-data, NOT PUT + raw body.
// This matches the main server's storagePut() implementation.

async function uploadToS3(pdfBuffer, storageUploadUrl, storageApiKey) {
  // Build multipart form data with the PDF as a file field
  const blob = new Blob([pdfBuffer], { type: "application/pdf" });
  const form = new FormData();
  form.append("file", blob, `render-${Date.now()}.pdf`);

  const uploadRes = await fetch(storageUploadUrl, {
    method: "POST",
    headers: {
      Authorization: `Bearer ${storageApiKey}`,
      // Do NOT set Content-Type — fetch will auto-set it with the boundary for FormData
    },
    body: form,
  });

  if (!uploadRes.ok) {
    const errText = await uploadRes.text().catch(() => "(no body)");
    throw new Error(`S3 upload failed (${uploadRes.status}): ${errText.substring(0, 200)}`);
  }

  const uploadResult = await uploadRes.json();
  return uploadResult;
}

// ── Page sizing detection ──────────────────────────────────────────────────

/**
 * Detect the optimal paper size from the loaded HTML page.
 * Checks: viewport meta, body/root computed width, @page CSS rules.
 */
async function detectPageSizing(page) {
  const sizing = await page.evaluate(() => {
    const result = {
      bodyWidth: 0,
      bodyHeight: 0,
      viewportMetaWidth: null,
      pageRuleWidth: null,
      pageRuleHeight: null,
    };

    // Check viewport meta tag
    const viewportMeta = document.querySelector('meta[name="viewport"]');
    if (viewportMeta) {
      const content = viewportMeta.getAttribute("content") || "";
      const widthMatch = content.match(/width=(\d+)/);
      if (widthMatch) result.viewportMetaWidth = parseInt(widthMatch[1], 10);
    }

    // Check body/root computed width
    const body = document.body;
    if (body) {
      result.bodyWidth = body.scrollWidth;
      result.bodyHeight = body.scrollHeight;
    }

    // Check @page CSS rules
    for (const sheet of document.styleSheets) {
      try {
        for (const rule of sheet.cssRules) {
          if (rule.type === CSSRule.PAGE_RULE) {
            const style = rule.style;
            if (style.width) result.pageRuleWidth = style.width;
            if (style.height) result.pageRuleHeight = style.height;
            if (style.size) {
              // Parse @page { size: A4 landscape } or { size: 210mm 297mm }
              const sizeVal = style.size.toLowerCase().trim();
              if (sizeVal.includes("a3")) result.pageRuleWidth = "A3";
              else if (sizeVal.includes("a4")) result.pageRuleWidth = "A4";
              else if (sizeVal.includes("letter")) result.pageRuleWidth = "Letter";
              else if (sizeVal.includes("legal")) result.pageRuleWidth = "Legal";
            }
          }
        }
      } catch (e) {
        // Cross-origin stylesheet — skip
      }
    }

    return result;
  });

  return sizing;
}

/**
 * Match detected dimensions to the closest standard paper size.
 * Returns { format, width, height, ptWidth, ptHeight, scale, label }
 */
function matchPaperSize(sizing) {
  // If @page CSS specifies a named size, use it directly
  if (sizing.pageRuleWidth) {
    const namedSize = String(sizing.pageRuleWidth).replace(/[^a-zA-Z]/g, "");
    for (const [key, paper] of Object.entries(PAPER_SIZES)) {
      if (namedSize.toLowerCase() === key.toLowerCase()) {
        return { ...paper, scale: 1, matched: key };
      }
    }
  }

  // Use body width to find the best match
  const contentWidth = sizing.viewportMetaWidth || sizing.bodyWidth || 794;

  // Find closest paper size by width
  let bestMatch = PAPER_SIZES.A4;
  let bestKey = "A4";
  let bestDiff = Math.abs(contentWidth - bestMatch.width);

  for (const [key, paper] of Object.entries(PAPER_SIZES)) {
    const diff = Math.abs(contentWidth - paper.width);
    if (diff < bestDiff) {
      bestDiff = diff;
      bestMatch = paper;
      bestKey = key;
    }
  }

  // If content is wider than any standard size, calculate scale
  let scale = 1;
  if (contentWidth > bestMatch.width * 1.1) {
    // Content is significantly wider — scale down to fit
    scale = bestMatch.width / contentWidth;
    scale = Math.max(0.3, Math.min(1, scale)); // Clamp between 0.3 and 1
  }

  return { ...bestMatch, scale, matched: bestKey };
}

// ── Render function ─────────────────────────────────────────────────────────

async function renderHtmlToPdf(s3HtmlUrl, options = {}) {
  const {
    timeoutMs = 5 * 60 * 1000,
    // Explicit overrides (caller can force specific sizing)
    paperWidth = null,     // in inches, e.g. 8.27 for A4
    paperHeight = null,    // in inches, e.g. 11.69 for A4
    paperFormat = null,    // "A4", "Letter", etc. — used only if width/height not set
    scale = null,          // explicit scale override
    viewportWidth = null,  // explicit viewport width override
    margins = null,        // { top, right, bottom, left } in CSS units
    autoDetect = true,     // whether to auto-detect sizing from HTML content
  } = options;

  const browser = await getBrowser();
  const page = await browser.newPage();
  const startTime = Date.now();

  try {
    // Set initial viewport — use override or default desktop width
    const vw = viewportWidth || 1280;
    await page.setViewport({ width: vw, height: 1024 });

    // Block unnecessary resources to speed up loading
    await page.setRequestInterception(true);
    page.on("request", (req) => {
      const type = req.resourceType();
      if (type === "media" || type === "websocket") {
        req.abort();
      } else {
        req.continue();
      }
    });

    console.log(`[Render] Navigating to ${s3HtmlUrl.substring(0, 100)}...`);

    // Navigate to the S3-hosted HTML
    await page.goto(s3HtmlUrl, {
      waitUntil: "networkidle0",
      timeout: timeoutMs,
    });

    // Wait for document to be fully ready
    await page.waitForFunction(
      () => document.readyState === "complete",
      { timeout: 30000 }
    );

    // Small delay for any deferred JS rendering
    await new Promise((r) => setTimeout(r, 2000));

    const loadTimeMs = Date.now() - startTime;
    console.log(`[Render] Page loaded in ${loadTimeMs}ms, detecting sizing...`);

    // ── Sizing detection ──────────────────────────────────────────────
    let pdfOptions = {
      printBackground: true,
      timeout: timeoutMs,
    };

    let sizingMeta = null;

    if (paperWidth && paperHeight) {
      // Explicit dimensions provided by caller (in inches)
      pdfOptions.width = `${paperWidth}in`;
      pdfOptions.height = `${paperHeight}in`;
      pdfOptions.scale = scale || 1;
      sizingMeta = {
        firstPageCssWidth: Math.round(paperWidth * 96),
        firstPageCssHeight: Math.round(paperHeight * 96),
        inferredDpi: 96,
        appliedScale: pdfOptions.scale,
        finalPdfWidthPt: Math.round(paperWidth * 72),
        finalPdfHeightPt: Math.round(paperHeight * 72),
        sizingMatchedPaper: "custom",
      };
    } else if (autoDetect) {
      // Auto-detect from HTML content
      const rawSizing = await detectPageSizing(page);
      const matched = matchPaperSize(rawSizing);

      console.log(`[Render] Sizing detected: body=${rawSizing.bodyWidth}x${rawSizing.bodyHeight}, matched=${matched.matched}, scale=${matched.scale.toFixed(2)}`);

      // If viewport meta suggests a wider layout, resize viewport and re-render
      if (rawSizing.viewportMetaWidth && rawSizing.viewportMetaWidth > vw) {
        console.log(`[Render] Resizing viewport to ${rawSizing.viewportMetaWidth}px to match content`);
        await page.setViewport({ width: rawSizing.viewportMetaWidth, height: 1024 });
        await new Promise((r) => setTimeout(r, 1000)); // Let layout settle
      }

      pdfOptions.format = paperFormat || matched.matched;
      pdfOptions.scale = scale || matched.scale;

      sizingMeta = {
        firstPageCssWidth: matched.width,
        firstPageCssHeight: matched.height,
        inferredDpi: 96,
        appliedScale: pdfOptions.scale,
        finalPdfWidthPt: matched.ptWidth,
        finalPdfHeightPt: matched.ptHeight,
        sizingMatchedPaper: matched.matched,
      };
    } else {
      // No detection, use format or default to A4
      pdfOptions.format = paperFormat || "A4";
      pdfOptions.scale = scale || 1;
      const paper = PAPER_SIZES[pdfOptions.format] || PAPER_SIZES.A4;
      sizingMeta = {
        firstPageCssWidth: paper.width,
        firstPageCssHeight: paper.height,
        inferredDpi: 96,
        appliedScale: pdfOptions.scale,
        finalPdfWidthPt: paper.ptWidth,
        finalPdfHeightPt: paper.ptHeight,
        sizingMatchedPaper: pdfOptions.format,
      };
    }

    // Apply margins
    if (margins) {
      pdfOptions.margin = margins;
    } else {
      pdfOptions.margin = { top: "10mm", right: "10mm", bottom: "10mm", left: "10mm" };
    }

    console.log(`[Render] Generating PDF: format=${pdfOptions.format || 'custom'}, scale=${pdfOptions.scale}, margins=${JSON.stringify(pdfOptions.margin)}`);

    // ── PDF generation with fallback for very large files ──────────────
    // Puppeteer's page.pdf() can crash with "Protocol error (Page.printToPDF):
    // Printing failed" on very large HTML documents (60MB+). When this happens,
    // we retry with a smaller scale factor, then fall back to screenshot-based
    // PDF generation as a last resort.
    let pdfBuffer;
    let usedFallback = false;

    try {
      pdfBuffer = await page.pdf(pdfOptions);
    } catch (printErr) {
      console.warn(`[Render] page.pdf() failed: ${printErr.message}`);

      // Retry 1: Try with a smaller scale to reduce output complexity
      if (pdfOptions.scale > 0.5) {
        console.log(`[Render] Retrying with reduced scale (0.5)...`);
        try {
          pdfBuffer = await page.pdf({ ...pdfOptions, scale: 0.5 });
          if (sizingMeta) sizingMeta.appliedScale = 0.5;
          console.log(`[Render] Retry with scale=0.5 succeeded`);
        } catch (retryErr) {
          console.warn(`[Render] Retry with scale=0.5 also failed: ${retryErr.message}`);
        }
      }

      // Retry 2: Try with minimal options (no format, explicit small dimensions)
      if (!pdfBuffer) {
        console.log(`[Render] Retrying with minimal A4 options...`);
        try {
          pdfBuffer = await page.pdf({
            width: "8.27in",
            height: "11.69in",
            scale: 0.4,
            printBackground: true,
            timeout: timeoutMs,
            margin: { top: "5mm", right: "5mm", bottom: "5mm", left: "5mm" },
          });
          if (sizingMeta) {
            sizingMeta.appliedScale = 0.4;
            sizingMeta.sizingMatchedPaper = "A4-fallback";
          }
          usedFallback = true;
          console.log(`[Render] Minimal A4 fallback succeeded`);
        } catch (minimalErr) {
          console.warn(`[Render] Minimal A4 fallback also failed: ${minimalErr.message}`);
        }
      }

      // Retry 3: Screenshot-based fallback — take full-page screenshot and wrap in PDF
      if (!pdfBuffer) {
        console.log(`[Render] All page.pdf() attempts failed. Using screenshot-based fallback...`);
        try {
          const screenshotBuffer = await page.screenshot({
            fullPage: true,
            type: "png",
          });

          // Create a minimal PDF wrapper around the screenshot
          // We'll use a simple approach: render a page with just the image
          const imgPage = await browser.newPage();
          try {
            const base64 = Buffer.from(screenshotBuffer).toString("base64");
            const imgHtml = `<!DOCTYPE html><html><head><style>
              * { margin: 0; padding: 0; }
              body { width: 100vw; }
              img { width: 100%; display: block; }
            </style></head><body>
              <img src="data:image/png;base64,${base64}" />
            </body></html>`;

            await imgPage.setContent(imgHtml, { waitUntil: "load" });
            pdfBuffer = await imgPage.pdf({
              width: "8.27in",
              printBackground: true,
              margin: { top: "0", right: "0", bottom: "0", left: "0" },
            });
            usedFallback = true;
            if (sizingMeta) {
              sizingMeta.appliedScale = 1;
              sizingMeta.sizingMatchedPaper = "screenshot-fallback";
            }
            console.log(`[Render] Screenshot-based fallback succeeded: ${(pdfBuffer.length / 1_000_000).toFixed(1)}MB`);
          } finally {
            await imgPage.close().catch(() => {});
          }
        } catch (ssErr) {
          // All fallbacks exhausted — re-throw the original error
          throw new Error(`All PDF generation methods failed. Original: ${printErr.message}. Screenshot: ${ssErr.message}`);
        }
      }
    }

    // Detect page count from pdf2htmlEX format
    const pageCount = await page.evaluate(() => {
      const pages = document.querySelectorAll("[data-page-no]");
      return pages.length > 0 ? pages.length : 0;
    }).catch(() => 0);

    const renderTimeMs = Date.now() - startTime;
    console.log(`[Render] PDF generated: ${(pdfBuffer.length / 1_000_000).toFixed(1)}MB, ${pageCount} pages, ${renderTimeMs}ms${usedFallback ? " (fallback)" : ""}`);

    renderCount++;

    return {
      pdfBuffer: Buffer.from(pdfBuffer),
      renderTimeMs,
      pageCount,
      pdfSizeBytes: pdfBuffer.length,
      sizingMeta,
      usedFallback,
    };
  } finally {
    await page.close().catch(() => {});

    // Proactive memory check
    const used = process.memoryUsage();
    const heapMb = Math.round(used.heapUsed / 1024 / 1024);
    const rssMb = Math.round(used.rss / 1024 / 1024);
    console.log(`[Memory] Heap: ${heapMb}MB, RSS: ${rssMb}MB`);

    if (rssMb > 1500) {
      console.log("[Memory] RSS > 1500MB — forcing browser restart");
      await browserInstance?.close().catch(() => {});
      browserInstance = null;
    }
  }
}


// ── Auth middleware ──────────────────────────────────────────────────────────

function authMiddleware(req, res, next) {
  if (!RENDER_SECRET) {
    // No secret configured — allow all requests (dev mode)
    return next();
  }

  const authHeader = req.headers.authorization;
  if (!authHeader || authHeader !== `Bearer ${RENDER_SECRET}`) {
    return res.status(401).json({ success: false, error: "Unauthorized" });
  }
  next();
}

// ── Routes ──────────────────────────────────────────────────────────────────

app.get("/health", (req, res) => {
  res.json({
    status: "ok",
    browserConnected: browserInstance?.connected ?? false,
    activeRenders,
    queuedRenders: waitQueue.length,
    totalRenders: renderCount,
    memoryMb: Math.round(process.memoryUsage().rss / 1024 / 1024),
  });
});

app.post("/render", authMiddleware, async (req, res) => {
  const { url, storageUploadUrl, storageApiKey, options } = req.body;

  if (!url) {
    return res.status(400).json({ success: false, error: "Missing 'url' (S3 HTML URL)" });
  }
  if (!storageUploadUrl || !storageApiKey) {
    return res.status(400).json({ success: false, error: "Missing 'storageUploadUrl' or 'storageApiKey'" });
  }

  console.log(`[Request] Render: ${url.substring(0, 100)} | Queue: ${waitQueue.length} | Active: ${activeRenders}/${MAX_CONCURRENT}`);

  try {
    // Wait for a concurrency slot
    const queueStart = Date.now();
    await acquireSlot();
    const queueTimeMs = Date.now() - queueStart;
    if (queueTimeMs > 1000) {
      console.log(`[Request] Waited ${queueTimeMs}ms for concurrency slot`);
    }

    try {
      // Render HTML to PDF
      const result = await renderHtmlToPdf(url, options || {});

      // Upload PDF to S3 using POST + FormData (matching Forge storage API)
      const uploadResult = await uploadToS3(result.pdfBuffer, storageUploadUrl, storageApiKey);

      res.json({
        success: true,
        pdfUrl: uploadResult.url,
        pdfSizeBytes: result.pdfSizeBytes,
        renderTimeMs: result.renderTimeMs,
        pageCount: result.pageCount,
        queueTimeMs,
        renderPath: "server-side-puppeteer",
        sizingMeta: result.sizingMeta,
        usedFallback: result.usedFallback || false,
      });
    } finally {
      releaseSlot();
    }
  } catch (err) {
    console.error(`[Error] Render failed: ${err.message}`);
    res.status(500).json({
      success: false,
      error: err.message,
      renderPath: "server-side-puppeteer",
    });
  }
});

// ── Start server ────────────────────────────────────────────────────────────

app.listen(PORT, () => {
  console.log(`NSM PDF Renderer listening on port ${PORT}`);
  console.log(`  MAX_CONCURRENT: ${MAX_CONCURRENT}`);
  console.log(`  BROWSER_RESTART_EVERY: ${BROWSER_RESTART_EVERY}`);
  console.log(`  Auth: ${RENDER_SECRET ? "enabled" : "disabled (dev mode)"}`);

  // Pre-launch browser so first request is fast
  getBrowser().catch((err) => {
    console.error(`[Browser] Failed to pre-launch: ${err.message}`);
  });
});
