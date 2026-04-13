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
 * Large file handling (v4):
 * - HEAD pre-check to determine file size before loading into Puppeteer
 * - Files > 30MB use waitUntil: "domcontentloaded" instead of "networkidle0"
 *   (networkidle0 waits for ALL network activity to stop, which may never happen
 *    for huge files with many inline resources)
 * - Pre-render memory check: if RSS is already high, restart browser first
 * - Chromium launched with memory-conservative flags
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

// Runtime Node.js version check — Blob, FormData, and fetch are globals only in Node 18+
const [nodeMajor] = process.versions.node.split(".").map(Number);
if (nodeMajor < 18) {
  console.error(`[FATAL] Node.js >= 18 required (current: ${process.versions.node}). Blob, FormData, and fetch are not available in older versions.`);
  process.exit(1);
}

const express = require("express");
const puppeteer = require("puppeteer");

const app = express();
app.use(express.json({ limit: "1mb" }));

const PORT = process.env.PORT || 3001;
const RENDER_SECRET = process.env.RENDER_SECRET || "";
const MAX_CONCURRENT = parseInt(process.env.MAX_CONCURRENT || "1", 10);
const BROWSER_RESTART_EVERY = parseInt(process.env.BROWSER_RESTART_EVERY || "5", 10);

// ── Large file threshold ─────────────────────────────────────────────────────
// Files above this size use domcontentloaded instead of networkidle0
const LARGE_FILE_THRESHOLD = 30_000_000; // 30MB

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
      "--disable-extensions",
      "--disable-background-networking",
      "--disable-default-apps",
      "--disable-sync",
      "--disable-translate",
      "--metrics-recording-only",
      "--no-first-run",
      "--js-flags=--max-old-space-size=3072",
    ],
  });

  browserInstance.on("disconnected", () => {
    console.log("[Browser] Disconnected — will relaunch on next request");
    browserInstance = null;
  });

  console.log("[Browser] Chromium launched successfully");
  return browserInstance;
}

/**
 * Force-restart the browser to reclaim memory before a large render.
 * Only restarts if RSS is above the threshold.
 */
async function ensureFreshBrowserForLargeFile() {
  const rssMb = Math.round(process.memoryUsage().rss / 1024 / 1024);
  if (rssMb > 600) {
    console.log(`[Memory] RSS ${rssMb}MB > 600MB threshold — restarting browser before large file render`);
    if (browserInstance) {
      await browserInstance.close().catch(() => {});
      browserInstance = null;
    }
    // Force garbage collection if available
    if (global.gc) {
      global.gc();
      console.log(`[Memory] Forced GC. RSS now: ${Math.round(process.memoryUsage().rss / 1024 / 1024)}MB`);
    }
  } else {
    console.log(`[Memory] RSS ${rssMb}MB — OK for large file render`);
  }
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

// ── File size pre-check ─────────────────────────────────────────────────────

/**
 * Perform a HEAD request on the S3 URL to determine the file size.
 * Returns the size in bytes, or null if the HEAD request fails.
 */
async function getFileSize(url) {
  try {
    const headRes = await fetch(url, {
      method: "HEAD",
      signal: AbortSignal.timeout(10000),
    });
    if (headRes.ok) {
      const cl = headRes.headers.get("content-length");
      if (cl) {
        const size = parseInt(cl, 10);
        console.log(`[Render] File size from HEAD: ${(size / 1_000_000).toFixed(1)}MB`);
        return size;
      }
    }
    return null;
  } catch (err) {
    console.warn(`[Render] HEAD request failed: ${err.message}`);
    return null;
  }
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
              else if (sizeVal.includes("tabloid") || sizeVal.includes("ledger")) result.pageRuleWidth = "Tabloid";
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

  // ── Step 0: Pre-check file size ────────────────────────────────────────────
  const fileSize = await getFileSize(s3HtmlUrl);
  const isLargeFile = fileSize && fileSize > LARGE_FILE_THRESHOLD;

  if (isLargeFile) {
    console.log(`[Render] Large file detected: ${(fileSize / 1_000_000).toFixed(0)}MB — using memory-conservative mode`);
    // Ensure browser has enough memory headroom for large files
    await ensureFreshBrowserForLargeFile();
  }

  const browser = await getBrowser();
  const page = await browser.newPage();
  const startTime = Date.now();

  try {
    // Set initial viewport — use override or default desktop width
    const vw = viewportWidth || 1280;
    await page.setViewport({ width: vw, height: 1024 });

    // Block unnecessary resources to speed up loading and save memory
    await page.setRequestInterception(true);
    page.on("request", (req) => {
      const type = req.resourceType();
      // For large files, also block images and fonts to reduce memory usage during initial load
      if (isLargeFile) {
        if (type === "media" || type === "websocket" || type === "image" || type === "font") {
          req.abort();
        } else {
          req.continue();
        }
      } else {
        if (type === "media" || type === "websocket") {
          req.abort();
        } else {
          req.continue();
        }
      }
    });

    console.log(`[Render] Navigating to ${s3HtmlUrl.substring(0, 100)}...`);

    // Navigate to the S3-hosted HTML
    // For large files, use "domcontentloaded" instead of "networkidle0".
    // networkidle0 waits until there are no more than 0 network connections for 500ms,
    // which may never happen for huge files with many inline base64 resources that
    // trigger secondary parsing. domcontentloaded fires as soon as the HTML is parsed.
    const waitStrategy = isLargeFile ? "domcontentloaded" : "networkidle0";
    console.log(`[Render] Using waitUntil: "${waitStrategy}" (file ${isLargeFile ? `${(fileSize / 1_000_000).toFixed(0)}MB > ${LARGE_FILE_THRESHOLD / 1_000_000}MB threshold` : "normal size"})`);

    await page.goto(s3HtmlUrl, {
      waitUntil: waitStrategy,
      timeout: timeoutMs,
    });

    // Wait for document to be fully ready
    await page.waitForFunction(
      () => document.readyState === "complete",
      { timeout: isLargeFile ? 120000 : 30000 } // 2 min for large files, 30s for normal
    );

    // Delay for deferred JS rendering — longer for large files
    const settleDelay = isLargeFile ? 5000 : 2000;
    await new Promise((r) => setTimeout(r, settleDelay));

    const loadTimeMs = Date.now() - startTime;
    console.log(`[Render] Page loaded in ${loadTimeMs}ms, detecting format...`);

    // Log memory after page load
    const postLoadMem = process.memoryUsage();
    console.log(`[Memory] After page load: Heap=${Math.round(postLoadMem.heapUsed / 1024 / 1024)}MB, RSS=${Math.round(postLoadMem.rss / 1024 / 1024)}MB`);

    // ── pdf2htmlEX detection and CSS injection ──────────────────────────
    // pdf2htmlEX HTML uses #page-container { position: absolute }, which means
    // the content doesn't contribute to body height. Puppeteer's page.pdf()
    // only prints content in normal document flow, resulting in blank PDFs.
    // Fix: detect pdf2htmlEX format and inject CSS to bring content into flow.
    const isPdf2HtmlEx = await page.evaluate(() => {
      // Check for pdf2htmlEX markers
      const hasPageContainer = !!document.getElementById('page-container');
      const hasPfPages = document.querySelectorAll('.pf').length > 0;
      const hasGenerator = document.documentElement.innerHTML.includes('pdf2htmlEX') ||
                           document.documentElement.innerHTML.includes('pdf2htmlex');
      return hasPageContainer || (hasPfPages && hasGenerator);
    }).catch(() => false);

    if (isPdf2HtmlEx) {
      console.log(`[Render] pdf2htmlEX format detected — injecting print-friendly CSS`);

      await page.evaluate(() => {
        const style = document.createElement('style');
        style.textContent = `
          /* Bring page-container into normal document flow */
          #page-container {
            position: relative !important;
            left: 0 !important;
            top: 0 !important;
            margin: 0 auto !important;
            overflow: visible !important;
            display: block !important;
          }

          /* Hide sidebar/navigation elements that pdf2htmlEX adds */
          #sidebar, #outline, .loading-indicator, #loading-css {
            display: none !important;
          }

          /* Ensure body and html have proper dimensions */
          html, body {
            overflow: visible !important;
            height: auto !important;
            width: auto !important;
            min-height: 100vh !important;
          }

          /* Each page should break properly for printing */
          .pf {
            position: relative !important;
            overflow: visible !important;
            page-break-after: always !important;
            page-break-inside: avoid !important;
            margin: 0 auto !important;
            display: block !important;
            content-visibility: visible !important;
          }

          /* Override content-visibility: auto which causes blank rendering in headless browsers */
          [style*="content-visibility"] {
            content-visibility: visible !important;
          }
          * {
            content-visibility: visible !important;
          }

          /* Remove any transforms that might hide content */
          .pc {
            position: relative !important;
          }

          /* Force ALL absolutely-positioned direct children of body into flow */
          body > div[style*="position: absolute"],
          body > div[style*="position:absolute"] {
            position: relative !important;
          }

          @media print {
            #page-container {
              position: relative !important;
              left: 0 !important;
            }
            .pf {
              box-shadow: none !important;
              border: none !important;
            }
          }
        `;
        document.head.appendChild(style);

        // Also force inline styles on key elements (CSS !important may not
        // override inline styles in all browsers)
        const pc = document.getElementById('page-container');
        if (pc) {
          pc.style.setProperty('position', 'relative', 'important');
          pc.style.setProperty('left', '0', 'important');
          pc.style.setProperty('top', '0', 'important');
          pc.style.setProperty('overflow', 'visible', 'important');
        }
        document.querySelectorAll('.pf').forEach(el => {
          el.style.setProperty('position', 'relative', 'important');
          el.style.setProperty('overflow', 'visible', 'important');
          el.style.setProperty('content-visibility', 'visible', 'important');
        });
      });

      // Wait for layout to settle after CSS injection
      await new Promise((r) => setTimeout(r, 1500));

      // Force reflow
      await page.evaluate(() => {
        void document.body.offsetHeight;
        void document.documentElement.offsetHeight;
      });

      // Verify content is now visible
      const postFixHeight = await page.evaluate(() => document.body.scrollHeight);
      console.log(`[Render] After CSS injection: body height = ${postFixHeight}px`);

      if (postFixHeight === 0) {
        console.warn(`[Render] Body still has zero height after CSS injection`);
        // Try to find the actual content dimensions from page-container or .pf elements
        const contentDims = await page.evaluate(() => {
          const pc = document.getElementById('page-container');
          if (pc) {
            const rect = pc.getBoundingClientRect();
            if (rect.height > 0) return { width: rect.width, height: rect.height, source: 'page-container' };
          }
          // Check total height of all .pf pages
          const pages = document.querySelectorAll('.pf');
          if (pages.length > 0) {
            let totalH = 0;
            let maxW = 0;
            pages.forEach(p => {
              const r = p.getBoundingClientRect();
              totalH += r.height;
              if (r.width > maxW) maxW = r.width;
            });
            if (totalH > 0) return { width: maxW, height: totalH, source: 'pf-pages' };
          }
          // Last resort: check any element with substantial dimensions
          const all = document.querySelectorAll('body *');
          let maxBottom = 0;
          let maxRight = 0;
          for (const el of all) {
            const r = el.getBoundingClientRect();
            if (r.bottom > maxBottom) maxBottom = r.bottom;
            if (r.right > maxRight) maxRight = r.right;
          }
          if (maxBottom > 0) return { width: maxRight, height: maxBottom, source: 'all-elements' };
          return null;
        }).catch(() => null);

        if (contentDims) {
          console.log(`[Render] Found content via ${contentDims.source}: ${Math.round(contentDims.width)}x${Math.round(contentDims.height)}px`);
          // Set viewport to match content so screenshot captures it
          await page.setViewport({
            width: Math.max(1280, Math.ceil(contentDims.width)),
            height: Math.max(1024, Math.ceil(contentDims.height)),
          });
          await new Promise((r) => setTimeout(r, 500));
        } else {
          console.warn(`[Render] Could not find any visible content dimensions`);
        }
      }
    }

    console.log(`[Render] Detecting sizing...`);

    // Force a layout reflow before sizing detection.
    // After CSS injection (especially for pdf2htmlEX), the browser may not have
    // fully recalculated layout. Reading offsetHeight forces a synchronous reflow.
    await page.evaluate(() => {
      // Force synchronous reflow by reading a layout property
      void document.body.offsetHeight;
      void document.body.scrollHeight;
      void document.documentElement.offsetHeight;
    });

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

      // For pdf2htmlEX, also try to detect page dimensions from .pf page elements
      if (isPdf2HtmlEx) {
        const pfSizing = await page.evaluate(() => {
          const firstPage = document.querySelector('.pf');
          if (!firstPage) return null;
          const rect = firstPage.getBoundingClientRect();
          return { width: Math.round(rect.width), height: Math.round(rect.height) };
        }).catch(() => null);

        if (pfSizing && pfSizing.width > 0 && pfSizing.height > 0) {
          console.log(`[Render] pdf2htmlEX page dimensions: ${pfSizing.width}x${pfSizing.height}px`);
          // Use the .pf page dimensions instead of body dimensions
          rawSizing.bodyWidth = pfSizing.width;
          rawSizing.bodyHeight = pfSizing.height;
        }
      }

      const matched = matchPaperSize(rawSizing);

      console.log(`[Render] Sizing detected: body=${rawSizing.bodyWidth}x${rawSizing.bodyHeight}, matched=${matched.matched}, scale=${matched.scale.toFixed(2)}`);

      // If viewport meta suggests a wider layout, resize viewport and re-render
      if (rawSizing.viewportMetaWidth && rawSizing.viewportMetaWidth > vw) {
        console.log(`[Render] Resizing viewport to ${rawSizing.viewportMetaWidth}px to match content`);
        await page.setViewport({ width: rawSizing.viewportMetaWidth, height: 1024 });
        await new Promise((r) => setTimeout(r, 1000)); // Let layout settle
      }

      // For pdf2htmlEX, resize viewport to match page width if needed
      if (isPdf2HtmlEx && rawSizing.bodyWidth > vw) {
        console.log(`[Render] Resizing viewport to ${rawSizing.bodyWidth}px to match pdf2htmlEX page width`);
        await page.setViewport({ width: rawSizing.bodyWidth, height: 1024 });
        await new Promise((r) => setTimeout(r, 1000));
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

    // Log memory before PDF generation
    const prePdfMem = process.memoryUsage();
    console.log(`[Memory] Before PDF gen: Heap=${Math.round(prePdfMem.heapUsed / 1024 / 1024)}MB, RSS=${Math.round(prePdfMem.rss / 1024 / 1024)}MB`);

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

    // Detect page count from HTML content
    const pageCount = await page.evaluate(() => {
      // pdf2htmlEX uses .pf class for pages
      const pfPages = document.querySelectorAll('.pf');
      if (pfPages.length > 0) return pfPages.length;
      // Also check data-page-no attribute
      const dataPages = document.querySelectorAll('[data-page-no]');
      if (dataPages.length > 0) return dataPages.length;
      // For generic HTML, check if body has visible content
      const bodyHeight = document.body.scrollHeight;
      if (bodyHeight > 100) return 1; // At least 1 page of content
      return 0;
    }).catch(() => 0);

    const renderTimeMs = Date.now() - startTime;
    console.log(`[Render] PDF generated: ${(pdfBuffer.length / 1_000_000).toFixed(1)}MB, ${pageCount} pages, ${renderTimeMs}ms${usedFallback ? " (fallback)" : ""}`);

    // ── Blank PDF detection ──────────────────────────────────────────────
    // If the PDF is suspiciously small (< 10KB), the rendering likely failed
    // silently. Try screenshot-based fallback regardless of detected page count,
    // because some formats (pdf2htmlEX with absolute positioning) may report 0 pages
    // even though content exists.
    const MIN_VALID_PDF_SIZE = 10_000; // 10KB
    if (pdfBuffer.length < MIN_VALID_PDF_SIZE && !usedFallback) {
      console.warn(`[Render] Blank PDF detected (${pdfBuffer.length} bytes, ${pageCount} pages detected). Trying screenshot fallback...`);
      try {
        // First try fullPage screenshot
        let screenshotBuffer = await page.screenshot({
          fullPage: true,
          type: "png",
        });

        // If fullPage screenshot is tiny (< 5KB), body height is probably 0.
        // Fall back to viewport-sized screenshot which captures visible area.
        if (screenshotBuffer.length < 5000) {
          console.log(`[Render] fullPage screenshot too small (${screenshotBuffer.length} bytes), trying viewport screenshot...`);
          // Get viewport dimensions
          const vp = page.viewport();
          screenshotBuffer = await page.screenshot({
            fullPage: false,
            type: "png",
            clip: { x: 0, y: 0, width: vp?.width || 1280, height: vp?.height || 1024 },
          });
          console.log(`[Render] Viewport screenshot: ${screenshotBuffer.length} bytes`);
        }
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
          const ssPdfBuffer = await imgPage.pdf({
            width: "8.27in",
            printBackground: true,
            margin: { top: "0", right: "0", bottom: "0", left: "0" },
          });
          if (ssPdfBuffer.length > pdfBuffer.length) {
            pdfBuffer = ssPdfBuffer;
            usedFallback = true;
            if (sizingMeta) {
              sizingMeta.appliedScale = 1;
              sizingMeta.sizingMatchedPaper = "screenshot-blank-recovery";
            }
            console.log(`[Render] Screenshot blank-recovery succeeded: ${(pdfBuffer.length / 1_000_000).toFixed(1)}MB`);
          }
        } finally {
          await imgPage.close().catch(() => {});
        }
      } catch (ssErr) {
        console.warn(`[Render] Screenshot blank-recovery failed: ${ssErr.message}`);
      }
    }

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
  console.log(`  LARGE_FILE_THRESHOLD: ${LARGE_FILE_THRESHOLD / 1_000_000}MB`);
  console.log(`  Auth: ${RENDER_SECRET ? "enabled" : "disabled (dev mode)"}`);

  // Pre-launch browser so first request is fast
  getBrowser().catch((err) => {
    console.error(`[Browser] Failed to pre-launch: ${err.message}`);
  });
});
