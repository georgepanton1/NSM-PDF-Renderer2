/**
 * NSM PDF Renderer — Standalone Puppeteer Rendering Microservice
 *
 * Accepts an S3 HTML URL and renders it to PDF using headless Chromium.
 * Designed for files > 50MB that exceed Cloudflare Worker limits.
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

// ── Render function ─────────────────────────────────────────────────────────

async function renderHtmlToPdf(s3HtmlUrl, options = {}) {
  const {
    timeoutMs = 5 * 60 * 1000,
    paperFormat = "A4",
    scale = 1,
    margins = { top: "10mm", right: "10mm", bottom: "10mm", left: "10mm" },
  } = options;

  const browser = await getBrowser();
  const page = await browser.newPage();
  const startTime = Date.now();

  try {
    // Set a generous viewport — iXBRL reports expect desktop widths
    await page.setViewport({ width: 1280, height: 1024 });

    // Block unnecessary resources to speed up loading
    await page.setRequestInterception(true);
    page.on("request", (req) => {
      const type = req.resourceType();
      // Block video/media but allow images, fonts, stylesheets
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

    console.log(`[Render] Page loaded in ${Date.now() - startTime}ms, generating PDF...`);

    const pdfBuffer = await page.pdf({
      format: paperFormat,
      scale,
      printBackground: true,
      margin: margins,
      timeout: timeoutMs,
    });

    // Detect page count from pdf2htmlEX format
    const pageCount = await page.evaluate(() => {
      const pages = document.querySelectorAll("[data-page-no]");
      return pages.length > 0 ? pages.length : 0;
    }).catch(() => 0);

    const renderTimeMs = Date.now() - startTime;
    console.log(`[Render] PDF generated: ${(pdfBuffer.length / 1_000_000).toFixed(1)}MB, ${pageCount} pages, ${renderTimeMs}ms`);

    renderCount++;

    return {
      pdfBuffer: Buffer.from(pdfBuffer),
      renderTimeMs,
      pageCount,
      pdfSizeBytes: pdfBuffer.length,
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

      // Upload PDF to S3 using the provided storage URL
      const uploadRes = await fetch(storageUploadUrl, {
        method: "PUT",
        headers: {
          "Content-Type": "application/pdf",
          Authorization: `Bearer ${storageApiKey}`,
        },
        body: result.pdfBuffer,
      });

      if (!uploadRes.ok) {
        const errText = await uploadRes.text().catch(() => "(no body)");
        throw new Error(`S3 upload failed (${uploadRes.status}): ${errText.substring(0, 200)}`);
      }

      const uploadResult = await uploadRes.json();

      res.json({
        success: true,
        pdfUrl: uploadResult.url,
        pdfSizeBytes: result.pdfSizeBytes,
        renderTimeMs: result.renderTimeMs,
        pageCount: result.pageCount,
        queueTimeMs,
        renderPath: "server-side-puppeteer",
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
