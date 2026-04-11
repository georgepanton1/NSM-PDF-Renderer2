# NSM PDF Renderer

Standalone Puppeteer-based PDF rendering microservice for XHTML files that exceed Cloudflare Worker limits (> 50MB).

## Quick Start

### Local Development

```bash
npm install
RENDER_SECRET=your-secret-here node server.js
```

### Docker

```bash
docker build -t nsm-pdf-renderer .
docker run -p 3001:3001 -e RENDER_SECRET=your-secret-here nsm-pdf-renderer
```

### Deploy to Railway

1. Push this directory to a GitHub repo
2. Create a new Railway project → "Deploy from GitHub repo"
3. Set environment variables:
   - `RENDER_SECRET` — shared secret (same value as `RENDER_SERVICE_SECRET` in the NSM app)
   - `MAX_CONCURRENT` — max simultaneous renders (default: 1, use 2 if you have 4GB+ RAM)
4. Railway will auto-detect the Dockerfile and deploy
5. Copy the Railway URL (e.g., `https://nsm-pdf-renderer-production.up.railway.app`)
6. Set `RENDER_SERVICE_URL` in the NSM app to this URL

**Recommended Railway plan:** Starter ($5/mo) with 2GB RAM minimum.

### Deploy to Fly.io

```bash
fly launch --name nsm-pdf-renderer
fly secrets set RENDER_SECRET=your-secret-here
fly scale memory 2048
fly deploy
```

## API

### POST /render

Renders an HTML page (loaded from a URL) to PDF and uploads the result to S3.

**Headers:**
- `Authorization: Bearer <RENDER_SECRET>`
- `Content-Type: application/json`

**Body:**
```json
{
  "url": "https://your-s3-bucket.com/path/to/file.html",
  "storageUploadUrl": "https://storage-api.example.com/upload/path/to/output.pdf",
  "storageApiKey": "bearer-token-for-storage-api",
  "options": {
    "timeoutMs": 300000,
    "paperFormat": "A4",
    "scale": 1
  }
}
```

**Response:**
```json
{
  "success": true,
  "pdfUrl": "https://cdn.example.com/path/to/output.pdf",
  "pdfSizeBytes": 2500000,
  "renderTimeMs": 45000,
  "pageCount": 120,
  "queueTimeMs": 0,
  "renderPath": "server-side-puppeteer"
}
```

### GET /health

Returns service health status.

```json
{
  "status": "ok",
  "browserConnected": true,
  "activeRenders": 0,
  "queuedRenders": 0,
  "totalRenders": 5,
  "memoryMb": 350
}
```

## Environment Variables

| Variable | Default | Description |
|----------|---------|-------------|
| `PORT` | 3001 | Server port |
| `RENDER_SECRET` | (none) | Shared secret for auth. If empty, auth is disabled (dev mode) |
| `MAX_CONCURRENT` | 1 | Max simultaneous renders. Use 1 for 2GB RAM, 2 for 4GB+ |
| `BROWSER_RESTART_EVERY` | 5 | Restart Chromium after N renders to reclaim memory |

## Architecture

```
NSM Server (Manus)                    PDF Renderer (Railway/Fly.io)
┌─────────────────┐                   ┌──────────────────────┐
│                  │  POST /render     │                      │
│  File > 50MB?  ─┼──────────────────►│  Puppeteer + Chromium│
│  Yes → call     │  {url, upload...} │  (2GB+ RAM, no limit)│
│  renderer       │                   │                      │
│                  │◄──────────────────┤  Renders PDF         │
│  No → Worker    │  {pdfUrl, ...}    │  Uploads to S3       │
│  (existing)     │                   │                      │
└─────────────────┘                   └──────────────────────┘
```

The renderer receives the S3 HTML URL (already uploaded by the Worker's proxy-download step), renders it with Puppeteer, and uploads the PDF to S3 using the same storage API as the Worker. The NSM server treats the response identically to a Worker response.
