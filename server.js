import http from 'node:http';
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const SITE_DIR = path.join(__dirname, 'site');
const MANIFEST_PATH = path.join(__dirname, 'manifest.json');
const PORT = Number(process.env.PORT || 8080);
const HOST = process.env.HOST || '127.0.0.1';

let manifest = { pages: {}, assets: {}, files: {}, contentTypes: {} };
let contentTypes = {};
let fileMap = {};
let externalRe = null;
let manifestMtime = 0;

function buildRewriters() {
  contentTypes = manifest.contentTypes || {};
  fileMap = manifest.files || {};
  const externalHosts = new Set();
  for (const remote of Object.keys(manifest.assets || {})) {
    try {
      const u = new URL(remote);
      if (!isFdicHostname(u.hostname)) externalHosts.add(u.hostname);
    } catch {}
  }
  const externalAlternatives = [...externalHosts]
    .sort((a, b) => b.length - a.length)
    .map((h) => h.replace(/[.*+?^${}()|[\]\\]/g, '\\$&'))
    .join('|');
  externalRe = externalAlternatives
    ? new RegExp(`(?:https?:)?//(${externalAlternatives})(?=[/?#"'\\\\]|$)`, 'gi')
    : null;
}

function reloadManifest(force = false) {
  let st;
  try {
    st = fs.statSync(MANIFEST_PATH);
  } catch {
    return;
  }
  if (!force && st.mtimeMs === manifestMtime) return;
  try {
    const next = JSON.parse(fs.readFileSync(MANIFEST_PATH, 'utf8'));
    manifest = next;
    manifestMtime = st.mtimeMs;
    buildRewriters();
    if (force) console.log('[server] manifest loaded');
  } catch (e) {
    console.warn('[server] could not reload manifest:', e.message);
  }
}

function isFdicHostname(host) {
  return host === 'www.fdic.gov' || host === 'fdic.gov';
}

reloadManifest(true);
if (!manifestMtime) console.warn('[server] manifest.json not found or invalid; serving raw files');
setInterval(() => reloadManifest(false), 3000);

const MIME = {
  '.html': 'text/html; charset=utf-8',
  '.htm': 'text/html; charset=utf-8',
  '.xhtml': 'application/xhtml+xml; charset=utf-8',
  '.css': 'text/css; charset=utf-8',
  '.js': 'application/javascript; charset=utf-8',
  '.mjs': 'application/javascript; charset=utf-8',
  '.json': 'application/json; charset=utf-8',
  '.map': 'application/json; charset=utf-8',
  '.xml': 'application/xml; charset=utf-8',
  '.txt': 'text/plain; charset=utf-8',
  '.svg': 'image/svg+xml',
  '.png': 'image/png',
  '.jpg': 'image/jpeg',
  '.jpeg': 'image/jpeg',
  '.gif': 'image/gif',
  '.webp': 'image/webp',
  '.avif': 'image/avif',
  '.ico': 'image/x-icon',
  '.bmp': 'image/bmp',
  '.tif': 'image/tiff',
  '.tiff': 'image/tiff',
  '.woff': 'font/woff',
  '.woff2': 'font/woff2',
  '.ttf': 'font/ttf',
  '.otf': 'font/otf',
  '.eot': 'application/vnd.ms-fontobject',
  '.pdf': 'application/pdf',
  '.mp4': 'video/mp4',
  '.m4v': 'video/mp4',
  '.webm': 'video/webm',
  '.mov': 'video/quicktime',
  '.avi': 'video/x-msvideo',
  '.mp3': 'audio/mpeg',
  '.wav': 'audio/wav',
  '.ogg': 'audio/ogg',
  '.oga': 'audio/ogg',
  '.zip': 'application/zip',
  '.gz': 'application/gzip',
  '.tgz': 'application/gzip',
  '.tar': 'application/x-tar',
  '.7z': 'application/x-7z-compressed',
  '.rar': 'application/vnd.rar',
  '.wasm': 'application/wasm',
  '.doc': 'application/msword',
  '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
  '.xls': 'application/vnd.ms-excel',
  '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
  '.ppt': 'application/vnd.ms-powerpoint',
  '.pptx': 'application/vnd.openxmlformats-officedocument.presentationml.presentation',
  '.csv': 'text/csv; charset=utf-8',
  '.ics': 'text/calendar; charset=utf-8',
  '.epub': 'application/epub+zip',
  '.exe': 'application/octet-stream',
  '.dmg': 'application/octet-stream',
  '.apk': 'application/vnd.android.package-archive',
};

const originBareRe = /(?:https?:)?\/\/(?:www\.)?fdic\.gov(?=[?#"'\\\s]|$)/gi;
const originPathRe = /(?:https?:)?\/\/(?:www\.)?fdic\.gov/gi;
const escapedOriginPathRe = /https?:\\\/\\\/(?:www\.)?fdic\.gov/gi;
const escapedOriginBareRe = /(?:https?:)?\\\/\\\/(?:www\.)?fdic\.gov(?=[?#"'\\\s]|$)/gi;

function rewriteText(text) {
  let out = text;
  if (externalRe) out = out.replace(externalRe, (_m, host) => `/_ext/${host.toLowerCase()}`);
  out = out.replace(originBareRe, '/');
  out = out.replace(escapedOriginBareRe, '/');
  out = out.replace(originPathRe, '');
  out = out.replace(escapedOriginPathRe, '');
  return out;
}

const rewriteCache = new Map();
let rewriteCacheBytes = 0;
const CACHE_LIMIT = 150 * 1024 * 1024;

function getRewritten(abs, rel, contentType) {
  let st;
  try {
    st = fs.statSync(abs);
  } catch {
    return null;
  }
  const cacheKey = `${rel}:${st.mtimeMs}:${st.size}`;
  const hit = rewriteCache.get(cacheKey);
  if (hit) return hit;
  const buf = fs.readFileSync(abs);
  const text = buf.toString('utf8');
  const rewritten = Buffer.from(rewriteText(text), 'utf8');
  if (rewritten.length < 8 * 1024 * 1024) {
    rewriteCache.set(cacheKey, rewritten);
    rewriteCacheBytes += rewritten.length;
    if (rewriteCacheBytes > CACHE_LIMIT) {
      const oldest = rewriteCache.keys().next().value;
      const old = rewriteCache.get(oldest);
      rewriteCache.delete(oldest);
      rewriteCacheBytes -= old ? old.length : 0;
    }
  }
  return rewritten;
}

function safeResolve(relPath) {
  const abs = path.resolve(SITE_DIR, relPath.replace(/^[/\\]+/, ''));
  if (abs !== SITE_DIR && !abs.startsWith(SITE_DIR + path.sep)) return null;
  return abs;
}

async function statFile(abs) {
  try {
    const st = await fsp.stat(abs);
    return st.isFile() ? st : null;
  } catch {
    return null;
  }
}

function decodePathname(pathname) {
  try {
    return decodeURIComponent(pathname);
  } catch {
    return pathname;
  }
}

async function resolveRequest(reqUrl) {
  const qIndex = reqUrl.indexOf('?');
  const rawPath = qIndex === -1 ? reqUrl : reqUrl.slice(0, qIndex);
  const search = qIndex === -1 ? '' : reqUrl.slice(qIndex);
  let pathname = rawPath;
  if (!pathname.startsWith('/')) {
    try {
      pathname = new URL(reqUrl, 'http://localhost').pathname;
    } catch {
      pathname = '/';
    }
  }
  if (pathname === '') pathname = '/';

  const exactKey = pathname + search;
  let rel = fileMap[exactKey] || fileMap[pathname];
  if (rel) {
    const abs = safeResolve(rel);
    if (abs && (await statFile(abs))) return { abs, rel, localKey: exactKey };
  }

  const decoded = decodePathname(pathname);
  const candidates = [];
  const direct = safeResolve(decoded);
  if (direct) candidates.push(direct);
  if (decoded !== '/') {
    if (decoded.endsWith('/')) {
      const d = safeResolve(decoded + 'index.html');
      if (d) candidates.push(d);
    } else {
      const withIndex = safeResolve(decoded + '/index.html');
      const withHtml = safeResolve(decoded + '.html');
      if (withIndex) candidates.push(withIndex);
      if (withHtml) candidates.push(withHtml);
    }
  } else {
    const root = safeResolve('index.html');
    if (root) candidates.push(root);
  }
  for (const abs of candidates) {
    if (await statFile(abs)) return { abs, rel: path.relative(SITE_DIR, abs), localKey: exactKey };
  }
  return null;
}

function contentTypeFor(rel, localKey, pathname) {
  const stored = contentTypes[localKey] || contentTypes[pathname];
  if (stored) return stored;
  const ext = path.extname(rel).toLowerCase();
  return MIME[ext] || 'application/octet-stream';
}

function isRewriteable(mime) {
  return (
    /text\/html|text\/css|javascript|ecmascript|application\/json|application\/xml|text\/xml|image\/svg\+xml/i.test(
      mime
    )
  );
}

async function sendFile(req, res, abs, rel, localKey, pathname) {
  const mime = contentTypeFor(rel, localKey, pathname);
  const rewrite = isRewriteable(mime);
  const range = req.headers.range;
  const headOnly = req.method === 'HEAD';

  if (!rewrite && range) {
    let st;
    try {
      st = await fsp.stat(abs);
    } catch {
      res.writeHead(404).end('Not found');
      return;
    }
    const m = /^bytes=(\d*)-(\d*)$/.exec(range);
    if (m) {
      let start = m[1] === '' ? null : Number(m[1]);
      let end = m[2] === '' ? null : Number(m[2]);
      if (start === null && end !== null) {
        start = Math.max(0, st.size - end);
        end = st.size - 1;
      }
      if (start !== null && end === null) end = st.size - 1;
      if (start === null || start > end || start >= st.size) {
        res.writeHead(416, { 'Content-Range': `bytes */${st.size}` }).end();
        return;
      }
      res.writeHead(206, {
        'Content-Type': mime,
        'Content-Length': end - start + 1,
        'Content-Range': `bytes ${start}-${end}/${st.size}`,
        'Accept-Ranges': 'bytes',
        'Cache-Control': 'no-store',
      });
      if (headOnly) {
        res.end();
        return;
      }
      fs.createReadStream(abs, { start, end }).pipe(res);
      return;
    }
  }

  let body;
  if (rewrite) {
    body = getRewritten(abs, rel, mime);
  } else {
    try {
      body = await fsp.readFile(abs);
    } catch {
      body = null;
    }
  }
  if (!body) {
    res.writeHead(404).end('Not found');
    return;
  }
  res.writeHead(200, {
    'Content-Type': mime,
    'Content-Length': body.length,
    'Cache-Control': 'no-store',
    'Accept-Ranges': rewrite ? 'none' : 'bytes',
  });
  if (headOnly) res.end();
  else res.end(body);
}

const server = http.createServer(async (req, res) => {
  if (req.method !== 'GET' && req.method !== 'HEAD') {
    res.writeHead(405, { Allow: 'GET, HEAD' }).end('Method not allowed');
    return;
  }
  try {
    const reqUrl = req.url || '/';
    const qIndex = reqUrl.indexOf('?');
    const pathname = qIndex === -1 ? reqUrl : reqUrl.slice(0, qIndex);
    const resolved = await resolveRequest(reqUrl);
    if (!resolved) {
      if (/\.(js|mjs|css|png|jpe?g|gif|svg|webp|avif|ico|woff2?|ttf|otf|eot|mp4|webm|mp3|pdf)$/i.test(pathname)) {
        console.warn(`[404] ${reqUrl}`);
      }
      res.writeHead(404, { 'Content-Type': 'text/plain; charset=utf-8' }).end(`Not found locally: ${reqUrl}`);
      return;
    }
    await sendFile(req, res, resolved.abs, resolved.rel, resolved.localKey, pathname);
  } catch (err) {
    console.error('[500]', req.url, err);
    if (!res.headersSent) res.writeHead(500, { 'Content-Type': 'text/plain; charset=utf-8' });
    res.end('Internal server error');
  }
});

server.listen(PORT, HOST, () => {
  console.log(`FDIC local mirror: http://${HOST}:${PORT}/`);
  console.log(`Serving: ${SITE_DIR}`);
  console.log(`Pages: ${Object.keys(manifest.pages || {}).length}, assets: ${Object.keys(manifest.assets || {}).length}`);
});
