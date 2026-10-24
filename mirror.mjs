import fs from 'node:fs';
import fsp from 'node:fs/promises';
import path from 'node:path';
import crypto from 'node:crypto';
import { Readable, Transform } from 'node:stream';
import { pipeline } from 'node:stream/promises';
import { load } from 'cheerio';

const ROOT = process.cwd();
const SITE_DIR = path.join(ROOT, 'site');
const MANIFEST_PATH = path.join(ROOT, 'manifest.json');
const LOG_PATH = path.join(ROOT, 'mirror.log');

const ORIGIN = 'https://www.fdic.gov';
const START_URL = 'https://www.fdic.gov/';
const SITEMAP_URL = 'https://www.fdic.gov/sitemap.xml';

const PAGE_CONCURRENCY = intEnv('CONCURRENCY', 6);
const ASSET_CONCURRENCY = intEnv('ASSET_CONCURRENCY', 10);
const RATE_PER_SEC = Number(process.env.RATE || 15);
const MAX_PAGES = intEnv('MAX_PAGES', 30000);
const MAX_ASSETS = intEnv('MAX_ASSETS', 200000);
const MAX_FILE_BYTES = intEnv('MAX_FILE_BYTES', 1536 * 1024 * 1024);
const RESUME = process.env.RESUME !== '0';
const STREAM_THRESHOLD = 5 * 1024 * 1024;
const MAX_RUNTIME_MIN = Number(process.env.MAX_RUNTIME_MIN || 0);
const USER_AGENT =
  'FDIC-Local-Archive/1.0 (one-time offline mirror; Node.js)';

const FILE_RE =
  /\.(pdf|zip|docx?|xlsx?|pptx?|txt|csv|json|xml|rtf|epub|mp3|mp4|webm|mov|avi|wav|ogg|oga|gz|tgz|tar|7z|rar|dmg|exe|apk|iso|ics|css|js|mjs|map|png|jpe?g|gif|svg|webp|avif|ico|bmp|tiff?|woff2?|ttf|otf|eot|wasm)$/i;
const PAGE_RE = /\.(html?|php|xhtml)$/i;
const SKIP_HOSTS = new Set([
  'youtube.com',
  'www.youtube.com',
  'm.youtube.com',
  'youtu.be',
  'player.vimeo.com',
  'vimeo.com',
  'platform.twitter.com',
  'twitter.com',
  'x.com',
  'www.facebook.com',
  'facebook.com',
  'www.instagram.com',
  'instagram.com',
  'www.linkedin.com',
  'linkedin.com',
  'www.tiktok.com',
  'tiktok.com',
  'maps.google.com',
  'www.google.com',
  'www.google-analytics.com',
  'google-analytics.com',
  'connect.facebook.net',
  'cct.google',
  'taggy.google',
]);
const DISALLOWED_PREFIXES = [
  '/admin/',
  '/comment/reply/',
  '/filter/tips',
  '/node/add/',
  '/search/',
  '/fdic-search/',
  '/user/register',
  '/user/password',
  '/user/login',
  '/user/logout',
];

const logStream = fs.createWriteStream(LOG_PATH, { flags: 'a' });
logStream.on('error', () => {});
function log(...args) {
  const line = `[${new Date().toISOString()}] ${args.join(' ')}`;
  console.log(line);
  logStream.write(line + '\n');
}
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function intEnv(name, def) {
  const n = Number(process.env[name]);
  return Number.isFinite(n) && n > 0 ? n : def;
}
function hash(str, len = 10) {
  return crypto.createHash('sha1').update(str).digest('hex').slice(0, len);
}
function safeSegment(seg) {
  let s = seg.replace(/[<>:"|?*\x00-\x1f]/g, '_');
  s = s.replace(/[. ]+$/g, '_');
  if (s === '' || s === '.' || s === '..') s = '_';
  return s;
}
function safeRelPath(p) {
  const parts = p.split('/').filter(Boolean).map(safeSegment);
  return parts.join('/') || 'index';
}
function isFdic(u) {
  return u.hostname === 'www.fdic.gov' || u.hostname === 'fdic.gov';
}
function normalize(input, base) {
  try {
    const u = new URL(input, base);
    if (u.protocol !== 'http:' && u.protocol !== 'https:') return null;
    u.hash = '';
    return u;
  } catch {
    return null;
  }
}
function localUrlFor(u) {
  if (isFdic(u)) return u.pathname + u.search;
  return `/_ext/${u.hostname}${u.pathname}${u.search}`;
}
function isPageLike(u) {
  const p = u.pathname.toLowerCase();
  if (p === '/') return true;
  if (p.endsWith('/')) return true;
  if (PAGE_RE.test(p)) return true;
  if (!path.posix.extname(p)) return true;
  return false;
}
function isDisallowedPath(u) {
  const p = u.pathname.toLowerCase();
  if (p === '/search/help.html' || p === '/search/help') return false;
  return DISALLOWED_PREFIXES.some((x) => p === x || p.startsWith(x));
}
function isCoreAllowedAsset(u) {
  const p = u.pathname.toLowerCase();
  if (p.startsWith('/core/') || p.startsWith('/profiles/')) {
    return /\.(css|js|gif|jpe?g|png|svg)$/i.test(p);
  }
  return true;
}

const manifest = {
  origin: ORIGIN,
  startedAt: new Date().toISOString(),
  pages: {},
  assets: {},
  files: {},
  contentTypes: {},
  stats: {
    pages: 0,
    assets: 0,
    bytesPages: 0,
    bytesAssets: 0,
    errors: 0,
    notFound: 0,
    skippedLarge: 0,
  },
  errors: [],
};

const fileOwners = new Map();
function registerFile(localUrl, remoteUrl, rel) {
  let candidate = safeRelPath(rel);
  if (!/\.[a-z0-9]{1,8}$/i.test(candidate)) candidate += '__asset';
  const owner = fileOwners.get(candidate);
  if (owner && owner !== remoteUrl) {
    const ext = path.posix.extname(candidate);
    const base = candidate.slice(0, candidate.length - ext.length);
    candidate = `${base}__${hash(remoteUrl + localUrl, 8)}${ext}`;
  }
  fileOwners.set(candidate, remoteUrl);
  return candidate;
}
function pageFilePath(u) {
  if (u.search) return `__q/pages/${hash(u.pathname + u.search, 12)}/index.html`;
  const p = u.pathname;
  if (p === '/') return 'index.html';
  const parts = p.split('/').filter(Boolean).map(safeSegment);
  const last = parts[parts.length - 1] || '';
  if (p.endsWith('/') || !PAGE_RE.test(last)) parts.push('index.html');
  return parts.join('/');
}
function assetFilePath(u) {
  const same = isFdic(u);
  let p = same ? u.pathname : `/_ext/${u.hostname}${u.pathname}`;
  if (!p || p.endsWith('/')) p += 'index';
  if (!/\.[a-z0-9]{1,8}$/i.test(p)) {
    const fileParam = u.searchParams.get('file');
    if (fileParam) {
      p += '-' + safeSegment(fileParam.split('/').pop() || 'file');
    }
  }
  const base = safeRelPath(p);
  if (u.search) {
    const ext = path.posix.extname(base);
    const stem = base.slice(0, base.length - ext.length);
    return `${stem}__${hash(u.search, 8)}${ext}`;
  }
  return base;
}

function recordError(kind, url, message) {
  manifest.stats.errors++;
  const entry = { kind, url, message: String(message).slice(0, 500), at: new Date().toISOString() };
  manifest.errors.push(entry);
  if (manifest.errors.length > 500) manifest.errors.shift();
  log(`ERROR ${kind} ${url} :: ${entry.message}`);
}

let manifestDirty = 0;
async function saveManifest(force = false) {
  if (!force && manifestDirty < 100) return;
  manifestDirty = 0;
  const snapshot = JSON.stringify(manifest);
  const tmp = MANIFEST_PATH + '.tmp';
  await fsp.writeFile(tmp, snapshot);
  await fsp.rename(tmp, MANIFEST_PATH);
}

async function writeSiteFile(rel, buf) {
  const abs = path.join(SITE_DIR, ...rel.split('/'));
  await fsp.mkdir(path.dirname(abs), { recursive: true });
  await fsp.writeFile(abs, buf);
}

// ---------- rate limiting / fetching ----------
let tokens = RATE_PER_SEC;
let lastRefill = Date.now();
let globalPauseUntil = 0;
async function rateLimit() {
  for (;;) {
    const now = Date.now();
    if (globalPauseUntil > now) {
      await sleep(globalPauseUntil - now);
      continue;
    }
    tokens = Math.min(RATE_PER_SEC, tokens + ((now - lastRefill) / 1000) * RATE_PER_SEC);
    lastRefill = now;
    if (tokens >= 1) {
      tokens -= 1;
      return;
    }
    await sleep(Math.max(20, Math.ceil(((1 - tokens) / RATE_PER_SEC) * 1000)));
  }
}

async function fetchWithRetry(url, { timeout = 30000, retries = 2 } = {}) {
  let lastErr;
  for (let attempt = 0; attempt <= retries; attempt++) {
    await rateLimit();
    try {
      const res = await fetch(url, {
        redirect: 'follow',
        headers: {
          'user-agent': USER_AGENT,
          accept: 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8',
          'accept-language': 'en-US,en;q=0.9',
          referer: 'https://www.fdic.gov/',
        },
        signal: AbortSignal.timeout(timeout),
      });
      if (res.status === 429 || res.status === 503) {
        const ra = Number(res.headers.get('retry-after'));
        const waitMs = Number.isFinite(ra) ? Math.min(ra * 1000, 120000) : 15000;
        globalPauseUntil = Math.max(globalPauseUntil, Date.now() + waitMs);
        log(`THROTTLED ${res.status} ${url} pause=${waitMs}ms`);
        await res.arrayBuffer().catch(() => {});
        continue;
      }
      if (res.status >= 500 && attempt < retries) {
        await res.arrayBuffer().catch(() => {});
        await sleep(1500 * (attempt + 1));
        continue;
      }
      return res;
    } catch (err) {
      lastErr = err;
      if (attempt < retries) await sleep(1000 * (attempt + 1));
    }
  }
  throw lastErr || new Error('fetch failed');
}

// ---------- queues ----------
const pageQueue = [];
const pageSeen = new Set();
const pageDone = new Set();
const assetQueue = [];
const assetSeen = new Set();
const assetDone = new Set();
let pageActive = 0;
let assetActive = 0;
let stopping = false;
let seedingComplete = false;

const MAX_SEEN_PAGES = MAX_PAGES * 2;
function enqueuePage(raw, base) {
  const u = normalize(raw, base);
  if (!u || !isFdic(u)) return;
  if (isDisallowedPath(u)) return;
  if (!isPageLike(u)) return;
  if (pageSeen.has(u.href)) return;
  if (pageSeen.size >= MAX_SEEN_PAGES) return;
  pageSeen.add(u.href);
  pageQueue.push(u.href);
}
function enqueueAsset(raw, base) {
  const u = normalize(raw, base);
  if (!u) return;
  if (u.protocol !== 'http:' && u.protocol !== 'https:') return;
  if (SKIP_HOSTS.has(u.hostname)) return;
  if (isFdic(u) && !isCoreAllowedAsset(u)) return;
  if (isFdic(u) && isDisallowedPath(u) && !/\.(css|js|png|jpe?g|gif|svg)$/i.test(u.pathname)) return;
  if (assetSeen.has(u.href)) return;
  if (assetSeen.size >= MAX_ASSETS) return;
  assetSeen.add(u.href);
  assetQueue.push(u.href);
}

function srcsetUrls(value) {
  if (!value) return [];
  const out = [];
  for (const part of String(value).split(',')) {
    const first = part.trim().split(/\s+/)[0];
    if (first && !first.startsWith('data:')) out.push(first);
  }
  return out;
}

function extractCssUrls(text) {
  const out = [];
  const urlRe = /url\(\s*(?:"([^"]*)"|'([^']*)'|([^)'"\s][^)]*?))\s*\)/gi;
  let m;
  while ((m = urlRe.exec(text))) {
    const v = (m[1] ?? m[2] ?? m[3] ?? '').trim();
    if (v && !v.startsWith('data:')) out.push(v);
  }
  const importRe = /@import\s+(?:url\(\s*)?(?:"([^"]+)"|'([^']+)')/gi;
  while ((m = importRe.exec(text))) {
    const v = (m[1] ?? m[2] ?? '').trim();
    if (v) out.push(v);
  }
  const imageSetRe = /image-set\(([^)]*)\)/gi;
  while ((m = imageSetRe.exec(text))) {
    const inner = m[1];
    const quoted = /["']([^"']+\.(?:png|jpe?g|gif|svg|webp|avif)[^"']*)["']/gi;
    let mm;
    while ((mm = quoted.exec(inner))) out.push(mm[1]);
  }
  return out;
}

function extractJsAssets(text) {
  const out = [];
  const re =
    /(["'`])((?:\/(?!\/)|https?:\/\/(?:www\.)?fdic\.gov\/)[^"'`\s<>]{1,400}?\.(?:js|mjs|css|woff2?|ttf|otf|eot|png|jpe?g|gif|svg|webp|avif|ico|json|wasm|mp4|webm|mp3|pdf))(?:\?[^"'`\s<>]*)?\1/g;
  let m;
  while ((m = re.exec(text))) {
    const raw = m[2];
    if (raw.includes('${') || raw.includes('*')) continue;
    out.push(raw);
  }
  return out;
}

const contentHashToRel = new Map();

async function saveAssetStream(url, res, contentType) {
  const u = new URL(url);
  const local = localUrlFor(u);
  const rel = registerFile(local, url, assetFilePath(u));
  const abs = path.join(SITE_DIR, ...rel.split('/'));
  await fsp.mkdir(path.dirname(abs), { recursive: true });
  const tmp = `${abs}.__part`;
  let bytes = 0;
  const counter = new Transform({
    transform(chunk, _enc, cb) {
      bytes += chunk.length;
      if (bytes > MAX_FILE_BYTES) cb(new Error(`file exceeds ${MAX_FILE_BYTES} bytes`));
      else cb(null, chunk);
    },
  });
  try {
    await pipeline(Readable.fromWeb(res.body), counter, fs.createWriteStream(tmp));
    await fsp.rm(abs, { force: true });
    await fsp.rename(tmp, abs);
  } catch (e) {
    await fsp.rm(tmp, { force: true }).catch(() => {});
    throw e;
  }
  manifest.assets[url] = local;
  manifest.files[local] = rel;
  if (contentType) manifest.contentTypes[local] = contentType;
  manifest.stats.assets++;
  manifest.stats.bytesAssets += bytes;
  manifest.stats.streamedAssets = (manifest.stats.streamedAssets || 0) + 1;
  manifestDirty++;
}

async function saveAsset(url, buf, contentType) {
  const u = new URL(url);
  const local = localUrlFor(u);
  const digest = crypto.createHash('sha256').update(buf).digest('hex');
  const existingRel = contentHashToRel.get(digest);
  manifest.assets[url] = local;
  if (contentType) manifest.contentTypes[local] = contentType;
  if (existingRel) {
    manifest.files[local] = existingRel;
    manifest.stats.assets++;
    manifest.stats.dedupedAssets = (manifest.stats.dedupedAssets || 0) + 1;
    manifestDirty++;
    return;
  }
  const rel = registerFile(local, url, assetFilePath(u));
  contentHashToRel.set(digest, rel);
  manifest.files[local] = rel;
  await writeSiteFile(rel, buf);
  manifest.stats.assets++;
  manifest.stats.bytesAssets += buf.length;
  manifestDirty++;
}

function parseHtml(html, pageUrl) {
  let $;
  try {
    $ = load(html);
  } catch (e) {
    recordError('html-parse', pageUrl, e.message);
    return;
  }
  const baseHref = $('base[href]').first().attr('href');
  let base = pageUrl;
  if (baseHref) {
    const b = normalize(baseHref, pageUrl);
    if (b) base = b.href;
  }

  const seenAttrs = new Set();
  const addAsset = (v) => {
    if (!v) return;
    const key = 'a:' + v;
    if (seenAttrs.has(key)) return;
    seenAttrs.add(key);
    enqueueAsset(v, base);
  };
  const addSrcset = (v) => {
    for (const u of srcsetUrls(v)) addAsset(u);
  };

  $('a[href], area[href]').each((_, el) => {
    const href = $(el).attr('href');
    if (!href) return;
    const u = normalize(href, base);
    if (!u) return;
    if (!isFdic(u)) return;
    if (FILE_RE.test(u.pathname)) {
      addAsset(u.href);
    } else if (isPageLike(u)) {
      enqueuePage(u.href);
    }
  });

  $('link[href]').each((_, el) => {
    const href = $(el).attr('href');
    const rel = String($(el).attr('rel') || '').toLowerCase();
    if (!href) return;
    if (rel.includes('preconnect') || rel.includes('dns-prefetch')) return;
    if (
      (rel.includes('alternate') || rel.includes('canonical') || rel.includes('shortlink')) &&
      !FILE_RE.test(href)
    ) {
      const u = normalize(href, base);
      if (u && isFdic(u) && isPageLike(u)) enqueuePage(u.href);
      return;
    }
    addAsset(href);
  });

  $('script[src]').each((_, el) => addAsset($(el).attr('src')));
  $('img').each((_, el) => {
    const $el = $(el);
    addAsset($el.attr('src'));
    addAsset($el.attr('data-src'));
    addSrcset($el.attr('srcset'));
    addSrcset($el.attr('data-srcset'));
  });
  $('source').each((_, el) => {
    const $el = $(el);
    addAsset($el.attr('src'));
    addSrcset($el.attr('srcset'));
    addSrcset($el.attr('data-srcset'));
  });
  $('video, audio').each((_, el) => {
    const $el = $(el);
    addAsset($el.attr('src'));
    addAsset($el.attr('poster'));
  });
  $('track').each((_, el) => addAsset($(el).attr('src')));
  $('embed').each((_, el) => addAsset($(el).attr('src')));
  $('object').each((_, el) => addAsset($(el).attr('data')));
  $('input[type="image"]').each((_, el) => addAsset($(el).attr('src')));
  $('body, table, td, th').each((_, el) => addAsset($(el).attr('background')));

  $('iframe[src]').each((_, el) => {
    const src = $(el).attr('src');
    const u = normalize(src, base);
    if (!u) return;
    if (isFdic(u) && isPageLike(u)) enqueuePage(u.href);
    else if (isFdic(u) && FILE_RE.test(u.pathname)) addAsset(u.href);
  });

  $('meta').each((_, el) => {
    const prop = String($(el).attr('property') || $(el).attr('name') || $(el).attr('itemprop') || '').toLowerCase();
    if (/(^|:)(image|video|audio)$|thumbnailurl|contenturl/.test(prop)) {
      addAsset($(el).attr('content'));
    }
  });

  $('svg image, svg use').each((_, el) => {
    const $el = $(el);
    addAsset($el.attr('href'));
    addAsset($el.attr('xlink:href'));
  });

  $('[style]').each((_, el) => {
    for (const u of extractCssUrls($(el).attr('style') || '')) addAsset(u);
  });
  $('style').each((_, el) => {
    for (const u of extractCssUrls($(el).text() || '')) addAsset(u);
  });

  $('[data-src], [data-srcset], [data-bg], [data-background], [data-background-image], [data-image], [data-poster], [data-url], [data-href]').each(
    (_, el) => {
      const $el = $(el);
      for (const name of ['data-src', 'data-srcset', 'data-bg', 'data-background', 'data-background-image', 'data-image', 'data-poster', 'data-url', 'data-href']) {
        const v = $el.attr(name);
        if (!v) continue;
        if (name.includes('srcset')) addSrcset(v);
        else addAsset(v);
      }
    }
  );

  $('script:not([src])').each((_, el) => {
    const text = $(el).text() || '';
    if (!text || text.length > 700000) return;
    for (const raw of extractJsAssets(text, base)) enqueueAsset(raw, base);
  });
}

async function processPage(url) {
  let res;
  try {
    res = await fetchWithRetry(url);
  } catch (e) {
    recordError('page-fetch', url, e.message);
    return;
  }
  const finalUrl = res.url || url;
  if (!res.ok) {
    if (res.status === 404) manifest.stats.notFound++;
    recordError('page-status', url, `HTTP ${res.status}`);
    await res.arrayBuffer().catch(() => {});
    return;
  }
  const ct = res.headers.get('content-type') || '';
  let buf;
  try {
    buf = Buffer.from(await res.arrayBuffer());
  } catch (e) {
    recordError('page-body', url, e.message);
    return;
  }
  if (!/html/i.test(ct)) {
    try {
      await saveAsset(url, buf, ct);
    } catch (e) {
      recordError('asset-save', url, e.message);
    }
    return;
  }
  const u = new URL(url);
  const local = localUrlFor(u);
  const rel = registerFile(local, url, pageFilePath(u));
  manifest.pages[url] = local;
  manifest.files[local] = rel;
  manifest.contentTypes[local] = ct || 'text/html; charset=utf-8';
  try {
    await writeSiteFile(rel, buf);
  } catch (e) {
    recordError('page-save', url, e.message);
    return;
  }
  manifest.stats.pages++;
  manifest.stats.bytesPages += buf.length;
  manifestDirty++;
  parseHtml(buf.toString('utf8'), finalUrl);
  if (manifestDirty >= 100) await saveManifest();
}

async function processAsset(url) {
  let res;
  const isMedia = /\.(mp3|mp4|m4v|webm|mov|avi|wav|ogg|oga|pdf|zip|gz|tgz|tar|7z|rar|epub|pptx?|docx?|xlsx?)$/i.test(
    new URL(url).pathname
  );
  try {
    res = await fetchWithRetry(url, { retries: 1, timeout: isMedia ? 180000 : 30000 });
  } catch (e) {
    recordError('asset-fetch', url, e.message);
    return;
  }
  if (!res.ok) {
    if (res.status === 404) manifest.stats.notFound++;
    recordError('asset-status', url, `HTTP ${res.status}`);
    await res.arrayBuffer().catch(() => {});
    return;
  }
  const ct = res.headers.get('content-type') || '';
  const len = Number(res.headers.get('content-length') || 0);
  if (len > MAX_FILE_BYTES) {
    manifest.stats.skippedLarge++;
    log(`SKIP large ${url} bytes=${len}`);
    await res.arrayBuffer().catch(() => {});
    return;
  }
  const streamIt =
    len > STREAM_THRESHOLD ||
    (len === 0 && /^(video\/|audio\/|application\/pdf|application\/zip|application\/octet-stream|font\/)/i.test(ct));
  if (streamIt) {
    try {
      await saveAssetStream(url, res, ct);
    } catch (e) {
      if (/exceeds/.test(e.message)) manifest.stats.skippedLarge++;
      recordError('asset-stream', url, e.message);
    }
    if (manifestDirty >= 100) await saveManifest();
    return;
  }
  let buf;
  try {
    buf = Buffer.from(await res.arrayBuffer());
  } catch (e) {
    recordError('asset-body', url, e.message);
    return;
  }
  if (buf.length > MAX_FILE_BYTES) {
    manifest.stats.skippedLarge++;
    log(`SKIP large ${url} bytes=${buf.length}`);
    return;
  }
  try {
    await saveAsset(url, buf, ct);
  } catch (e) {
    recordError('asset-save', url, e.message);
    return;
  }
  const looksCss = /text\/css/i.test(ct) || /\.css$/i.test(new URL(url).pathname);
  const looksJs = /javascript|ecmascript/i.test(ct) || /\.m?js$/i.test(new URL(url).pathname);
  const looksSvg = /image\/svg\+xml/i.test(ct) || /\.svg$/i.test(new URL(url).pathname);
  if (looksCss) {
    const text = buf.toString('utf8');
    for (const raw of extractCssUrls(text)) enqueueAsset(raw, url);
  } else if (looksJs) {
    if (buf.length < 8 * 1024 * 1024) {
      const text = buf.toString('utf8');
      for (const raw of extractJsAssets(text, url)) enqueueAsset(raw, url);
    }
  } else if (looksSvg) {
    const text = buf.toString('utf8');
    const re = /(?:href|xlink:href)\s*=\s*["']([^"']+)["']/gi;
    let m;
    while ((m = re.exec(text))) enqueueAsset(m[1], url);
    const re2 = /url\(\s*(?:"([^"]*)"|'([^']*)'|([^)'"]*))\s*\)/gi;
    while ((m = re2.exec(text))) enqueueAsset(m[1] ?? m[2] ?? m[3], url);
  }
  if (manifestDirty >= 100) await saveManifest();
}

async function pageWorker() {
  for (;;) {
    if (stopping) return;
    if (pageQueue.length === 0) {
      if (!seedingComplete) {
        await sleep(100);
        continue;
      }
      if (pageDone.size >= pageSeen.size && pageActive === 0) return;
      await sleep(100);
      continue;
    }
    const url = pageQueue.shift();
    if (pageDone.has(url)) continue;
    pageDone.add(url);
    pageActive++;
    try {
      await processPage(url);
    } catch (e) {
      recordError('page-unhandled', url, e.stack || e.message);
    } finally {
      pageActive--;
    }
  }
}

async function assetWorker() {
  for (;;) {
    if (stopping) return;
    if (assetQueue.length === 0) {
      if (!seedingComplete) {
        await sleep(100);
        continue;
      }
      if (assetDone.size >= assetSeen.size && pageActive === 0 && pageQueue.length === 0) return;
      await sleep(100);
      continue;
    }
    const url = assetQueue.shift();
    if (assetDone.has(url)) continue;
    assetDone.add(url);
    assetActive++;
    try {
      await processAsset(url);
    } catch (e) {
      recordError('asset-unhandled', url, e.stack || e.message);
    } finally {
      assetActive--;
    }
  }
}

async function fetchSitemapUrls() {
  const found = [];
  let indexXml;
  try {
    const res = await fetchWithRetry(SITEMAP_URL);
    indexXml = await res.text();
  } catch (e) {
    recordError('sitemap', SITEMAP_URL, e.message);
    return found;
  }
  const subPages = [...indexXml.matchAll(/<loc>([^<]+)<\/loc>/g)].map((m) => m[1].trim());
  const pages = subPages.length ? subPages : [SITEMAP_URL];
  for (const pageUrl of pages) {
    try {
      const res = await fetchWithRetry(pageUrl);
      const xml = await res.text();
      for (const m of xml.matchAll(/<loc>([^<]+)<\/loc>/g)) found.push(m[1].trim());
      log(`sitemap ${pageUrl} -> total ${found.length}`);
    } catch (e) {
      recordError('sitemap-page', pageUrl, e.message);
    }
  }
  return found;
}

async function fileExists(rel) {
  if (!rel) return false;
  try {
    const st = await fsp.stat(path.join(SITE_DIR, ...rel.split('/')));
    return st.isFile();
  } catch {
    return false;
  }
}

async function loadExisting() {
  if (!RESUME) return;
  let prev;
  try {
    prev = JSON.parse(await fsp.readFile(MANIFEST_PATH, 'utf8'));
  } catch (e) {
    if (e.code !== 'ENOENT') log('RESUME parse failed: ' + e.message);
    return;
  }
  if (!prev || !prev.pages) return;
  manifest.pages = prev.pages || {};
  manifest.assets = prev.assets || {};
  manifest.files = prev.files || {};
  manifest.contentTypes = prev.contentTypes || {};
  manifest.stats = { ...manifest.stats, ...(prev.stats || {}) };
  manifest.errors = Array.isArray(prev.errors) ? prev.errors : [];
  manifest.startedAt = prev.startedAt || manifest.startedAt;
  manifest.resumedAt = new Date().toISOString();

  for (const rel of Object.values(manifest.files)) fileOwners.set(rel, '__existing__');

  let pagesOk = 0;
  let assetsOk = 0;
  let pagesMissing = 0;
  let assetsMissing = 0;
  for (const [url, local] of Object.entries(manifest.pages)) {
    const rel = manifest.files[local];
    if (rel && (await fileExists(rel))) {
      pageSeen.add(url);
      pageDone.add(url);
      pagesOk++;
    } else {
      delete manifest.pages[url];
      if (rel) delete manifest.files[local];
      pagesMissing++;
    }
  }
  for (const [url, local] of Object.entries(manifest.assets)) {
    const rel = manifest.files[local];
    if (rel && (await fileExists(rel))) {
      assetSeen.add(url);
      assetDone.add(url);
      assetsOk++;
    } else {
      delete manifest.assets[url];
      if (rel) delete manifest.files[local];
      assetsMissing++;
    }
  }
  log(`RESUME pages=${pagesOk} (missing ${pagesMissing}) assets=${assetsOk} (missing ${assetsMissing})`);
}

async function rescanExisting() {
  let pages = 0;
  let css = 0;
  let js = 0;
  let svg = 0;
  for (const [url, local] of Object.entries(manifest.pages)) {
    const rel = manifest.files[local];
    if (!rel) continue;
    try {
      const abs = path.join(SITE_DIR, ...rel.split('/'));
      const st = await fsp.stat(abs);
      if (st.size > 4 * 1024 * 1024) continue;
      const html = await fsp.readFile(abs, 'utf8');
      parseHtml(html, url);
      pages++;
      if (pages % 1000 === 0) log(`RESCAN pages=${pages} queuedAssets=${assetSeen.size}`);
    } catch {}
  }
  for (const [url, local] of Object.entries(manifest.assets)) {
    const rel = manifest.files[local];
    if (!rel) continue;
    const ext = path.extname(rel).toLowerCase();
    if (!['.css', '.js', '.mjs', '.svg'].includes(ext)) continue;
    try {
      const abs = path.join(SITE_DIR, ...rel.split('/'));
      const st = await fsp.stat(abs);
      if (st.size > 8 * 1024 * 1024) continue;
      const text = await fsp.readFile(abs, 'utf8');
      if (ext === '.css') {
        for (const raw of extractCssUrls(text)) enqueueAsset(raw, url);
        css++;
      } else if (ext === '.svg') {
        const re = /(?:href|xlink:href)\s*=\s*["']([^"']+)["']/gi;
        let m;
        while ((m = re.exec(text))) enqueueAsset(m[1], url);
        for (const raw of extractCssUrls(text)) enqueueAsset(raw, url);
        svg++;
      } else {
        for (const raw of extractJsAssets(text)) enqueueAsset(raw, url);
        js++;
      }
    } catch {}
  }
  log(`RESCAN done pages=${pages} css=${css} js=${js} svg=${svg} queuedAssets=${assetSeen.size}`);
}

let progressTimer;
function startProgress() {
  progressTimer = setInterval(() => {
    log(
      `PROGRESS pages=${pageDone.size}/${pageSeen.size} active=${pageActive} assets=${assetDone.size}/${assetSeen.size} active=${assetActive} queues=${pageQueue.length}/${assetQueue.length} bytes=${(
        (manifest.stats.bytesPages + manifest.stats.bytesAssets) /
        1048576
      ).toFixed(1)}MB errors=${manifest.stats.errors}`
    );
    saveManifest().catch((e) => log('MANIFEST ERROR ' + e.message));
  }, 20000);
}

async function main() {
  await fsp.mkdir(SITE_DIR, { recursive: true });
  log(`Starting mirror: ${START_URL}`);
  log(`Rate=${RATE_PER_SEC}/s pages=${PAGE_CONCURRENCY} assets=${ASSET_CONCURRENCY} maxPages=${MAX_PAGES}`);
  await loadExisting();
  if (RESUME) await rescanExisting();

  enqueuePage(START_URL);
  const workers = [];
  for (let i = 0; i < PAGE_CONCURRENCY; i++) workers.push(pageWorker());
  for (let i = 0; i < ASSET_CONCURRENCY; i++) workers.push(assetWorker());
  startProgress();

  let runtimeTimer = null;
  if (MAX_RUNTIME_MIN > 0) {
    runtimeTimer = setTimeout(() => {
      log(`Runtime limit ${MAX_RUNTIME_MIN}m reached; stopping gracefully (resume to continue)`);
      stopping = true;
    }, MAX_RUNTIME_MIN * 60000);
  }

  const sitemapUrls = await fetchSitemapUrls();
  const sorted = sitemapUrls
    .map((u) => ({ u, depth: new URL(u).pathname.split('/').filter(Boolean).length }))
    .sort((a, b) => a.depth - b.depth)
    .map((x) => x.u);
  for (const u of sorted) enqueuePage(u, SITEMAP_URL);
  seedingComplete = true;
  log(`Seeded sitemap URLs: ${sorted.length}, page queue=${pageQueue.length}`);

  const shutdown = async (sig) => {
    log(`Received ${sig}, shutting down...`);
    stopping = true;
    await saveManifest(true);
    await fsp.appendFile(LOG_PATH, '').catch(() => {});
    process.exit(0);
  };
  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  await Promise.all(workers);
  stopping = true;
  if (runtimeTimer) clearTimeout(runtimeTimer);
  await sleep(300);
  clearInterval(progressTimer);
  const remainingPages = pageSeen.size - pageDone.size;
  const remainingAssets = assetSeen.size - assetDone.size;
  const complete = remainingPages <= 0 && remainingAssets <= 0;
  if (complete) manifest.finishedAt = new Date().toISOString();
  await saveManifest(true);
  log(
    `${complete ? 'DONE' : 'PAUSED'} pages=${manifest.stats.pages} assets=${manifest.stats.assets} errors=${
      manifest.stats.errors
    } notFound=${manifest.stats.notFound} skippedLarge=${manifest.stats.skippedLarge} remainingPages=${remainingPages} remainingAssets=${remainingAssets}`
  );
  logStream.end();
}

process.on('uncaughtException', async (e) => {
  try {
    recordError('uncaught', START_URL, e.stack || e.message);
    await saveManifest(true);
  } catch {}
  process.exit(1);
});
process.on('unhandledRejection', async (e) => {
  try {
    recordError('unhandled-rejection', START_URL, (e && (e.stack || e.message)) || String(e));
    await saveManifest(true);
  } catch {}
  process.exit(1);
});

main().catch(async (e) => {
  recordError('fatal', START_URL, e.stack || e.message);
  await saveManifest(true).catch(() => {});
  logStream.end();
  process.exit(1);
});
