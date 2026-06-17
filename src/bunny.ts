import { config, type LibraryCreds } from './config.js';
import type { BunnyVideo } from './types.js';

const base = config.bunnyApiBase;

export class BunnyError extends Error {
  constructor(message: string, readonly status: number, readonly body?: unknown) {
    super(message);
  }
}

function withTimeout(ms: number): { signal: AbortSignal; cancel: () => void } {
  const ac = new AbortController();
  const t = setTimeout(() => ac.abort(new Error(`timeout after ${ms}ms`)), ms);
  return { signal: ac.signal, cancel: () => clearTimeout(t) };
}

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

/** API call with timeout + retry on network errors / 429 / 5xx (not on other 4xx). */
async function api(
  method: string,
  url: string,
  apiKey: string,
  opts: { body?: unknown; retries?: number } = {},
): Promise<{ status: number; body: unknown }> {
  const retries = opts.retries ?? 3;
  let lastErr: unknown;
  for (let attempt = 0; attempt <= retries; attempt++) {
    const { signal, cancel } = withTimeout(config.httpTimeoutMs);
    try {
      const res = await fetch(url, {
        method,
        headers: {
          AccessKey: apiKey,
          accept: 'application/json',
          ...(opts.body !== undefined ? { 'content-type': 'application/json' } : {}),
        },
        body: opts.body !== undefined ? JSON.stringify(opts.body) : undefined,
        signal,
      });
      const text = await res.text();
      let body: unknown = text;
      try { body = text ? JSON.parse(text) : null; } catch { /* keep text */ }

      if (res.status === 429 || res.status >= 500) {
        lastErr = new BunnyError(`HTTP ${res.status} on ${method} ${url}`, res.status, body);
        if (attempt < retries) { await sleep(1000 * (attempt + 1) * (attempt + 1)); continue; }
      }
      return { status: res.status, body };
    } catch (e) {
      lastErr = e;
      if (attempt < retries) { await sleep(1000 * (attempt + 1)); continue; }
    } finally {
      cancel();
    }
  }
  throw lastErr instanceof Error ? lastErr : new Error(String(lastErr));
}

export async function getVideo(creds: LibraryCreds, guid: string): Promise<BunnyVideo | null> {
  const { status, body } = await api('GET', `${base}/library/${creds.id}/videos/${guid}`, creds.apiKey);
  if (status === 404) return null;
  if (status !== 200) throw new BunnyError(`getVideo ${guid} -> ${status}`, status, body);
  return body as BunnyVideo;
}

export async function listVideos(
  creds: LibraryCreds,
  opts: { page?: number; perPage?: number; collectionId?: string; search?: string; orderBy?: string } = {},
): Promise<{ totalItems: number; items: BunnyVideo[] }> {
  const p = new URLSearchParams();
  p.set('page', String(opts.page ?? 1));
  p.set('itemsPerPage', String(opts.perPage ?? 100));
  if (opts.collectionId) p.set('collection', opts.collectionId);
  if (opts.search) p.set('search', opts.search);
  p.set('orderBy', opts.orderBy ?? 'date');
  const { status, body } = await api('GET', `${base}/library/${creds.id}/videos?${p}`, creds.apiKey);
  if (status !== 200) throw new BunnyError(`listVideos -> ${status}`, status, body);
  const b = body as { totalItems?: number; items?: BunnyVideo[] };
  return { totalItems: b.totalItems ?? 0, items: b.items ?? [] };
}

export interface FetchResult {
  httpStatus: number;
  guid: string | null;   // GUID of the created video, if the API returned it
  raw: unknown;
}

/**
 * Trigger a server-side fetch into the destination library. Bunny downloads `url`
 * (sending the provided headers, e.g. Referer) and creates+transcodes a new video.
 */
export async function fetchVideo(
  dest: LibraryCreds,
  args: { url: string; headers?: Record<string, string>; title: string; collectionId?: string; thumbnailTime?: number },
): Promise<FetchResult> {
  const p = new URLSearchParams();
  if (args.collectionId) p.set('collectionId', args.collectionId);
  if (args.thumbnailTime !== undefined) p.set('thumbnailTime', String(args.thumbnailTime));
  const url = `${base}/library/${dest.id}/videos/fetch?${p}`;
  const { status, body } = await api('POST', url, dest.apiKey, {
    body: { url: args.url, headers: args.headers ?? {}, title: args.title },
    retries: 1, // fetch is not safely idempotent; minimise blind retries
  });
  if (status < 200 || status >= 300) throw new BunnyError(`fetchVideo -> ${status}`, status, body);
  const b = (body ?? {}) as Record<string, unknown>;
  // Bunny can return HTTP 200 with {success:false} when the fetch is rejected (bad URL, referer, etc.).
  if (b.success === false) throw new BunnyError(`fetchVideo rejected: ${String(b.message ?? '')}`, status, body);
  const guid = (b.guid ?? b.videoId ?? b.id ?? null) as string | null;
  return { httpStatus: status, guid: typeof guid === 'string' ? guid : null, raw: body };
}

/** Update a video's title (used to strip the migration correlation marker after fetch). */
export async function updateVideoTitle(creds: LibraryCreds, guid: string, title: string): Promise<void> {
  const { status, body } = await api('POST', `${base}/library/${creds.id}/videos/${guid}`, creds.apiKey, { body: { title } });
  if (status < 200 || status >= 300) throw new BunnyError(`updateVideoTitle ${guid} -> ${status}`, status, body);
}

/** Find destination videos whose title contains a (unique) marker — used to recover from a lost fetch response. */
export async function findVideosByTitleContains(creds: LibraryCreds, collectionId: string, needle: string): Promise<BunnyVideo[]> {
  const { items } = await listVideos(creds, { perPage: 100, collectionId, search: needle, orderBy: 'date' });
  return items.filter((v) => (v.title ?? '').includes(needle));
}

export async function deleteVideo(creds: LibraryCreds, guid: string): Promise<boolean> {
  const { status, body } = await api('DELETE', `${base}/library/${creds.id}/videos/${guid}`, creds.apiKey);
  if (status === 200 || status === 404) return true; // 404 = already gone
  throw new BunnyError(`deleteVideo ${guid} -> ${status}`, status, body);
}

export function refererFor(host: string): string {
  return `https://${host}/`;
}

/** Range-GET a CDN file (HEAD is unreliable on Bunny). Returns reachability + total size. */
export async function probeFile(host: string, path: string): Promise<{ ok: boolean; status: number; sizeBytes: number | null }> {
  const { signal, cancel } = withTimeout(30_000);
  try {
    const res = await fetch(`https://${host}${path}`, {
      headers: { Referer: refererFor(host), Range: 'bytes=0-0' },
      signal,
    });
    let sizeBytes: number | null = null;
    const cr = res.headers.get('content-range'); // e.g. "bytes 0-0/12345"
    if (cr && cr.includes('/')) {
      const total = Number(cr.split('/')[1]);
      if (Number.isFinite(total)) sizeBytes = total;
    }
    return { ok: res.status === 200 || res.status === 206, status: res.status, sizeBytes };
  } catch {
    return { ok: false, status: 0, sizeBytes: null };
  } finally {
    cancel();
  }
}

export interface ResolvedSource {
  url: string;          // the URL Bunny should fetch (a CDN URL, or a GCS V4 signed URL)
  via: string;          // 'original' | 'play_720p.mp4' | 'gcs:<bucket>'
  sizeBytes: number | null;
  referer: string;      // sent as the Referer header; '' (omitted) for signed GCS URLs
  // What to persist in jobs.source_url. For GCS this is a short, stable `gcs://bucket/key` ref
  // (the signed URL is huge + expires); defaults to `url` when omitted.
  persistUrl?: string;
}

/**
 * Choose the best fetchable source URL for a video:
 *   1) /{guid}/original  (raw upload, best quality)
 *   2) highest /{guid}/play_{res}.mp4 from availableResolutions
 * Returns null if nothing is reachable (broken/upload-failed source).
 */
export async function resolveSourceUrl(creds: LibraryCreds, video: BunnyVideo): Promise<ResolvedSource | null> {
  const host = creds.cdnHost;
  const referer = refererFor(host);

  const orig = await probeFile(host, `/${video.guid}/original`);
  if (orig.ok) return { url: `https://${host}/${video.guid}/original`, via: 'original', sizeBytes: orig.sizeBytes, referer };

  const resolutions = (video.availableResolutions ?? '')
    .split(',')
    .map((r) => parseInt(r, 10))
    .filter((n) => Number.isFinite(n))
    .sort((a, b) => b - a);

  for (const res of resolutions) {
    const path = `/${video.guid}/play_${res}p.mp4`;
    const probe = await probeFile(host, path);
    if (probe.ok) return { url: `https://${host}${path}`, via: `play_${res}p.mp4`, sizeBytes: probe.sizeBytes, referer };
  }
  return null;
}

/** Confirm a destination video is actually watchable (HLS playlist reachable). */
export async function verifyWatchable(dest: LibraryCreds, guid: string): Promise<boolean> {
  const probe = await probeFile(dest.cdnHost, `/${guid}/playlist.m3u8`);
  return probe.ok;
}
