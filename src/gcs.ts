import { readFileSync } from 'node:fs';
import { Storage, type Bucket } from '@google-cloud/storage';
import { config, type GcsRoute } from './config.js';
import type { ResolvedSource } from './bunny.js';
import { log } from './logger.js';

/** Prefix used in jobs.source_url to mark a GCS-origin job (signed URLs are huge + expire). */
export const GCS_URL_PREFIX = 'gcs://';

/** A source_url is a GCS-origin marker (re-sign on use) rather than a directly-fetchable URL. */
export function isGcsRef(url: string | null | undefined): boolean {
  return !!url && url.startsWith(GCS_URL_PREFIX);
}

/**
 * Choose the route for a video_source_path by keyword. The keyword must appear as a whole
 * `/`-delimited path segment (e.g. `originals/ashtar/123.mp4` → `ashtar`). Matching is
 * case-insensitive and segment-anchored so a path can never be mis-routed by an incidental
 * substring (e.g. a filename like `clip_blb.mp4` does NOT match the `blb` route). Returns null
 * when no segment matches — the caller then treats the source as unavailable.
 */
export function gcsRouteFor(path: string | null | undefined): GcsRoute | null {
  if (!path) return null;
  const segments = path.toLowerCase().split('/').filter(Boolean);
  for (const r of config.gcs.routes) if (segments.includes(r.match)) return r;
  return null;
}

// One Storage client + Bucket per key file (lazily created, cached).
const bucketCache = new Map<string, Bucket>();

function bucketFor(route: GcsRoute): Bucket {
  const cacheKey = `${route.keyFile}::${route.bucket}`;
  const cached = bucketCache.get(cacheKey);
  if (cached) return cached;
  const sa = JSON.parse(readFileSync(route.keyFile, 'utf8')) as { project_id: string; client_email: string; private_key: string };
  const storage = new Storage({
    projectId: sa.project_id,
    credentials: { client_email: sa.client_email, private_key: sa.private_key },
  });
  const bucket = storage.bucket(route.bucket);
  bucketCache.set(cacheKey, bucket);
  return bucket;
}

/**
 * Resolve a fetchable source from GCS for the given `video_source_path`. Confirms the object exists,
 * reads its size, and mints a short-lived V4 signed READ URL that Bunny's fetch API can pull
 * (no Referer/token needed — the signature authorizes the request).
 * Returns null when there is no matching route or the object does not exist.
 */
export async function resolveGcsSource(path: string | null | undefined): Promise<ResolvedSource | null> {
  if (!config.gcs.enabled || !path) return null;
  const route = gcsRouteFor(path);
  if (!route) {
    await log.warn(`GCS fallback: no route matches source_path "${path}"`, { event: 'gcs_noroute' });
    return null;
  }

  const file = bucketFor(route).file(path);
  // IMPORTANT: distinguish "object genuinely absent" (return null → terminal skip) from a transient
  // GCS/network error (rethrow → the pool marks the job 'failed' and retries it with backoff). A
  // swallowed transient error would otherwise be recorded as permanently unrecoverable.
  let exists: boolean;
  try {
    [exists] = await file.exists();
  } catch (e) {
    await log.warn(`GCS exists() error for ${route.bucket}/${path} (retryable): ${(e as Error).message}`, { event: 'gcs_error' });
    throw e;
  }
  if (!exists) return null;

  let sizeBytes: number | null = null;
  try { const [md] = await file.getMetadata(); sizeBytes = Number(md.size) || null; } catch { /* size is advisory */ }

  const [url] = await file.getSignedUrl({
    version: 'v4',
    action: 'read',
    expires: Date.now() + config.gcs.signedUrlTtlMs,
  });

  return {
    url,
    via: `gcs:${route.bucket}`,
    sizeBytes,
    referer: '',
    persistUrl: `${GCS_URL_PREFIX}${route.bucket}/${path}`,
  };
}
