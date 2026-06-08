import 'dotenv/config';

function req(name: string): string {
  const v = process.env[name];
  if (v === undefined || v === '') throw new Error(`Missing required env var: ${name}`);
  return v;
}
function opt(name: string, fallback = ''): string {
  const v = process.env[name];
  return v === undefined || v === '' ? fallback : v;
}
function bool(name: string, fallback = false): boolean {
  const v = process.env[name];
  if (v === undefined || v === '') return fallback;
  return /^(1|true|yes|on)$/i.test(v.trim());
}
function int(name: string, fallback: number): number {
  const v = process.env[name];
  if (v === undefined || v === '') return fallback;
  const n = Number(v);
  if (!Number.isFinite(n)) throw new Error(`Env ${name} must be a number, got "${v}"`);
  return n;
}

export interface DbConfig {
  host: string; port: number; user: string; password: string; database: string;
}
export interface LibraryCreds {
  id: number;
  apiKey: string;
  cdnHost: string;
  collectionId: string;
  tokenKey: string;
}

function dbCfg(prefix: string, defaultDb: string): DbConfig {
  return {
    host: req(`${prefix}_HOST`),
    port: int(`${prefix}_PORT`, 3306),
    user: req(`${prefix}_USER`),
    password: req(`${prefix}_PASSWORD`),
    database: opt(`${prefix}_DATABASE`, defaultDb),
  };
}

function sourceLibs(): Map<number, LibraryCreds> {
  const ids = req('SOURCE_LIBS').split(',').map((s) => Number(s.trim())).filter((n) => Number.isFinite(n));
  const map = new Map<number, LibraryCreds>();
  for (const id of ids) {
    const key = opt(`SRC_${id}_KEY`);
    const host = opt(`SRC_${id}_HOST`);
    if (!key || !host) {
      // A configured source lib with no creds is allowed (e.g. 650549 with no videos),
      // but we record it so the scanner can warn if it actually has videos to move.
      map.set(id, { id, apiKey: key, cdnHost: host, collectionId: opt(`SRC_${id}_COLLECTION`), tokenKey: opt(`SRC_${id}_TOKEN`) });
      continue;
    }
    map.set(id, {
      id,
      apiKey: key,
      cdnHost: host,
      collectionId: opt(`SRC_${id}_COLLECTION`),
      tokenKey: opt(`SRC_${id}_TOKEN`),
    });
  }
  return map;
}

export const config = {
  blb: dbCfg('BLB', 'blb'),
  transfer: dbCfg('TRANSFER', 'transfer'),

  teachers: req('TEACHERS').split(',').map((s) => Number(s.trim())).filter((n) => Number.isFinite(n)),
  activityType: opt('ACTIVITY_TYPE', 'Video'),

  dest: {
    id: Number(req('DEST_LIBRARY_ID')),
    apiKey: req('DEST_API_KEY'),
    cdnHost: req('DEST_CDN_HOST'),
    collectionId: req('DEST_COLLECTION_ID'),
    tokenKey: opt('DEST_TOKEN_KEY'),
  } as LibraryCreds,

  // Leg 2: redundant copy destination (only used when a source video is also referenced in the 2nd DB).
  enableSecondDb: bool('ENABLE_SECOND_DB', false),
  dest2: {
    id: Number(opt('DEST2_LIBRARY_ID', '0')),
    apiKey: opt('DEST2_API_KEY'),
    cdnHost: opt('DEST2_CDN_HOST'),
    collectionId: opt('DEST2_COLLECTION_ID'),
    tokenKey: opt('DEST2_TOKEN_KEY'),
  } as LibraryCreds,
  secondDb: {
    host: opt('SECOND_DB_HOST'),
    port: int('SECOND_DB_PORT', 3306),
    user: opt('SECOND_DB_USER'),
    password: opt('SECOND_DB_PASSWORD'),
    database: opt('SECOND_DB_DATABASE', 'blb'),
  } as DbConfig,

  // Extra first-blb activities.id values to force-include in the selection (targeted tests).
  includeActivityIds: opt('INCLUDE_ACTIVITY_IDS')
    .split(',').map((s) => Number(s.trim())).filter((n) => Number.isFinite(n)),

  sources: sourceLibs(),

  concurrency: int('CONCURRENCY', 8),
  // Master switch: when false, `run` simulates and performs NO Bunny/blb writes.
  live: bool('LIVE', false),
  // Gates the blb.activities UPDATE (only meaningful when live=true).
  enableDbUpdate: bool('ENABLE_DB_UPDATE', false),
  // Gates the source-video DELETE (only meaningful when live=true).
  enableSourceDelete: bool('ENABLE_SOURCE_DELETE', false),

  pollIntervalMs: int('POLL_INTERVAL_MS', 20_000),
  maxTranscodeWaitMs: int('MAX_TRANSCODE_WAIT_MS', 10_800_000),
  httpTimeoutMs: int('HTTP_TIMEOUT_MS', 120_000),
  maxAttempts: int('MAX_ATTEMPTS', 3),
  dashboardPort: int('DASHBOARD_PORT', 4545),

  bunnyApiBase: 'https://video.bunnycdn.com',
};

export type AppConfig = typeof config;

export function sourceCreds(libId: number): LibraryCreds | undefined {
  return config.sources.get(libId);
}

/** A short, secret-free description of the active configuration, for the dashboard/logs. */
export function configSummary() {
  return {
    teachers: config.teachers,
    activityType: config.activityType,
    destLibrary: config.dest.id,
    destCollection: config.dest.collectionId,
    sourceLibraries: [...config.sources.keys()],
    concurrency: config.concurrency,
    live: config.live,
    enableDbUpdate: config.enableDbUpdate,
    enableSourceDelete: config.enableSourceDelete,
    enableSecondDb: config.enableSecondDb,
    dest2Library: config.enableSecondDb ? config.dest2.id : null,
    dest2Collection: config.enableSecondDb ? config.dest2.collectionId : null,
    secondDbHost: config.enableSecondDb ? config.secondDb.host : null,
    includeActivityIds: config.includeActivityIds,
  };
}

/** Validate that leg-2 config is complete when enabled. Throws with a clear message otherwise. */
export function assertSecondDbConfig(): void {
  if (!config.enableSecondDb) return;
  const missing: string[] = [];
  if (!config.dest2.id) missing.push('DEST2_LIBRARY_ID');
  if (!config.dest2.apiKey) missing.push('DEST2_API_KEY');
  if (!config.dest2.cdnHost) missing.push('DEST2_CDN_HOST');
  if (!config.secondDb.host) missing.push('SECOND_DB_HOST');
  if (!config.secondDb.user) missing.push('SECOND_DB_USER');
  if (missing.length) throw new Error(`ENABLE_SECOND_DB=true but missing: ${missing.join(', ')}`);
}
