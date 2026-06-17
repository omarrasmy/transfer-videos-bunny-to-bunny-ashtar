import type { RowDataPacket } from 'mysql2';
import { config } from './config.js';
import { blbQuery, tExec, tQuery } from './db.js';
import { populateSecondDbMap } from './seconddb.js';
import { log } from './logger.js';

export type Linkage = 'subject' | 'lesson' | 'union';

interface ActivityRow extends RowDataPacket {
  id: number;
  bunny_video_id: string | null;
  bunny_library_id: number | null;
  bunny_collection_id: string | null;
  title: string | null;
  video_source_path: string | null;
}

function selectionSql(linkage: Linkage): string {
  const t = config.teachers.map(() => '?').join(',');
  const subj = `SELECT a.id, a.bunny_video_id, a.bunny_library_id, a.bunny_collection_id, a.title, a.video_source_path
                  FROM activities a JOIN subjects s ON a.subject = s.id
                 WHERE a.type = ? AND s.teacher IN (${t})`;
  const less = `SELECT a.id, a.bunny_video_id, a.bunny_library_id, a.bunny_collection_id, a.title, a.video_source_path
                  FROM activities a JOIN lessons l ON a.lesson = l.id JOIN subjects s2 ON l.subject = s2.id
                 WHERE a.type = ? AND s2.teacher IN (${t})`;
  if (linkage === 'subject') return subj;
  if (linkage === 'lesson') return less;
  // union: dedupe activity ids across both paths
  return `SELECT id, bunny_video_id, bunny_library_id, bunny_collection_id, title, video_source_path FROM (
            ${subj} UNION ${less}
          ) u`;
}

function selectionParams(linkage: Linkage): unknown[] {
  const one = [config.activityType, ...config.teachers];
  return linkage === 'union' ? [...one, ...one] : one;
}

export interface ScanSummary {
  linkage: Linkage;
  totalActivityRows: number;
  rowsNoVideo: number;
  rowsAlreadyInDest: number;
  rowsUnknownLibrary: number;
  distinctVideos: number;
  jobsByLibrary: Record<string, number>;
  jobsCreated: number;
  jobsExisting: number;
  jobsSkippedNoCreds: number;
  includedActivityIds: number;
  secondDbJobsFlagged: number;
  secondDbRowsMapped: number;
}

/** Scan blb for the selection, dedupe by (library, guid), upsert jobs + activity map. */
export async function scan(linkage: Linkage = (process.env.LINKAGE as Linkage) || 'union'): Promise<ScanSummary> {
  const rows = await blbQuery<ActivityRow[]>(selectionSql(linkage), selectionParams(linkage));

  // Force-include explicit activity ids (targeted tests), regardless of teacher.
  let includedActivityIds = 0;
  if (config.includeActivityIds.length) {
    const ph = config.includeActivityIds.map(() => '?').join(',');
    const extra = await blbQuery<ActivityRow[]>(
      `SELECT a.id, a.bunny_video_id, a.bunny_library_id, a.bunny_collection_id, a.title, a.video_source_path
         FROM activities a WHERE a.type = ? AND a.id IN (${ph})`,
      [config.activityType, ...config.includeActivityIds],
    );
    const seen = new Set(rows.map((r) => r.id));
    for (const e of extra) if (!seen.has(e.id)) { rows.push(e); includedActivityIds++; }
  }

  const summary: ScanSummary = {
    linkage,
    totalActivityRows: rows.length,
    rowsNoVideo: 0,
    rowsAlreadyInDest: 0,
    rowsUnknownLibrary: 0,
    distinctVideos: 0,
    jobsByLibrary: {},
    jobsCreated: 0,
    jobsExisting: 0,
    jobsSkippedNoCreds: 0,
    includedActivityIds,
    secondDbJobsFlagged: 0,
    secondDbRowsMapped: 0,
  };

  // group by (lib, guid)
  interface Group { lib: number; guid: string; collection: string | null; title: string | null; sourcePath: string | null; activityIds: number[]; }
  const groups = new Map<string, Group>();

  for (const r of rows) {
    const guid = (r.bunny_video_id ?? '').trim();
    if (!guid) { summary.rowsNoVideo++; continue; }
    if (r.bunny_library_id == null) { summary.rowsNoVideo++; continue; }
    if (r.bunny_library_id === config.dest.id) { summary.rowsAlreadyInDest++; continue; }

    const key = `${r.bunny_library_id}::${guid}`;
    let g = groups.get(key);
    if (!g) {
      g = { lib: r.bunny_library_id, guid, collection: r.bunny_collection_id, title: r.title, sourcePath: r.video_source_path, activityIds: [] };
      groups.set(key, g);
    }
    if (!g.title && r.title) g.title = r.title;
    if (!g.collection && r.bunny_collection_id) g.collection = r.bunny_collection_id;
    if (!g.sourcePath && r.video_source_path) g.sourcePath = r.video_source_path;
    g.activityIds.push(r.id);
  }

  summary.distinctVideos = groups.size;
  const guidToJobId = new Map<string, number>();

  for (const g of groups.values()) {
    summary.jobsByLibrary[g.lib] = (summary.jobsByLibrary[g.lib] ?? 0) + 1;
    const creds = config.sources.get(g.lib);
    const hasCreds = !!creds && !!creds.apiKey && !!creds.cdnHost;
    if (!hasCreds) { summary.jobsSkippedNoCreds++; summary.rowsUnknownLibrary += g.activityIds.length; }

    const initialState = hasCreds ? 'pending' : 'skipped';
    const initialError = hasCreds ? null : `no source credentials configured for library ${g.lib}`;

    // Upsert job. On duplicate, DO NOT touch state / new_video_guid / progress columns.
    // source_path is backfilled (COALESCE) so pre-existing jobs gain it without nulling a known value.
    const res = await tExec(
      `INSERT INTO bunny_transfer_jobs
         (source_library_id, source_video_guid, source_collection_id, title, source_path,
          dest_library_id, dest_collection_id, state, activity_count, error)
       VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
       ON DUPLICATE KEY UPDATE
         title = VALUES(title),
         source_collection_id = VALUES(source_collection_id),
         source_path = COALESCE(VALUES(source_path), source_path),
         activity_count = VALUES(activity_count)`,
      [g.lib, g.guid, g.collection, g.title?.slice(0, 700) ?? null, g.sourcePath,
       config.dest.id, config.dest.collectionId, initialState, g.activityIds.length, initialError],
    );
    if (res.affectedRows === 1) summary.jobsCreated++; else summary.jobsExisting++;

    // get job id
    const idRows = await tQuery<(RowDataPacket & { id: number })[]>(
      `SELECT id FROM bunny_transfer_jobs WHERE source_library_id = ? AND source_video_guid = ?`,
      [g.lib, g.guid],
    );
    const jobId = idRows[0]?.id;
    if (!jobId) continue;
    // Key by (sourceLib, guid) so a guid shared across source libraries maps to the correct job.
    guidToJobId.set(`${g.lib}::${g.guid}`, jobId);

    // upsert activity map rows (preserve 'updated' status)
    for (const aid of g.activityIds) {
      // On re-home (job_id or source guid changed) reset to a clean pending state so a stale
      // 'updated' marker can never satisfy a delete gate for the wrong job. Order matters: the
      // status/new_* IFs are evaluated before old_video_guid/job_id are overwritten.
      await tExec(
        `INSERT INTO bunny_transfer_activity_map
           (job_id, activity_id, old_library_id, old_video_guid, old_collection_id, status)
         VALUES (?, ?, ?, ?, ?, 'pending')
         ON DUPLICATE KEY UPDATE
           status = IF(job_id <> VALUES(job_id) OR old_video_guid <> VALUES(old_video_guid), 'pending', status),
           new_library_id = IF(job_id <> VALUES(job_id) OR old_video_guid <> VALUES(old_video_guid), NULL, new_library_id),
           new_video_guid = IF(job_id <> VALUES(job_id) OR old_video_guid <> VALUES(old_video_guid), NULL, new_video_guid),
           old_library_id = VALUES(old_library_id),
           old_collection_id = VALUES(old_collection_id),
           old_video_guid = VALUES(old_video_guid),
           job_id = VALUES(job_id)`,
        [jobId, aid, g.lib, g.guid, g.collection],
      );
    }
  }

  // ---- EXPAND: map EVERY blb.activities row that still references each job's source video ----
  // The selection (teachers / INCLUDE_ACTIVITY_IDS) decides WHICH videos to migrate, but once a
  // video is migrated its 655017 source is deleted, so ALL activities pointing at it must follow —
  // not just the selected ones. (Mirrors leg-2, which already covers every reference by GUID.)
  const allJobs = await tQuery<(RowDataPacket & { id: number; lib: number; guid: string })[]>(
    `SELECT id, source_library_id lib, source_video_guid guid FROM bunny_transfer_jobs`,
  );
  const jobIdByPair = new Map<string, number>();
  for (const j of allJobs) jobIdByPair.set(`${j.lib}::${j.guid}`, j.id);
  const pairs = allJobs.map((j) => [j.lib, j.guid] as [number, string]);
  const CHUNK = 400;
  for (let i = 0; i < pairs.length; i += CHUNK) {
    const chunk = pairs.slice(i, i + CHUNK);
    const ph = chunk.map(() => '(?,?)').join(',');
    const refs = await blbQuery<ActivityRow[]>(
      `SELECT id, bunny_library_id, bunny_video_id, bunny_collection_id, title
         FROM activities WHERE (bunny_library_id, bunny_video_id) IN (${ph})`,
      chunk.flat(),
    );
    for (const r of refs) {
      const jobId = jobIdByPair.get(`${r.bunny_library_id}::${(r.bunny_video_id ?? '').trim()}`);
      if (!jobId) continue;
      await tExec(
        `INSERT INTO bunny_transfer_activity_map
           (job_id, activity_id, old_library_id, old_video_guid, old_collection_id, status)
         VALUES (?, ?, ?, ?, ?, 'pending')
         ON DUPLICATE KEY UPDATE
           status = IF(job_id <> VALUES(job_id) OR old_video_guid <> VALUES(old_video_guid), 'pending', status),
           new_library_id = IF(job_id <> VALUES(job_id) OR old_video_guid <> VALUES(old_video_guid), NULL, new_library_id),
           new_video_guid = IF(job_id <> VALUES(job_id) OR old_video_guid <> VALUES(old_video_guid), NULL, new_video_guid),
           old_library_id = VALUES(old_library_id),
           old_collection_id = VALUES(old_collection_id),
           old_video_guid = VALUES(old_video_guid),
           job_id = VALUES(job_id)`,
        [jobId, r.id, r.bunny_library_id, r.bunny_video_id, r.bunny_collection_id],
      );
    }
  }
  // activity_count reflects the full reference set.
  await tExec(
    `UPDATE bunny_transfer_jobs j
        SET activity_count = (SELECT COUNT(*) FROM bunny_transfer_activity_map m WHERE m.job_id = j.id)`,
  );

  // Backfill source_path for any job still missing one, from ANY blb.activities row that references
  // its (library, guid) — not just the teacher-selected rows the main loop saw. This guarantees a
  // GCS-routable path whenever one exists anywhere, so the fallback can reach it. COALESCE never
  // nulls a value the main loop already set. (Both DBs live on the same server, so the cross-schema
  // join runs on one connection.)
  if (config.gcs.enabled) {
    await tExec(
      `UPDATE bunny_transfer_jobs j
         JOIN (
           SELECT bunny_library_id AS lib, bunny_video_id AS guid, MIN(video_source_path) AS sp
             FROM ${config.blb.database}.activities
            WHERE video_source_path IS NOT NULL AND video_source_path <> ''
            GROUP BY bunny_library_id, bunny_video_id
         ) a ON a.lib = j.source_library_id AND a.guid = j.source_video_guid
          SET j.source_path = a.sp
        WHERE j.source_path IS NULL OR j.source_path = ''`,
    );
  }

  // Re-open completed jobs that have gained new (un-migrated) activity rows since they finished.
  // They resume from polling (new_video_guid is set) and repoint only the new pending rows.
  const reopened = await tExec(
    `UPDATE bunny_transfer_jobs j
        SET state = 'pending', worker_id = NULL, attempts = 0, error = NULL
      WHERE state = 'done'
        AND EXISTS (SELECT 1 FROM bunny_transfer_activity_map m WHERE m.job_id = j.id AND m.status <> 'updated')`,
  );
  if (reopened.affectedRows > 0) {
    await log.info(`re-opened ${reopened.affectedRows} done job(s) with new un-migrated activities`, { event: 'scan_reopen' });
  }

  // Re-open jobs that were terminally skipped because the Bunny source was gone/broken, but which
  // now have a routable source_path — a run will retry them via the GCS fallback. Two cases:
  //   1. Pre-GCS-release skips carrying the bare 'source ...' error strings (one-time backfill).
  //   2. New-worker skips stamped 'unrecoverable: ... and no source_path' that have SINCE gained a
  //      path (the COALESCE backfill above). The `source_path IS NOT NULL` guard means each such job
  //      is re-opened only once a path arrives; if GCS then also fails the worker stamps it
  //      '... and GCS object missing', which matches NEITHER clause — so this can never loop.
  if (config.gcs.enabled && config.gcs.reopenSkipped) {
    const reopenedGcs = await tExec(
      `UPDATE bunny_transfer_jobs
          SET state = 'pending', worker_id = NULL, attempts = 0, error = NULL
        WHERE state = 'skipped'
          AND source_path IS NOT NULL AND source_path <> ''
          AND (error LIKE 'source video not found%'
            OR error LIKE 'source unusable%'
            OR error LIKE 'source not fetchable%'
            OR error LIKE 'unrecoverable:% and no source_path')`,
    );
    if (reopenedGcs.affectedRows > 0) {
      await log.info(`GCS fallback: re-opened ${reopenedGcs.affectedRows} skipped job(s) with a routable source_path`, { event: 'gcs_reopen' });
    }
  }

  // Leg 2: detect which source videos are also referenced in the second DB.
  if (config.enableSecondDb) {
    const second = await populateSecondDbMap(guidToJobId);
    summary.secondDbJobsFlagged = second.jobsFlagged;
    summary.secondDbRowsMapped = second.rowsMapped;
    await log.info(`second-DB: ${second.jobsFlagged} videos also referenced there, ${second.rowsMapped} rows mapped`, { event: 'scan2' });
    // A re-opened/leg-2 job that is 'done' but now has pending second-DB rows must run again.
    await tExec(
      `UPDATE bunny_transfer_jobs j SET state='pending', worker_id=NULL, attempts=0, error=NULL
        WHERE state='done' AND second_db_present=1
          AND EXISTS (SELECT 1 FROM bunny_transfer_seconddb_map m WHERE m.job_id=j.id AND m.status<>'updated')`,
    );
  }

  await log.info(
    `scan(${linkage}): ${summary.distinctVideos} distinct videos from ${summary.totalActivityRows} rows ` +
    `(created ${summary.jobsCreated}, existing ${summary.jobsExisting}, no-creds ${summary.jobsSkippedNoCreds}, ` +
    `no-video ${summary.rowsNoVideo}, already-dest ${summary.rowsAlreadyInDest})`,
    { event: 'scan', data: summary },
  );
  return summary;
}
