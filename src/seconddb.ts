import type { RowDataPacket } from 'mysql2';
import { config } from './config.js';
import { secondQuery, secondExec, tQuery, tExec } from './db.js';
import { SECOND_DB_TABLES, type JobRow } from './types.js';
import { log } from './logger.js';

/**
 * For the given (sourceLib::guid -> jobId) map, find every referencing row in the second DB's
 * 4 tables and (re)build bunny_transfer_seconddb_map. Rows are matched by BOTH guid and library
 * (the second DB stores the source library id), so a guid shared across source libraries is
 * attributed to the correct job. Stale non-'updated' rows are pruned and second_db_present is
 * recomputed unconditionally, so a video whose 2nd-DB reference disappeared can never block deletion.
 */
export async function populateSecondDbMap(jobByLibGuid: Map<string, number>): Promise<{ jobsFlagged: number; rowsMapped: number }> {
  const scannedJobIds = [...new Set(jobByLibGuid.values())];
  if (scannedJobIds.length === 0) return { jobsFlagged: 0, rowsMapped: 0 };
  const guids = [...new Set([...jobByLibGuid.keys()].map((k) => k.slice(k.indexOf('::') + 2)))];

  // Rebuild non-'updated' rows from the current 2nd-DB state (keep 'updated' rows for audit).
  await tExec(
    `DELETE FROM bunny_transfer_seconddb_map WHERE job_id IN (${scannedJobIds.map(() => '?').join(',')}) AND status <> 'updated'`,
    scannedJobIds,
  );

  let rowsMapped = 0;
  const perJob = new Map<number, number>();
  if (guids.length) {
    const inList = guids.map(() => '?').join(',');
    for (const t of SECOND_DB_TABLES) {
      let rows: (RowDataPacket & { id: number; g: string; lib: string | null; coll: string | null })[];
      try {
        rows = await secondQuery(
          `SELECT id, ${t.videoCol} g, ${t.libCol} lib, ${t.collectionCol} coll
             FROM ${t.table} WHERE ${t.videoCol} IN (${inList})`,
          guids,
        );
      } catch (e) {
        await log.warn(`second-DB scan of ${t.table} failed: ${(e as Error).message}`, { event: 'scan2_fail' });
        continue;
      }
      for (const r of rows) {
        // Resolve the owning job by (sourceLib, guid) — the second DB's lib value is the source lib.
        const jobId = jobByLibGuid.get(`${String(r.lib).trim()}::${r.g}`);
        if (!jobId) continue; // not one of our jobs for that (library, guid)
        await tExec(
          `INSERT INTO bunny_transfer_seconddb_map
             (job_id, table_name, row_id, video_column, old_video_guid, old_library_id, old_collection_id, status)
           VALUES (?, ?, ?, ?, ?, ?, ?, 'pending')
           ON DUPLICATE KEY UPDATE
             job_id = VALUES(job_id), video_column = VALUES(video_column),
             old_video_guid = VALUES(old_video_guid), old_library_id = VALUES(old_library_id),
             old_collection_id = VALUES(old_collection_id),
             status = IF(status = 'updated', 'updated', 'pending')`,
          [jobId, t.table, r.id, t.videoCol, r.g, r.lib, r.coll],
        );
        perJob.set(jobId, (perJob.get(jobId) ?? 0) + 1);
        rowsMapped++;
      }
    }
  }

  // Recompute the flag + count for EVERY scanned job from the actual map state (no length guard).
  await tExec(
    `UPDATE bunny_transfer_jobs j
        SET second_db_present = IF((SELECT COUNT(*) FROM bunny_transfer_seconddb_map m WHERE m.job_id = j.id) > 0, 1, 0),
            second_db_row_count = (SELECT COUNT(*) FROM bunny_transfer_seconddb_map m WHERE m.job_id = j.id)
      WHERE j.id IN (${scannedJobIds.map(() => '?').join(',')})`,
    scannedJobIds,
  );
  return { jobsFlagged: perJob.size, rowsMapped };
}

export async function pendingSecondDbMapCount(jobId: number): Promise<number> {
  const rows = await tQuery<(RowDataPacket & { n: number })[]>(
    `SELECT COUNT(*) n FROM bunny_transfer_seconddb_map WHERE job_id = ? AND status <> 'updated'`,
    [jobId],
  );
  return rows[0]?.n ?? 0;
}
export async function secondDbUpdatedCount(jobId: number): Promise<number> {
  const rows = await tQuery<(RowDataPacket & { n: number })[]>(
    `SELECT COUNT(*) n FROM bunny_transfer_seconddb_map WHERE job_id = ? AND status = 'updated'`,
    [jobId],
  );
  return rows[0]?.n ?? 0;
}

interface MapRow extends RowDataPacket {
  id: number; table_name: string; row_id: number; video_column: string;
  old_video_guid: string | null; old_library_id: string | null; status: string;
}

/**
 * Repoint every second-DB row for this job to the leg-2 video (673029). Each table uses its own
 * column names; updates are guarded by BOTH the old guid AND the old library (mirroring leg 1) and
 * verified before the map row is marked done.
 */
export async function updateSecondDbForJob(job: JobRow, newGuid2: string): Promise<{ attempted: number; updated: number }> {
  const dest2 = config.dest2;
  const newLib = String(dest2.id);
  const newColl = dest2.collectionId || null;

  const mapRows = await tQuery<MapRow[]>(
    `SELECT id, table_name, row_id, video_column, old_video_guid, old_library_id, status
       FROM bunny_transfer_seconddb_map WHERE job_id = ? AND status <> 'updated'`,
    [job.id],
  );
  if (mapRows.length === 0) return { attempted: 0, updated: 0 };

  let updated = 0;
  for (const m of mapRows) {
    const t = SECOND_DB_TABLES.find((x) => x.table === m.table_name && x.videoCol === m.video_column);
    if (!t) {
      await tExec(`UPDATE bunny_transfer_seconddb_map SET status='mismatch', updated_at=NOW() WHERE id=?`, [m.id]);
      continue;
    }
    // Guarded by (old guid AND old library) — cannot clobber a row that has since changed.
    await secondExec(
      `UPDATE ${t.table} SET ${t.videoCol} = ?, ${t.libCol} = ?, ${t.collectionCol} = ?
        WHERE id = ? AND ${t.videoCol} = ? AND ${t.libCol} <=> ?`,
      [newGuid2, newLib, newColl, m.row_id, m.old_video_guid, m.old_library_id],
    );
    const check = await secondQuery<(RowDataPacket & { id: number })[]>(
      `SELECT id FROM ${t.table} WHERE id = ? AND ${t.videoCol} = ?`,
      [m.row_id, newGuid2],
    );
    if (check.length) {
      await tExec(
        `UPDATE bunny_transfer_seconddb_map
            SET status='updated', new_video_guid=?, new_library_id=?, new_collection_id=?, updated_at=NOW()
          WHERE id=?`,
        [newGuid2, newLib, newColl, m.id],
      );
      updated++;
    } else {
      await tExec(`UPDATE bunny_transfer_seconddb_map SET status='mismatch', updated_at=NOW() WHERE id=?`, [m.id]);
    }
  }
  return { attempted: mapRows.length, updated };
}
