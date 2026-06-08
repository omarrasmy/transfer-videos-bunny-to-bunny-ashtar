import type { RowDataPacket } from 'mysql2';
import { blbExec, blbQuery, tExec, tQuery } from './db.js';
import { config } from './config.js';
import type { JobRow } from './types.js';

interface MapRow extends RowDataPacket { activity_id: number; status: string; }

export interface UpdateResult {
  attempted: number;
  updated: number;       // verified now pointing at the new video
  alreadyDone: number;   // map rows already marked updated
}

/**
 * Update every blb.activities row that references this job's source video, then verify
 * the write landed and reconcile the activity map. Guarded by the old (guid, library)
 * so it is idempotent and cannot clobber rows that have since changed.
 */
export async function updateActivitiesForJob(job: JobRow, newGuid: string): Promise<UpdateResult> {
  const dest = config.dest;
  const mapRows = await tQuery<MapRow[]>(
    `SELECT activity_id, status FROM bunny_transfer_activity_map WHERE job_id = ?`,
    [job.id],
  );
  const alreadyDone = mapRows.filter((r) => r.status === 'updated').length;
  const pendingIds = mapRows.filter((r) => r.status !== 'updated').map((r) => r.activity_id);
  if (pendingIds.length === 0) return { attempted: 0, updated: 0, alreadyDone };

  const placeholders = pendingIds.map(() => '?').join(',');

  // Guarded bulk update: only rows that still reference the OLD (guid, library).
  await blbExec(
    `UPDATE activities
        SET bunny_video_id = ?, bunny_library_id = ?, bunny_collection_id = ?
      WHERE id IN (${placeholders})
        AND bunny_video_id = ?
        AND bunny_library_id = ?`,
    [newGuid, dest.id, dest.collectionId, ...pendingIds, job.source_video_guid, job.source_library_id],
  );

  // Verify which rows now point at the new video.
  const verified = await blbQuery<(RowDataPacket & { id: number })[]>(
    `SELECT id FROM activities
      WHERE id IN (${placeholders}) AND bunny_video_id = ? AND bunny_library_id = ?`,
    [...pendingIds, newGuid, dest.id],
  );
  const verifiedIds = new Set(verified.map((r) => r.id));

  // Reconcile the activity map.
  for (const aid of pendingIds) {
    if (verifiedIds.has(aid)) {
      await tExec(
        `UPDATE bunny_transfer_activity_map
            SET status = 'updated', new_library_id = ?, new_video_guid = ?, new_collection_id = ?, updated_at = NOW()
          WHERE job_id = ? AND activity_id = ?`,
        [dest.id, newGuid, dest.collectionId, job.id, aid],
      );
    } else {
      // Row no longer referenced the expected source; leave a marker for audit.
      await tExec(
        `UPDATE bunny_transfer_activity_map SET status = 'mismatch', updated_at = NOW()
          WHERE job_id = ? AND activity_id = ?`,
        [job.id, aid],
      );
    }
  }

  return { attempted: pendingIds.length, updated: verifiedIds.size, alreadyDone };
}

/**
 * Best-effort trace-table sync: repoint blb.bunny_videos (the upload/where-is-it registry) to the
 * new destination so it doesn't keep pointing at a soon-to-be-deleted source library. Guarded by
 * the old (guid, library); not part of any delete gate.
 */
export async function syncBunnyVideosTrace(job: JobRow, newGuid: string): Promise<number> {
  const dest = config.dest;
  const res = await blbExec(
    `UPDATE bunny_videos SET library_id = ?, bunny_video_id = ?, bunny_guid = ?, bunny_collection_id = ?
      WHERE bunny_video_id = ? AND library_id = ?`,
    [String(dest.id), newGuid, newGuid, dest.collectionId, job.source_video_guid, String(job.source_library_id)],
  );
  return res.affectedRows;
}

/** Total activity rows updated for a job (for reporting). */
export async function updatedCountForJob(jobId: number): Promise<number> {
  const rows = await tQuery<(RowDataPacket & { n: number })[]>(
    `SELECT COUNT(*) n FROM bunny_transfer_activity_map WHERE job_id = ? AND status = 'updated'`,
    [jobId],
  );
  return rows[0]?.n ?? 0;
}

/**
 * Count of activity-map rows for a job that are NOT yet verified-updated. The source video may be
 * deleted only when this is 0 (every referencing row confirmed pointing at the new video) — a fresh,
 * membership-accurate gate that cannot be satisfied by stale/unrelated 'updated' rows.
 */
export async function pendingMapCount(jobId: number): Promise<number> {
  const rows = await tQuery<(RowDataPacket & { n: number })[]>(
    `SELECT COUNT(*) n FROM bunny_transfer_activity_map WHERE job_id = ? AND status <> 'updated'`,
    [jobId],
  );
  return rows[0]?.n ?? 0;
}
