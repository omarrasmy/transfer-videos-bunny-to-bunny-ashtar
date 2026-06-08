import type { RowDataPacket } from 'mysql2';
import { tExec, tQuery } from './db.js';
import { config } from './config.js';
import { ACTIVE_STATES, type JobRow, type JobState } from './types.js';

type Row<T> = RowDataPacket & T;

const COLUMN_ALLOWLIST = new Set([
  'source_collection_id', 'title', 'new_video_guid', 'source_url', 'source_status',
  'dest_status', 'encode_progress', 'state', 'attempts', 'activity_count',
  'activity_updated_count', 'size_bytes', 'error', 'worker_id',
  'started_at', 'fetched_at', 'ready_at', 'db_updated_at', 'deleted_at',
  // leg 2
  'second_db_present', 'second_db_row_count', 'second_db_rows_updated',
  'new_video_guid_2', 'dest2_status', 'dest2_encode_progress', 'dest2_collection_id',
  'dest2_url', 'fetched2_at', 'ready2_at', 'second_db_updated_at',
]);

export async function updateJob(id: number, fields: Record<string, unknown>): Promise<void> {
  const keys = Object.keys(fields).filter((k) => COLUMN_ALLOWLIST.has(k));
  if (keys.length === 0) return;
  const set = keys.map((k) => `${k} = ?`).join(', ');
  const params = keys.map((k) => fields[k]);
  params.push(id);
  await tExec(`UPDATE bunny_transfer_jobs SET ${set} WHERE id = ?`, params);
}

export async function setState(id: number, state: JobState, extra: Record<string, unknown> = {}): Promise<void> {
  await updateJob(id, { state, ...extra });
}

export async function getJob(id: number): Promise<JobRow | null> {
  const rows = await tQuery<Row<JobRow>[]>(`SELECT * FROM bunny_transfer_jobs WHERE id = ?`, [id]);
  return rows[0] ?? null;
}

/**
 * Atomically claim the next runnable job for a worker.
 * Picks pending first, then failed jobs (with backoff) under the attempt cap.
 */
export async function claimNext(workerId: number): Promise<JobRow | null> {
  for (let tries = 0; tries < 25; tries++) {
    const candidates = await tQuery<Row<{ id: number; state: JobState }>[]>(
      `SELECT id, state FROM bunny_transfer_jobs
        WHERE (state = 'pending' AND attempts < ?)
           OR (state = 'failed' AND attempts < ? AND updated_at < (NOW() - INTERVAL 30 SECOND))
        ORDER BY (state = 'failed'), id
        LIMIT 1`,
      [config.maxAttempts, config.maxAttempts],
    );
    const cand = candidates[0];
    if (!cand) return null;

    const res = await tExec(
      `UPDATE bunny_transfer_jobs
          SET state = 'claimed', worker_id = ?, attempts = attempts + 1, started_at = NOW(), error = NULL
        WHERE id = ? AND state = ?`,
      [workerId, cand.id, cand.state],
    );
    if (res.affectedRows === 1) return await getJob(cand.id);
    // lost the race; try again
  }
  return null;
}

/**
 * On startup, return any jobs stuck in an active state (from a previous crash) to 'pending' so
 * claimNext can pick them up. The worker resumes correctly via the new_video_guid check (it skips
 * re-fetch when a destination video already exists and just polls). attempts is reset because an
 * interrupted job was not a genuine failure. (Must be 'pending', NOT 'transcoding' — claimNext only
 * claims pending/failed, so a 'transcoding' reset would strand the job forever.)
 */
export async function resetStaleActiveJobs(): Promise<number> {
  const placeholders = ACTIVE_STATES.map(() => '?').join(',');
  const res = await tExec(
    `UPDATE bunny_transfer_jobs
        SET state = 'pending', worker_id = NULL, attempts = 0, error = NULL
      WHERE state IN (${placeholders})`,
    [...ACTIVE_STATES],
  );
  return res.affectedRows;
}

export interface Counts { state: string; n: number; }
export async function stateCounts(): Promise<Counts[]> {
  return await tQuery<Row<Counts>[]>(
    `SELECT state, COUNT(*) n FROM bunny_transfer_jobs GROUP BY state ORDER BY n DESC`,
  );
}

export async function hasRunnableJobs(): Promise<boolean> {
  const rows = await tQuery<Row<{ n: number }>[]>(
    `SELECT COUNT(*) n FROM bunny_transfer_jobs
      WHERE (state = 'pending' AND attempts < ?) OR (state = 'failed' AND attempts < ?)`,
    [config.maxAttempts, config.maxAttempts],
  );
  return (rows[0]?.n ?? 0) > 0;
}
