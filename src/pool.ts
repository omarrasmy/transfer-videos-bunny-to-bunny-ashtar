import type { RowDataPacket } from 'mysql2';
import { config } from './config.js';
import { tQuery } from './db.js';
import { claimNext, getJob, hasRunnableJobs, setState } from './jobs.js';
import { processJob } from './worker.js';
import { log } from './logger.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

export interface WorkerView {
  id: number;
  jobId: number | null;
  guid: string | null;
  title: string | null;
  phase: 'idle' | 'processing' | 'stopped';
  since: number;
}

export const workerViews: WorkerView[] = [];
export const abort = { aborted: false };
export function requestAbort(): void { abort.aborted = true; }

let activeCount = 0;
let claimedTotal = 0;
let claimLimit = Infinity;

function ensureViews(n: number): void {
  if (workerViews.length) return;
  for (let i = 0; i < n; i++) workerViews.push({ id: i + 1, jobId: null, guid: null, title: null, phase: 'idle', since: Date.now() });
}

async function workerLoop(workerId: number): Promise<void> {
  const view = workerViews[workerId - 1]!;
  while (!abort.aborted) {
    if (claimedTotal >= claimLimit) { view.phase = 'idle'; view.jobId = null; break; }
    claimedTotal++; // reserve a slot synchronously so a low limit is honoured across workers
    const job = await claimNext(workerId);
    if (!job) {
      claimedTotal--; // release the reservation
      view.phase = 'idle'; view.jobId = null; view.guid = null; view.title = null;
      // Finish only when nothing is runnable AND nothing is in flight.
      if (activeCount === 0 && !(await hasRunnableJobs())) break;
      await sleep(2000);
      continue;
    }
    activeCount++;
    view.jobId = job.id; view.guid = job.source_video_guid; view.title = job.title; view.phase = 'processing'; view.since = Date.now();
    try {
      await processJob(job, { workerId, abort });
    } catch (e) {
      const msg = (e as Error).message?.slice(0, 2000) ?? String(e);
      await setState(job.id, 'failed', { error: msg });
      await log.error(`worker error: ${msg}`, { jobId: job.id, event: 'worker_error' });
    } finally {
      activeCount--;
    }
  }
  view.phase = 'stopped';
}

/** Live run: N workers claim and process jobs until the queue drains (or `limit` jobs processed, or abort). */
export async function runLive(limit?: number): Promise<void> {
  claimedTotal = 0;
  claimLimit = limit && limit > 0 ? limit : Infinity;
  ensureViews(config.concurrency);
  await log.info(`pool starting: ${config.concurrency} workers (live=${config.live}, dbUpdate=${config.enableDbUpdate}, delete=${config.enableSourceDelete}${Number.isFinite(claimLimit) ? `, limit=${claimLimit}` : ''})`, { event: 'pool_start' });
  await Promise.all(Array.from({ length: config.concurrency }, (_, i) => workerLoop(i + 1)));
  await log.info('pool drained', { event: 'pool_done' });
}

/** Simulation: process each pending job once (resolve-only, no writes) with bounded concurrency. */
export async function runSimulation(): Promise<void> {
  ensureViews(config.concurrency);
  const rows = await tQuery<(RowDataPacket & { id: number })[]>(
    `SELECT id FROM bunny_transfer_jobs WHERE state = 'pending' ORDER BY id`,
  );
  const ids = rows.map((r) => r.id);
  await log.info(`simulation: ${ids.length} pending jobs, ${config.concurrency}-way`, { event: 'sim_start' });

  let cursor = 0;
  async function lane(workerId: number): Promise<void> {
    const view = workerViews[workerId - 1]!;
    while (cursor < ids.length && !abort.aborted) {
      const id = ids[cursor++]!;
      const job = await getJob(id);
      if (!job) continue;
      view.jobId = job.id; view.guid = job.source_video_guid; view.title = job.title; view.phase = 'processing'; view.since = Date.now();
      try { await processJob(job, { workerId, abort }); }
      catch (e) { await log.error(`sim error: ${(e as Error).message}`, { jobId: id, event: 'sim_error' }); }
    }
    view.phase = 'stopped';
  }
  await Promise.all(Array.from({ length: config.concurrency }, (_, i) => lane(i + 1)));
  await log.info('simulation complete', { event: 'sim_done' });
}
