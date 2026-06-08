import type { RowDataPacket } from 'mysql2';
import { config, configSummary, sourceCreds, assertSecondDbConfig } from './config.js';
import { migrate as migrateDb, closePools, tExec, tQuery, acquireRunLock, releaseRunLock } from './db.js';
import { scan } from './scan.js';
import { runLive, runSimulation, requestAbort, abort } from './pool.js';
import { resetStaleActiveJobs, stateCounts, getJob, updateJob } from './jobs.js';
import { pendingMapCount } from './activities.js';
import { pendingSecondDbMapCount } from './seconddb.js';
import { startServer } from './server.js';
import { fetchVideo, getVideo, resolveSourceUrl, verifyWatchable, deleteVideo } from './bunny.js';
import { log } from './logger.js';
import type { JobRow } from './types.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

async function recordRun(mode: string, note = '') {
  await tExec(
    `INSERT INTO bunny_transfer_runs (mode, live, enable_db_update, enable_source_delete, config, note) VALUES (?,?,?,?,?,?)`,
    [mode, config.live ? 1 : 0, config.enableDbUpdate ? 1 : 0, config.enableSourceDelete ? 1 : 0, JSON.stringify(configSummary()), note],
  );
}

async function printStats() {
  const counts = await stateCounts();
  console.log('\n  Job states:');
  for (const c of counts) console.log(`    ${c.state.padEnd(12)} ${c.n}`);
  const agg = await tQuery<(RowDataPacket & { delivered: number; deleted: number; rows_updated: number })[]>(
    `SELECT
        SUM(state='done') delivered,
        SUM(deleted_at IS NOT NULL) deleted,
        SUM(activity_updated_count) rows_updated
       FROM bunny_transfer_jobs`,
  );
  const a = agg[0];
  if (a) console.log(`\n  done=${a.delivered ?? 0}  source-deleted=${a.deleted ?? 0}  activity-rows-updated=${a.rows_updated ?? 0}\n`);
}

function banner(mode: string) {
  const c = configSummary();
  console.log('\n========================================================');
  console.log(`  MODE: ${mode}`);
  console.log(`  teachers=${c.teachers.join(',')}  type=${config.activityType}`);
  console.log(`  sources=${c.sourceLibraries.join(',')}  ->  dest lib ${c.destLibrary} / collection ${c.destCollection}`);
  console.log(`  LIVE=${c.live}  DB_UPDATE=${c.enableDbUpdate}  SOURCE_DELETE=${c.enableSourceDelete}  concurrency=${c.concurrency}`);
  if (c.enableSecondDb) console.log(`  LEG2: 2nd DB ${c.secondDbHost} -> lib ${c.dest2Library} / collection ${c.dest2Collection}`);
  if (c.includeActivityIds.length) console.log(`  INCLUDE activity ids: ${c.includeActivityIds.join(',')}`);
  console.log('========================================================\n');
}

function installSignals() {
  let count = 0;
  const handler = () => {
    count++;
    if (count === 1) { console.log('\n  Aborting gracefully (Ctrl-C again to force)...'); requestAbort(); }
    else { releaseRunLock().catch(() => {}).finally(() => process.exit(1)); }
  };
  process.on('SIGINT', handler);
  process.on('SIGTERM', handler);
}

/** Controlled single live transfer test: creates ONE dest video then deletes it. Never touches blb/source. */
async function testOne(argv: string[]) {
  banner('test-one (controlled, dest-only, self-cleaning)');
  let job: JobRow | null = null;
  if (argv[0] && argv[1]) {
    const rows = await tQuery<(RowDataPacket & JobRow)[]>(
      `SELECT * FROM bunny_transfer_jobs WHERE source_library_id=? AND source_video_guid=?`, [Number(argv[0]), argv[1]]);
    job = rows[0] ?? null;
  } else {
    // prefer the smallest already-resolved pending job (fast test), else first pending
    const pref = await tQuery<(RowDataPacket & JobRow)[]>(
      `SELECT * FROM bunny_transfer_jobs
         WHERE state='pending' AND source_url IS NOT NULL AND size_bytes IS NOT NULL
         ORDER BY size_bytes ASC LIMIT 1`);
    job = pref[0] ?? (await tQuery<(RowDataPacket & JobRow)[]>(
      `SELECT * FROM bunny_transfer_jobs WHERE state='pending' ORDER BY id LIMIT 1`))[0] ?? null;
  }
  if (!job) { console.log('No job available. Run `scan` first.'); return; }

  const creds = sourceCreds(job.source_library_id)!;
  console.log(`Testing job #${job.id}  src lib ${job.source_library_id}  guid ${job.source_video_guid}`);
  const src = await getVideo(creds, job.source_video_guid);
  if (!src) { console.log('Source 404 — pick another.'); return; }
  const resolved = await resolveSourceUrl(creds, src);
  if (!resolved) { console.log(`Source not fetchable (status ${src.status}).`); return; }
  console.log(`Source: "${src.title}" via ${resolved.via} (${resolved.sizeBytes} bytes)\n  ${resolved.url}`);

  console.log('\n→ Fetching into destination...');
  const fr = await fetchVideo(config.dest, { url: resolved.url, headers: { Referer: resolved.referer }, title: `[TEST] ${src.title}`, collectionId: config.dest.collectionId });
  console.log('  fetch HTTP', fr.httpStatus, '| guid from response:', fr.guid ?? '(none)');
  console.log('  raw response:', JSON.stringify(fr.raw));
  await log.info('test-one fetch', { jobId: job.id, event: 'test_fetch', data: fr });

  let guid = fr.guid;
  if (!guid) {
    console.log('  (no guid returned — locating by title; this confirms we need the reconcile path)');
    // reuse worker's reconcile via a short poll on the collection
    const { listVideos } = await import('./bunny.js');
    for (let i = 0; i < 8 && !guid; i++) {
      const { items } = await listVideos(config.dest, { perPage: 100, collectionId: config.dest.collectionId, orderBy: 'date' });
      guid = items.find((v) => v.title === `[TEST] ${src.title}`)?.guid ?? null;
      if (!guid) await sleep(3000);
    }
  }
  if (!guid) { console.log('  Could not determine new guid. Inspect the raw response above.'); return; }
  console.log('  new dest guid:', guid);

  console.log('\n→ Polling transcode...');
  const t0 = Date.now();
  for (;;) {
    const dv = await getVideo(config.dest, guid);
    if (!dv) { console.log('  dest video vanished'); break; }
    process.stdout.write(`\r  status ${dv.status} (${config.bunnyApiBase ? '' : ''}${dv.encodeProgress}%)   `);
    if (dv.status === 4) { console.log('\n  Finished.'); break; }
    if (dv.status === 5 || dv.status === 6) { console.log(`\n  FAILED status ${dv.status}`); break; }
    if (Date.now() - t0 > 20 * 60_000) { console.log('\n  test timeout (20m)'); break; }
    await sleep(config.pollIntervalMs);
  }

  const watch = await verifyWatchable(config.dest, guid);
  console.log('  watchable (playlist 206):', watch);

  console.log('\n→ Cleaning up: deleting the TEST copy from destination...');
  await deleteVideo(config.dest, guid);
  console.log('  test copy deleted. Source + blb untouched.');
  console.log('\n  RESULT: fetch mechanism', watch ? 'WORKS ✅' : 'needs review ⚠️');
  console.log('  fetch returns guid directly:', fr.guid ? 'YES (no reconcile needed)' : 'NO (reconcile-by-title path required)');
}

async function deletePhase() {
  banner('delete-phase');
  if (!config.live || !config.enableSourceDelete) {
    console.log('Refusing: set LIVE=true and ENABLE_SOURCE_DELETE=true to run delete-phase.'); return;
  }
  const rows = await tQuery<(RowDataPacket & JobRow)[]>(
    `SELECT * FROM bunny_transfer_jobs
      WHERE state='done' AND deleted_at IS NULL AND new_video_guid IS NOT NULL AND db_updated_at IS NOT NULL`);
  console.log(`Candidate completed jobs: ${rows.length}`);
  let deleted = 0, skipped = 0;
  for (const job of rows) {
    const creds = sourceCreds(job.source_library_id);
    if (!creds || !job.new_video_guid) { skipped++; continue; }
    // Re-verify the destination NOW (it may have been removed/corrupted since the run) and that
    // every referencing activity row is confirmed updated — never delete a source on stale flags.
    const dv = await getVideo(config.dest, job.new_video_guid);
    if (!dv || dv.status !== 4 || !(await verifyWatchable(config.dest, job.new_video_guid))) {
      await log.warn('delete-phase skip: destination not Finished/watchable', { jobId: job.id, event: 'delete_skip' });
      skipped++; continue;
    }
    const pending = await pendingMapCount(job.id);
    if (pending !== 0) {
      await log.warn(`delete-phase skip: ${pending} activity row(s) not updated`, { jobId: job.id, event: 'delete_skip' });
      skipped++; continue;
    }
    // Leg-2 gate: if this video is also in the 2nd DB, its 673029 copy must be Finished+watchable
    // and all 2nd-DB rows updated before the shared source can be deleted.
    if (config.enableSecondDb && job.second_db_present) {
      const p2 = await pendingSecondDbMapCount(job.id);
      const dv2 = job.new_video_guid_2 ? await getVideo(config.dest2, job.new_video_guid_2) : null;
      const ok2 = p2 === 0 && dv2 && dv2.status === 4 && (await verifyWatchable(config.dest2, job.new_video_guid_2!));
      if (!ok2) {
        await log.warn(`delete-phase skip: leg-2 not complete (pending=${p2})`, { jobId: job.id, event: 'delete_skip' });
        skipped++; continue;
      }
    }
    try {
      await deleteVideo(creds, job.source_video_guid);
      await updateJob(job.id, { deleted_at: new Date() });
      await log.info('source deleted (delete-phase)', { jobId: job.id, guid: job.source_video_guid, event: 'delete' });
      console.log(`  #${job.id} ${job.source_video_guid} deleted`);
      deleted++;
    } catch (e) {
      await log.error(`delete-phase failed: ${(e as Error).message}`, { jobId: job.id, event: 'delete_fail' });
      skipped++;
    }
  }
  console.log(`\ndelete-phase: deleted ${deleted}, skipped ${skipped}`);
}

async function main() {
  const mode = process.argv[2] ?? 'help';
  const rest = process.argv.slice(3);
  installSignals();

  try {
    if (mode === 'help' || mode === '--help') {
      console.log(`Usage: tsx src/index.ts <mode>
  migrate        Create queue/audit tables in the transfer DB.
  scan           Build/refresh the job set from blb (no Bunny/blb writes).
  stats          Print job-state counts.
  simulate       scan + resolve-only dry run (read-only GET/HEAD), with dashboard.
  serve          Dashboard only.
  test-one [lib guid]  Controlled single live transfer into dest, then delete the test copy.
  run            Live migration (8 workers) + dashboard. Requires LIVE=true.
  delete-phase   Delete sources for completed+updated jobs (LIVE + ENABLE_SOURCE_DELETE).`);
      return;
    }

    await migrateDb();
    assertSecondDbConfig();

    if (mode === 'migrate') { console.log('transfer DB tables ready.'); await closePools(); return; }
    if (mode === 'stats') { await printStats(); await closePools(); return; }

    if (mode === 'scan') {
      banner('scan');
      const s = await scan();
      console.log(JSON.stringify(s, null, 2));
      await printStats();
      await closePools();
      return;
    }

    if (mode === 'serve') { await recordRun('serve'); startServer(); return; /* keep alive */ }

    if (mode === 'test-one') { await testOne(rest); await closePools(); return; }

    if (mode === 'delete-phase') { await recordRun('delete-phase'); await deletePhase(); await printStats(); await closePools(); return; }

    if (mode === 'simulate') {
      banner('simulate');
      await recordRun('simulate');
      await scan();
      startServer();
      await runSimulation();
      await printStats();
      console.log('\nSimulation done. Dashboard still serving; Ctrl-C to exit.');
      return; // keep server alive for review
    }

    if (mode === 'run') {
      banner('run');
      if (!config.live) {
        console.log('Refusing to run: LIVE is false. Use `simulate` for a dry run, or set LIVE=true in .env to perform real transfers.');
        await closePools();
        return;
      }
      if (!(await acquireRunLock())) {
        console.log('Refusing to run: another `run` process holds the lock (concurrent runs corrupt jobs).');
        await closePools();
        return;
      }
      await recordRun('run');
      await scan();
      const reset = await resetStaleActiveJobs();
      if (reset) await log.info(`reset ${reset} stale in-flight jobs to resumable`, { event: 'resume' });
      startServer();
      const limit = rest[0] ? Number(rest[0]) : undefined;
      if (limit) console.log(`  (limited to ${limit} job(s) this run)`);
      await runLive(limit);
      await printStats();
      console.log('\nRun complete. Dashboard still serving; Ctrl-C to exit.');
      return; // keep server alive
    }

    console.log(`Unknown mode: ${mode}. Try \`help\`.`);
  } catch (e) {
    console.error('FATAL:', (e as Error).stack ?? e);
    process.exitCode = 1;
    if (!abort.aborted) await closePools().catch(() => {});
  }
}

main();
