import { config, sourceCreds } from './config.js';
import { getVideo, fetchVideo, deleteVideo, resolveSourceUrl, verifyWatchable, updateVideoTitle, findVideosByTitleContains } from './bunny.js';
import { setState, updateJob } from './jobs.js';
import { updateActivitiesForJob, updatedCountForJob, pendingMapCount } from './activities.js';
import { updateSecondDbForJob, secondDbUpdatedCount, pendingSecondDbMapCount } from './seconddb.js';
import { log } from './logger.js';
import type { JobRow } from './types.js';

const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
function fmtBytes(b: number | null): string {
  if (!b) return '?';
  const u = ['B', 'KB', 'MB', 'GB']; let i = 0; let n = b;
  while (n >= 1024 && i < u.length - 1) { n /= 1024; i++; }
  return `${n.toFixed(1)}${u[i]}`;
}

/** Unique-per-job correlation marker embedded in the fetched title so a lost fetch response can be recovered. */
const markerFor = (jobId: number) => `[mig#${jobId}]`;

export interface WorkerCtx { workerId: number; abort: { aborted: boolean }; }

/** Run a single job through the full lifecycle. Resumable: a job with new_video_guid skips re-fetch. */
export async function processJob(job: JobRow, ctx: WorkerCtx): Promise<void> {
  const creds = sourceCreds(job.source_library_id);
  const dest = config.dest;
  const tag = { jobId: job.id, guid: job.source_video_guid };

  if (!creds || !creds.apiKey || !creds.cdnHost) {
    await setState(job.id, 'skipped', { error: `no source credentials for library ${job.source_library_id}` });
    await log.warn(`skipped: no creds for lib ${job.source_library_id}`, { ...tag, event: 'skip' });
    return;
  }

  let newGuid = job.new_video_guid;
  let title = job.title ?? '';

  // ---- 1. RESOLVE + FETCH (skipped entirely when resuming an already-fetched job) ----
  if (!newGuid) {
    await setState(job.id, 'resolving', { worker_id: ctx.workerId });
    const srcVideo = await getVideo(creds, job.source_video_guid);
    if (!srcVideo) {
      await setState(job.id, 'skipped', { error: 'source video not found (404)' });
      await log.warn('skipped: source 404', { ...tag, event: 'skip' });
      return;
    }
    if (srcVideo.status === 5 || srcVideo.status === 6) {
      await setState(job.id, 'skipped', { error: `source unusable (status ${srcVideo.status})`, source_status: srcVideo.status });
      await log.warn(`skipped: source status ${srcVideo.status}`, { ...tag, event: 'skip' });
      return;
    }
    title = (srcVideo.title || title || job.source_video_guid).slice(0, 700);
    const resolved = await resolveSourceUrl(creds, srcVideo);
    if (!resolved) {
      await setState(job.id, 'skipped', { error: `source not fetchable (status ${srcVideo.status})`, source_status: srcVideo.status });
      await log.warn(`skipped: unfetchable source (status ${srcVideo.status})`, { ...tag, event: 'skip' });
      return;
    }
    await updateJob(job.id, { source_status: srcVideo.status, source_url: resolved.url, size_bytes: resolved.sizeBytes, title });
    await log.info(`resolved via ${resolved.via} (${fmtBytes(resolved.sizeBytes)})`, { ...tag, event: 'resolve', data: { url: resolved.url, via: resolved.via } });

    // ---- SIMULATION: stop before any write ----
    if (!config.live) {
      await setState(job.id, 'pending', {});
      await log.info('SIMULATION: would fetch -> poll -> update db -> delete', {
        ...tag, event: 'simulate',
        data: { wouldFetch: resolved.url, wouldUpdateDb: config.enableDbUpdate, wouldDelete: config.enableSourceDelete },
      });
      return;
    }

    // ---- 2. FETCH (idempotent via the per-job marker) ----
    const marker = markerFor(job.id);
    const cleanTitle = title.slice(0, 680);

    // Resume safety: a previous attempt may have created the dest video before its guid was persisted.
    if (job.attempts > 1) {
      const prior = await findVideosByTitleContains(dest, dest.collectionId, marker);
      if (prior[0]) {
        newGuid = prior[0].guid;
        await log.info(`adopted dest video from a prior attempt: ${newGuid}`, { ...tag, event: 'adopt' });
      }
    }

    if (!newGuid) {
      await setState(job.id, 'fetching', {});
      const fr = await fetchVideo(dest, { url: resolved.url, headers: { Referer: resolved.referer }, title: `${cleanTitle} ${marker}`, collectionId: dest.collectionId });
      await log.info(`fetch submitted (http ${fr.httpStatus}, guid ${fr.guid ?? 'none'})`, { ...tag, event: 'fetch', data: fr.raw });
      newGuid = fr.guid ?? (await findVideosByTitleContains(dest, dest.collectionId, marker))[0]?.guid ?? null;
      if (!newGuid) {
        await setState(job.id, 'failed', { error: 'fetch returned no guid and reconcile failed' });
        await log.error('fetch: no guid + reconcile failed', { ...tag, event: 'fetch_fail' });
        return;
      }
    }

    await updateJob(job.id, { new_video_guid: newGuid, fetched_at: new Date(), dest_status: 0, encode_progress: 0 });
    // Strip the correlation marker from the destination title (best-effort; cosmetic only).
    try { await updateVideoTitle(dest, newGuid, cleanTitle); }
    catch (e) { await log.warn(`title cleanup failed: ${(e as Error).message}`, { ...tag, event: 'rename_fail' }); }
  }

  // ---- 3. POLL TRANSCODING ----
  await setState(job.id, 'transcoding', {});
  const startWait = Date.now();
  for (;;) {
    if (ctx.abort.aborted) { await setState(job.id, 'transcoding', {}); return; } // leave resumable
    const dv = await getVideo(dest, newGuid);
    if (!dv) {
      await updateJob(job.id, { new_video_guid: null });
      await setState(job.id, 'failed', { error: 'destination video vanished during transcode' });
      await log.error('dest video vanished', { ...tag, event: 'transcode_fail' });
      return;
    }
    await updateJob(job.id, { dest_status: dv.status, encode_progress: dv.encodeProgress });
    if (dv.status === 4) { await updateJob(job.id, { ready_at: new Date() }); break; }
    if (dv.status === 5 || dv.status === 6) {
      // Remove the bad dest copy so a retry re-fetches cleanly — but only clear the guid if the
      // delete actually succeeded, otherwise keep it so resume re-checks the same video (no duplicate).
      const deleted = await deleteVideo(dest, newGuid).catch(() => false);
      if (deleted) await updateJob(job.id, { new_video_guid: null });
      else await log.warn('failed dest copy could not be deleted; kept for retry', { ...tag, event: 'leak', data: { leakedGuid: newGuid } });
      await setState(job.id, 'failed', { error: `dest transcode failed (status ${dv.status})` });
      await log.error(`dest transcode failed status ${dv.status}`, { ...tag, event: 'transcode_fail' });
      return;
    }
    if (Date.now() - startWait > config.maxTranscodeWaitMs) {
      await setState(job.id, 'failed', { error: 'transcode timeout' }); // keep guid -> resume re-poll later
      await log.warn('transcode timeout; will resume on retry', { ...tag, event: 'timeout' });
      return;
    }
    await sleep(config.pollIntervalMs);
  }

  // ---- 4. VERIFY WATCHABLE ----
  await setState(job.id, 'verifying', {});
  let watchable = false;
  for (let i = 0; i < 5; i++) { if (await verifyWatchable(dest, newGuid)) { watchable = true; break; } await sleep(3000); }
  if (!watchable) {
    await setState(job.id, 'failed', { error: 'dest finished but playlist not reachable' });
    await log.error('verify failed: playlist not reachable', { ...tag, event: 'verify_fail' });
    return;
  }
  await log.info('verified watchable', { ...tag, event: 'verified', data: { newGuid } });

  // ---- 5. UPDATE blb.activities ----
  if (config.enableDbUpdate) {
    await setState(job.id, 'updating_db', {});
    const r = await updateActivitiesForJob(job, newGuid);
    const total = await updatedCountForJob(job.id);
    await updateJob(job.id, { db_updated_at: new Date(), activity_updated_count: total });
    await log.info(`db updated: ${r.updated}/${r.attempted} now -> ${newGuid} (total ${total}/${job.activity_count})`, { ...tag, event: 'db_update', data: r });
  } else {
    await log.warn('db update disabled (ENABLE_DB_UPDATE=false)', { ...tag, event: 'db_skip' });
  }

  // ---- 5b. LEG 2: redundant copy to dest2 (673029) + second-DB update ----
  if (config.enableSecondDb && job.second_db_present) {
    const dest2 = config.dest2;
    let newGuid2 = job.new_video_guid_2;

    // Need a fetchable source URL (persisted during resolve; re-resolve if missing).
    let srcUrl = job.source_url;
    let referer = `https://${creds.cdnHost}/`;
    if (!srcUrl) {
      const sv = await getVideo(creds, job.source_video_guid);
      const rs = sv ? await resolveSourceUrl(creds, sv) : null;
      if (!rs) { await setState(job.id, 'failed', { error: 'leg2: source not resolvable' }); await log.error('leg2: source not resolvable', { ...tag, event: 'fetch2_fail' }); return; }
      srcUrl = rs.url; referer = rs.referer;
    }

    if (!newGuid2) {
      const marker2 = `[mig2#${job.id}]`;
      const cleanTitle2 = (job.title || job.source_video_guid).slice(0, 680);
      if (job.attempts > 1) {
        const prior = await findVideosByTitleContains(dest2, dest2.collectionId, marker2);
        if (prior[0]) { newGuid2 = prior[0].guid; await log.info(`leg2 adopted prior video: ${newGuid2}`, { ...tag, event: 'adopt2' }); }
      }
      if (!newGuid2) {
        await setState(job.id, 'fetching', {});
        const fr2 = await fetchVideo(dest2, { url: srcUrl, headers: { Referer: referer }, title: `${cleanTitle2} ${marker2}`, collectionId: dest2.collectionId });
        await log.info(`leg2 fetch (http ${fr2.httpStatus}, guid ${fr2.guid ?? 'none'})`, { ...tag, event: 'fetch2', data: fr2.raw });
        newGuid2 = fr2.guid ?? (await findVideosByTitleContains(dest2, dest2.collectionId, marker2))[0]?.guid ?? null;
        if (!newGuid2) { await setState(job.id, 'failed', { error: 'leg2 fetch returned no guid' }); await log.error('leg2 fetch: no guid', { ...tag, event: 'fetch2_fail' }); return; }
      }
      await updateJob(job.id, { new_video_guid_2: newGuid2, fetched2_at: new Date(), dest2_status: 0, dest2_encode_progress: 0, dest2_collection_id: dest2.collectionId, dest2_url: srcUrl });
      try { await updateVideoTitle(dest2, newGuid2, cleanTitle2); } catch (e) { await log.warn(`leg2 title cleanup failed: ${(e as Error).message}`, { ...tag, event: 'rename2_fail' }); }
    }

    // poll leg2 transcode
    await setState(job.id, 'transcoding', {});
    const startWait2 = Date.now();
    for (;;) {
      if (ctx.abort.aborted) { await setState(job.id, 'transcoding', {}); return; }
      const dv2 = await getVideo(dest2, newGuid2);
      if (!dv2) { await updateJob(job.id, { new_video_guid_2: null }); await setState(job.id, 'failed', { error: 'leg2 dest vanished' }); await log.error('leg2 dest vanished', { ...tag, event: 'transcode2_fail' }); return; }
      await updateJob(job.id, { dest2_status: dv2.status, dest2_encode_progress: dv2.encodeProgress });
      if (dv2.status === 4) { await updateJob(job.id, { ready2_at: new Date() }); break; }
      if (dv2.status === 5 || dv2.status === 6) {
        const del = await deleteVideo(dest2, newGuid2).catch(() => false);
        if (del) await updateJob(job.id, { new_video_guid_2: null });
        else await log.warn('leg2 failed dest kept for retry', { ...tag, event: 'leak2', data: { leakedGuid: newGuid2 } });
        await setState(job.id, 'failed', { error: `leg2 transcode failed (status ${dv2.status})` });
        await log.error(`leg2 transcode failed status ${dv2.status}`, { ...tag, event: 'transcode2_fail' });
        return;
      }
      if (Date.now() - startWait2 > config.maxTranscodeWaitMs) { await setState(job.id, 'failed', { error: 'leg2 transcode timeout' }); await log.warn('leg2 transcode timeout', { ...tag, event: 'timeout2' }); return; }
      await sleep(config.pollIntervalMs);
    }

    // verify leg2 watchable
    await setState(job.id, 'verifying', {});
    let w2 = false;
    for (let i = 0; i < 5; i++) { if (await verifyWatchable(dest2, newGuid2)) { w2 = true; break; } await sleep(3000); }
    if (!w2) { await setState(job.id, 'failed', { error: 'leg2 finished but playlist not reachable' }); await log.error('leg2 verify failed', { ...tag, event: 'verify2_fail' }); return; }
    await log.info('leg2 verified watchable', { ...tag, event: 'verified2', data: { newGuid2 } });

    // update second DB (4 tables)
    if (config.enableDbUpdate) {
      await setState(job.id, 'updating_db', {});
      const r2 = await updateSecondDbForJob(job, newGuid2);
      const tot2 = await secondDbUpdatedCount(job.id);
      await updateJob(job.id, { second_db_updated_at: new Date(), second_db_rows_updated: tot2 });
      await log.info(`second-DB updated: ${r2.updated}/${r2.attempted} -> ${newGuid2} (total ${tot2}/${job.second_db_row_count})`, { ...tag, event: 'db_update2', data: r2 });
    } else {
      await log.warn('leg2 db update disabled (ENABLE_DB_UPDATE=false)', { ...tag, event: 'db_skip2' });
    }
  }

  // ---- 6. DELETE SOURCE (only after ALL active legs are verified + their DB rows updated) ----
  if (config.enableSourceDelete && config.enableDbUpdate) {
    const pending = await pendingMapCount(job.id);
    const updated = await updatedCountForJob(job.id);
    const leg2Pending = (config.enableSecondDb && job.second_db_present) ? await pendingSecondDbMapCount(job.id) : 0;
    if (pending === 0 && updated >= 1 && leg2Pending === 0) {
      await setState(job.id, 'deleting', {});
      await deleteVideo(creds, job.source_video_guid);
      await updateJob(job.id, { deleted_at: new Date() });
      await log.info('source video deleted', { ...tag, event: 'delete', data: { lib: job.source_library_id } });
    } else {
      await log.warn(`delete skipped: leg1 pending=${pending} (updated=${updated}), leg2 pending=${leg2Pending}`, { ...tag, event: 'delete_skip' });
    }
  } else {
    await log.info(`source delete ${config.enableSourceDelete ? 'pending (db update off)' : 'disabled'}`, { ...tag, event: 'delete_skip' });
  }

  // ---- 7. DONE ----
  await setState(job.id, 'done', { error: null });
  await log.info('DONE', { ...tag, event: 'done', data: { newGuid } });
}
