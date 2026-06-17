import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import type { RowDataPacket } from 'mysql2';
import { config, configSummary } from './config.js';
import { tQuery } from './db.js';
import { stateCounts } from './jobs.js';
import { workerViews } from './pool.js';
import { BUNNY_STATUS } from './types.js';

const DASH = fileURLToPath(new URL('../public/dashboard.html', import.meta.url));

const JOB_COLS = `id, source_library_id, source_video_guid, new_video_guid, title, state,
  dest_status, encode_progress, attempts, activity_count, activity_updated_count,
  size_bytes, error, worker_id, source_url, source_path,
  started_at, fetched_at, ready_at, db_updated_at, deleted_at, updated_at, created_at`;

async function status() {
  const counts = await stateCounts();
  const total = counts.reduce((a, c) => a + c.n, 0);
  const by = Object.fromEntries(counts.map((c) => [c.state, c.n]));
  const active = await tQuery<RowDataPacket[]>(
    `SELECT ${JOB_COLS} FROM bunny_transfer_jobs
      WHERE state IN ('claimed','resolving','fetching','transcoding','verifying','updating_db','deleting')
      ORDER BY updated_at DESC LIMIT 50`,
  );
  const recent = await tQuery<RowDataPacket[]>(
    `SELECT ${JOB_COLS} FROM bunny_transfer_jobs ORDER BY updated_at DESC LIMIT 60`,
  );
  const logs = await tQuery<RowDataPacket[]>(
    `SELECT id, job_id, source_video_guid, level, event, message, created_at
       FROM bunny_transfer_logs ORDER BY id DESC LIMIT 120`,
  );
  return {
    now: new Date().toISOString(),
    config: configSummary(),
    statusNames: BUNNY_STATUS,
    totals: { total, ...by },
    workers: workerViews,
    active,
    recent,
    logs,
  };
}

function json(res: http.ServerResponse, code: number, body: unknown) {
  const s = JSON.stringify(body);
  res.writeHead(code, { 'content-type': 'application/json', 'cache-control': 'no-store' });
  res.end(s);
}

export function startServer(): http.Server {
  const server = http.createServer(async (req, res) => {
    try {
      const url = new URL(req.url ?? '/', `http://localhost`);
      if (url.pathname === '/' || url.pathname === '/index.html') {
        const html = await readFile(DASH, 'utf8');
        res.writeHead(200, { 'content-type': 'text/html; charset=utf-8' });
        res.end(html);
        return;
      }
      if (url.pathname === '/api/status') return json(res, 200, await status());
      if (url.pathname === '/api/jobs') {
        const state = url.searchParams.get('state');
        const limit = Math.min(500, Number(url.searchParams.get('limit') ?? 200));
        const where = state ? `WHERE state = ?` : '';
        const params = state ? [state] : [];
        const rows = await tQuery<RowDataPacket[]>(
          `SELECT ${JOB_COLS} FROM bunny_transfer_jobs ${where} ORDER BY updated_at DESC LIMIT ${limit}`, params);
        return json(res, 200, rows);
      }
      if (url.pathname === '/api/logs') {
        const jobId = url.searchParams.get('jobId');
        const limit = Math.min(1000, Number(url.searchParams.get('limit') ?? 200));
        const where = jobId ? `WHERE job_id = ?` : '';
        const params = jobId ? [Number(jobId)] : [];
        const rows = await tQuery<RowDataPacket[]>(
          `SELECT id, job_id, source_video_guid, level, event, message, data, created_at
             FROM bunny_transfer_logs ${where} ORDER BY id DESC LIMIT ${limit}`, params);
        return json(res, 200, rows);
      }
      res.writeHead(404); res.end('not found');
    } catch (e) {
      json(res, 500, { error: (e as Error).message });
    }
  });
  // The dashboard is auxiliary — never let a port problem crash the migration.
  let port = config.dashboardPort;
  let attempts = 0;
  const tryListen = () => server.listen(port, () => console.log(`\n  Dashboard:  http://localhost:${port}\n`));
  server.on('error', (err: NodeJS.ErrnoException) => {
    if (err.code === 'EADDRINUSE' && attempts < 8) { attempts++; port++; setTimeout(tryListen, 100); }
    else console.warn(`  Dashboard disabled (${err.message}); migration continues without it.`);
  });
  tryListen();
  return server;
}
