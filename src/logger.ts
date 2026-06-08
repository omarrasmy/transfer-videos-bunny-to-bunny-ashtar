import { tExec } from './db.js';

type Level = 'info' | 'warn' | 'error' | 'debug';

export interface LogFields {
  jobId?: number | null;
  guid?: string | null;
  event?: string;
  data?: unknown;
}

function ts(): string {
  return new Date().toISOString().replace('T', ' ').replace('Z', '');
}

async function persist(level: Level, message: string, f: LogFields): Promise<void> {
  try {
    await tExec(
      `INSERT INTO bunny_transfer_logs (job_id, source_video_guid, level, event, message, data)
       VALUES (?, ?, ?, ?, ?, ?)`,
      [
        f.jobId ?? null,
        f.guid ?? null,
        level,
        f.event ?? null,
        message?.slice(0, 65000) ?? null,
        f.data === undefined ? null : JSON.stringify(f.data),
      ],
    );
  } catch (e) {
    // Never let logging break the pipeline.
    console.error(`[${ts()}] (log-persist-failed) ${(e as Error).message}`);
  }
}

function line(level: Level, message: string, f: LogFields): string {
  const tag = f.jobId ? `job#${f.jobId}` : f.guid ? f.guid.slice(0, 8) : '-';
  const ev = f.event ? ` ${f.event}` : '';
  return `[${ts()}] ${level.toUpperCase().padEnd(5)} [${tag}]${ev} ${message}`;
}

export const log = {
  async info(message: string, f: LogFields = {}) {
    console.log(line('info', message, f));
    await persist('info', message, f);
  },
  async warn(message: string, f: LogFields = {}) {
    console.warn(line('warn', message, f));
    await persist('warn', message, f);
  },
  async error(message: string, f: LogFields = {}) {
    console.error(line('error', message, f));
    await persist('error', message, f);
  },
  /** Console-only (high-frequency progress); not persisted. */
  debug(message: string, f: LogFields = {}) {
    if (process.env.DEBUG) console.log(line('debug', message, f));
  },
};
