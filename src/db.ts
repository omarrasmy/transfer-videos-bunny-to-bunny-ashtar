import mysql, { type Pool, type PoolConnection, type RowDataPacket, type ResultSetHeader } from 'mysql2/promise';
import { config, type DbConfig } from './config.js';

function makePool(c: DbConfig): Pool {
  return mysql.createPool({
    host: c.host,
    port: c.port,
    user: c.user,
    password: c.password,
    database: c.database,
    connectionLimit: Math.max(4, config.concurrency + 4),
    connectTimeout: 30_000,
    enableKeepAlive: true,
    namedPlaceholders: false,
    timezone: 'Z',
  });
}

export const blbPool: Pool = makePool(config.blb);
export const transferPool: Pool = makePool(config.transfer);
// Second DB pool (the other system) — only created when leg 2 is enabled and configured.
export const secondPool: Pool | null =
  config.enableSecondDb && config.secondDb.host ? makePool(config.secondDb) : null;

export async function secondQuery<T extends RowDataPacket[]>(sql: string, params: unknown[] = []): Promise<T> {
  if (!secondPool) throw new Error('second DB pool not configured');
  const [rows] = await secondPool.query<T>(sql, params);
  return rows;
}
export async function secondExec(sql: string, params: unknown[] = []): Promise<ResultSetHeader> {
  if (!secondPool) throw new Error('second DB pool not configured');
  const [res] = await secondPool.query<ResultSetHeader>(sql, params);
  return res;
}

export async function blbQuery<T extends RowDataPacket[]>(sql: string, params: unknown[] = []): Promise<T> {
  const [rows] = await blbPool.query<T>(sql, params);
  return rows;
}
export async function tQuery<T extends RowDataPacket[]>(sql: string, params: unknown[] = []): Promise<T> {
  const [rows] = await transferPool.query<T>(sql, params);
  return rows;
}
export async function tExec(sql: string, params: unknown[] = []): Promise<ResultSetHeader> {
  const [res] = await transferPool.query<ResultSetHeader>(sql, params);
  return res;
}
export async function blbExec(sql: string, params: unknown[] = []): Promise<ResultSetHeader> {
  const [res] = await blbPool.query<ResultSetHeader>(sql, params);
  return res;
}

export async function closePools(): Promise<void> {
  await Promise.allSettled([blbPool.end(), transferPool.end(), ...(secondPool ? [secondPool.end()] : [])]);
}

const SCHEMA = `
CREATE TABLE IF NOT EXISTS bunny_transfer_jobs (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  source_library_id INT NOT NULL,
  source_video_guid VARCHAR(64) NOT NULL,
  source_collection_id VARCHAR(64) NULL,
  title VARCHAR(700) NULL,
  dest_library_id INT NOT NULL,
  dest_collection_id VARCHAR(64) NULL,
  new_video_guid VARCHAR(64) NULL,
  source_url VARCHAR(900) NULL,
  source_status INT NULL,
  dest_status INT NULL,
  encode_progress INT NULL DEFAULT 0,
  state VARCHAR(20) NOT NULL DEFAULT 'pending',
  attempts INT NOT NULL DEFAULT 0,
  activity_count INT NOT NULL DEFAULT 0,
  activity_updated_count INT NOT NULL DEFAULT 0,
  size_bytes BIGINT NULL,
  error TEXT NULL,
  worker_id INT NULL,
  started_at DATETIME NULL,
  fetched_at DATETIME NULL,
  ready_at DATETIME NULL,
  db_updated_at DATETIME NULL,
  deleted_at DATETIME NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  UNIQUE KEY uq_src (source_library_id, source_video_guid),
  KEY idx_state (state),
  KEY idx_newguid (new_video_guid)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bunny_transfer_activity_map (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  job_id BIGINT NOT NULL,
  activity_id INT NOT NULL,
  old_library_id INT NULL,
  old_video_guid VARCHAR(64) NULL,
  old_collection_id VARCHAR(64) NULL,
  new_library_id INT NULL,
  new_video_guid VARCHAR(64) NULL,
  new_collection_id VARCHAR(64) NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  updated_at DATETIME NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_activity (activity_id),
  KEY idx_job (job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bunny_transfer_logs (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  job_id BIGINT NULL,
  source_video_guid VARCHAR(64) NULL,
  level VARCHAR(10) NOT NULL DEFAULT 'info',
  event VARCHAR(48) NULL,
  message TEXT NULL,
  data JSON NULL,
  created_at TIMESTAMP(3) NOT NULL DEFAULT CURRENT_TIMESTAMP(3),
  KEY idx_job (job_id),
  KEY idx_created (created_at),
  KEY idx_event (event)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bunny_transfer_runs (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  mode VARCHAR(20) NOT NULL,
  live TINYINT NOT NULL DEFAULT 0,
  enable_db_update TINYINT NOT NULL DEFAULT 0,
  enable_source_delete TINYINT NOT NULL DEFAULT 0,
  config JSON NULL,
  note VARCHAR(255) NULL,
  started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

CREATE TABLE IF NOT EXISTS bunny_transfer_seconddb_map (
  id BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  job_id BIGINT NOT NULL,
  table_name VARCHAR(40) NOT NULL,
  row_id INT NOT NULL,
  video_column VARCHAR(40) NOT NULL,
  old_video_guid VARCHAR(64) NULL,
  old_library_id VARCHAR(50) NULL,
  old_collection_id VARCHAR(255) NULL,
  new_video_guid VARCHAR(64) NULL,
  new_library_id VARCHAR(50) NULL,
  new_collection_id VARCHAR(255) NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  updated_at DATETIME NULL,
  created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uq_row (table_name, row_id),
  KEY idx_job (job_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
`;

// Leg-2 columns added to the existing jobs table (idempotent).
const JOB_LEG2_COLUMNS = [
  `ADD COLUMN second_db_present TINYINT NOT NULL DEFAULT 0`,
  `ADD COLUMN second_db_row_count INT NOT NULL DEFAULT 0`,
  `ADD COLUMN second_db_rows_updated INT NOT NULL DEFAULT 0`,
  `ADD COLUMN new_video_guid_2 VARCHAR(64) NULL`,
  `ADD COLUMN dest2_status INT NULL`,
  `ADD COLUMN dest2_encode_progress INT NULL DEFAULT 0`,
  `ADD COLUMN dest2_collection_id VARCHAR(64) NULL`,
  `ADD COLUMN dest2_url VARCHAR(900) NULL`,
  `ADD COLUMN fetched2_at DATETIME NULL`,
  `ADD COLUMN ready2_at DATETIME NULL`,
  `ADD COLUMN second_db_updated_at DATETIME NULL`,
];

/** Idempotently create the queue/audit tables in the transfer DB. */
export async function migrate(): Promise<void> {
  const statements = SCHEMA.split(';').map((s) => s.trim()).filter(Boolean);
  for (const stmt of statements) {
    await transferPool.query(stmt);
  }
  // Enforce one destination video per job at the DB level (defence against duplicate adoption).
  try {
    await transferPool.query(`ALTER TABLE bunny_transfer_jobs ADD UNIQUE KEY uq_newguid (new_video_guid)`);
  } catch {
    /* already exists — ignore */
  }
  // Leg-2 columns (idempotent: ignore "duplicate column" errors).
  for (const clause of JOB_LEG2_COLUMNS) {
    try { await transferPool.query(`ALTER TABLE bunny_transfer_jobs ${clause}`); } catch { /* exists */ }
  }
  try { await transferPool.query(`ALTER TABLE bunny_transfer_jobs ADD UNIQUE KEY uq_newguid2 (new_video_guid_2)`); } catch { /* exists */ }
}

// ---- Single-run advisory lock (prevents two concurrent `run` processes corrupting jobs) ----
let lockConn: PoolConnection | undefined;

export async function acquireRunLock(): Promise<boolean> {
  lockConn = await transferPool.getConnection();
  // Wait briefly: a just-killed run's lock lingers until MySQL reaps the dead connection.
  const [rows] = await lockConn.query<RowDataPacket[]>(`SELECT GET_LOCK('bunny_transfer_run', 10) AS got`);
  if (rows[0]?.got === 1) return true;
  lockConn.release();
  lockConn = undefined;
  return false;
}

export async function releaseRunLock(): Promise<void> {
  if (!lockConn) return;
  try { await lockConn.query(`SELECT RELEASE_LOCK('bunny_transfer_run')`); } catch { /* ignore */ }
  lockConn.release();
  lockConn = undefined;
}
