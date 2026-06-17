/** Lifecycle states for a transfer job (one per unique source video). */
export type JobState =
  | 'pending'      // queued, not yet claimed
  | 'claimed'      // a worker has taken it
  | 'resolving'    // looking up source video + choosing a fetch URL
  | 'fetching'     // fetch request submitted to destination
  | 'transcoding'  // destination is processing/transcoding
  | 'verifying'    // destination finished; confirming it is watchable
  | 'updating_db'  // updating blb.activities rows
  | 'deleting'     // deleting the source video
  | 'done'         // fully complete
  | 'failed'       // transient/needs-retry failure (attempts < max)
  | 'skipped';     // terminal: source missing/unfetchable/already-migrated

export const ACTIVE_STATES: JobState[] = ['claimed', 'resolving', 'fetching', 'transcoding', 'verifying', 'updating_db', 'deleting'];
export const RESUMABLE_STATES: JobState[] = ['pending', 'failed', ...ACTIVE_STATES];
export const TERMINAL_STATES: JobState[] = ['done', 'skipped'];

export interface JobRow {
  id: number;
  source_library_id: number;
  source_video_guid: string;
  source_collection_id: string | null;
  title: string | null;
  dest_library_id: number;
  dest_collection_id: string | null;
  new_video_guid: string | null;
  source_url: string | null;
  source_path: string | null;   // activities.video_source_path — the GCS object key for the fallback
  source_status: number | null;
  dest_status: number | null;
  encode_progress: number | null;
  state: JobState;
  attempts: number;
  activity_count: number;
  activity_updated_count: number;
  error: string | null;
  worker_id: number | null;
  size_bytes: number | null;
  started_at: Date | null;
  fetched_at: Date | null;
  ready_at: Date | null;
  db_updated_at: Date | null;
  deleted_at: Date | null;
  created_at: Date;
  updated_at: Date;
  // ---- Leg 2 (redundant copy to 673029 + second-DB update) ----
  second_db_present: number;
  second_db_row_count: number;
  second_db_rows_updated: number;
  new_video_guid_2: string | null;
  dest2_status: number | null;
  dest2_encode_progress: number | null;
  dest2_collection_id: string | null;
  dest2_url: string | null;
  fetched2_at: Date | null;
  ready2_at: Date | null;
  second_db_updated_at: Date | null;
}

/**
 * The second DB's video-bearing tables to rewrite, with their column names.
 * `plans` uses `video_`-prefixed columns; the others use the plain names.
 */
export interface SecondDbTable { table: string; videoCol: string; libCol: string; collectionCol: string; }
export const SECOND_DB_TABLES: SecondDbTable[] = [
  { table: 'activities',       videoCol: 'bunny_video_id',       libCol: 'bunny_library_id',       collectionCol: 'bunny_collection_id' },
  { table: 'lesson_resources', videoCol: 'bunny_video_id',       libCol: 'bunny_library_id',       collectionCol: 'bunny_collection_id' },
  { table: 'resources',        videoCol: 'bunny_video_id',       libCol: 'bunny_library_id',       collectionCol: 'bunny_collection_id' },
  { table: 'plans',            videoCol: 'video_bunny_video_id', libCol: 'video_bunny_library_id', collectionCol: 'video_bunny_collection_id' },
];

/** Subset of Bunny's VideoModel we rely on. */
export interface BunnyVideo {
  guid: string;
  title: string;
  status: number;          // 0 Created,1 Uploaded,2 Processing,3 Transcoding,4 Finished,5 Error,6 UploadFailed,7 JitSegmenting,8 JitPlaylistsCreated
  encodeProgress: number;
  length: number;
  availableResolutions: string | null;
  hasMP4Fallback: boolean;
  storageSize: number;
  collectionId?: string;
  dateUploaded?: string;
}

export const BUNNY_STATUS: Record<number, string> = {
  0: 'Created', 1: 'Uploaded', 2: 'Processing', 3: 'Transcoding',
  4: 'Finished', 5: 'Error', 6: 'UploadFailed', 7: 'JitSegmenting', 8: 'JitPlaylistsCreated',
};
