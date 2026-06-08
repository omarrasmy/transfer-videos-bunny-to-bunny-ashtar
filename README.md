# transfer-bunny-lib

Migrate Bunny Stream videos **library → library**, fully server-side (Bunny's `fetch` API — no
local/Firebase staging), driven by the `blb.activities` table. DB-backed queue with **8 parallel
workers**, a **live dashboard**, full audit logging, idempotent + restart-safe.

## What it does
For each **distinct** source video referenced by `activities` (type `Video`) belonging to the
configured teachers:
1. Resolve a fetchable source URL (`/{guid}/original`, else highest `play_{res}.mp4`) — the source
   pull zones use **referer/hotlink protection**, so requests carry a `Referer` header (no token signing).
2. `POST /library/{dest}/videos/fetch` with that URL + `Referer` header → Bunny downloads & transcodes.
3. Poll the destination video until **status 4 (Finished)** and confirm the HLS playlist is reachable.
4. Update **every** `activities` row that references the video (`bunny_video_id`, `bunny_library_id`,
   `bunny_collection_id`). A video referenced by many activities is transferred **once**.
5. Delete the source video (gated — see safety).

## Selection
Activities where `type = ACTIVITY_TYPE` and the video belongs to a teacher in `TEACHERS`, via
`subject.teacher` **or** `lesson.subject.teacher` (`LINKAGE=union|subject|lesson`, default `union`).
Rows with no `bunny_video_id`, or already in the destination library, are ignored.

## Safety model (three independent switches in `.env`)
| Flag | Effect when `false` |
|------|---------------------|
| `LIVE` | No writes to Bunny or `blb` at all. `simulate` runs resolve-only. |
| `ENABLE_DB_UPDATE` | Destination videos are created, but `blb.activities` is **not** modified. |
| `ENABLE_SOURCE_DELETE` | Source videos are **never** deleted (use `delete-phase` later). |

A source is deleted **only** after: destination is Finished + playlist verified + all referencing
`activities` rows updated. Unrecoverable sources (404 / UploadFailed) are terminally **skipped**, never deleted.

## Commands
```bash
npm run migrate        # create queue/audit tables in the `transfer` DB
npm run scan           # build/refresh the job set from blb (no Bunny/blb writes)
npm run simulate       # scan + resolve-only dry run (read-only) + dashboard
npm run serve          # dashboard only
npm run test-one       # controlled single live transfer into dest, then delete the test copy
npm run run            # live migration (8 workers) + dashboard   (requires LIVE=true)
npm run delete-phase   # delete sources for completed+updated jobs (LIVE + ENABLE_SOURCE_DELETE)
npm run stats          # job-state counts
```
Dashboard: http://localhost:4545

## Audit tables (in the `transfer` DB)
- `bunny_transfer_jobs` — one row per unique source video; full lifecycle state + timestamps.
- `bunny_transfer_activity_map` — every `activities` row touched, with before/after values.
- `bunny_transfer_logs` — timestamped event log.
- `bunny_transfer_runs` — one row per invocation (mode + active flags).

## Resumability
Re-running `scan` is idempotent (won't duplicate jobs or reset progress). On restart, in-flight jobs
are returned to a resumable state; a job that already has a destination GUID **resumes polling**
instead of re-fetching, so transfers are never duplicated.
