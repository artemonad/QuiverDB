# QuiverDB Snapshots (Phase 1 + Phase 2)

Status
- Phase 1: in‑process snapshots (do not survive restarts), no on-disk format changes.
- Phase 2: persisted snapshots (registry), SnapStore (content-addressed dedup), hashindex and compact.

On-disk formats remain frozen: meta v3, page v2, WAL v1.

Contents
- Overview
- Semantics and invariants
- Write path and COW (freeze)
- Sidecar (Phase 1): freeze.bin and index.bin
- SnapStore (Phase 2): store.bin and index.bin, hashindex.bin, refcount and compact
- Reading under a snapshot (live/frozen/snapstore fallback)
- Backup/Restore (full/incremental)
- CLI and ENV
- Metrics
- Performance
- Troubleshooting
- Examples

---

## Overview

Snapshots provide a consistent database view “as of” a specific LSN (snapshot_lsn):
- Phase 1: in-process snapshots with page-level copy-on-write (COW).
- Phase 2: persisted snapshots with a registry under `.snapshots` and a shared content-addressed store (SnapStore) for deduplicating frozen pages by content hash (with refcount).

Key ideas:
- Each v2 page stores `lsn` and a whole-page CRC.
- Before overwriting/freeing a v2 page that may be needed by an active snapshot, the current image is frozen (COW).
- Readers pick live vs frozen per page “as of snapshot_lsn”. In Phase 2, when sidecar is missing, readers can fallback to SnapStore by hash.

---

## Semantics and invariants

- As-of-LSN view: for v2 pages, the effective image is the one whose `page_lsn ≤ snapshot_lsn`.
- If the live page is newer (`page_lsn > snapshot_lsn`), use a frozen image (Phase 1 sidecar) or SnapStore (Phase 2).
- Tail-wins within a bucket chain remains in effect at the snapshot boundary.
- Directory heads are not frozen; correctness is guaranteed by freezing pages and a rare fallback that reconstructs tail-wins when hot chain mutations occur after the snapshot.
- Integrity: CRC is verified on read; both sidecar and snapstore frames carry their own CRCs.

---

## Write path and COW (freeze)

On a v2 page update:
1. If there are active snapshots with `snapshot_lsn ≥ current_page_lsn`, freeze the current page for each such snapshot.
2. KV and OVERFLOW pages are frozen before overwrite or free.
3. Freeing overflow chains and cutting empty KV pages from chains also triggers freeze of those pages when needed by snapshots.

Freezing a page:
- Phase 1: write the page into sidecar `<root>/.snapshots/<id>/freeze.bin`, append an entry to `index.bin`.
- Phase 2: additionally (best-effort) put the same payload into the shared SnapStore (dedup/refcount) and write `hashindex.bin` in the sidecar.

---

## Sidecar (Phase 1): freeze.bin and index.bin

Per-snapshot directory:
- `<root>/.snapshots/<snapshot_id>/`

Files:
- `freeze.bin`: append-only frames for frozen page images.
- `index.bin`: append-only mapping `page_id → offset` in `freeze.bin`.

Frame format (freeze.bin):
- Header (24 bytes):
  - `page_id u64`
  - `page_lsn u64`
  - `page_len u32` (equals page_size)
  - `crc32 u32` (CRC over header bytes [0..20] + payload)
- Payload: full page image (page_size bytes)

Index format (index.bin):
- Records `[page_id u64][offset u64][page_lsn u64]`, last one for a given page_id wins.

Notes:
- Freeze is written without fsync to keep latency low; CRC is verified on read.
- Backup retries index refresh (with small sleeps) to capture freshly written frames under hot load.

---

## SnapStore (Phase 2): store.bin and index.bin, hashindex.bin, refcount and compact

Goal: deduplicate identical frozen page images across snapshots and retain a single content copy with reference counting.

SnapStore directory:
- Default: `<root>/.snapstore`
- Configurable via `P1_SNAPSTORE_DIR`:
  - absolute path is used as-is,
  - relative path is resolved against `<root>`.

Files:
- `store.bin`: frames `[hash u64][len u32][crc32 u32] + payload(page_size)`
- `index.bin`: full mapping of `hash → (offset u64, refcnt u32, pad u32)`

Per-snapshot sidecar addition:
- `hashindex.bin`: `[page_id u64][hash u64][page_lsn u64]` (for fallback reads when freeze.bin is missing)

Rules:
- Freezing identical content for multiple snapshots:
  - first `put(page_bytes)` creates a new frame (refcnt=1),
  - subsequent snapshots increment references via `add_ref(hash)`.
- Removing a snapshot (`snaprm`) decrements references (`dec_ref`) for all hashes listed in its `hashindex.bin`.
- `snapcompact` rewrites `store.bin` to keep only frames with `refcnt > 0`, updates offsets in `index.bin`.

Fallback read (when `freeze.bin` is missing) uses `hashindex.bin` to fetch pages from SnapStore by hash.

---

## Reading under a snapshot (live/frozen/snapstore fallback)

For each `page_id`:
1. Read live page:
  - If it is v2 and `page_lsn ≤ snapshot_lsn`, use live.
  - If it is v2 and `page_lsn > snapshot_lsn`, use frozen (Phase 1) or SnapStore (Phase 2).
  - If it is non-v2, use live.
2. If live is unavailable/invalid:
  - Phase 1: read from sidecar using `index.bin` → `freeze.bin`.
  - Phase 2: if sidecar has no `freeze.bin`, use SnapStore by `hashindex.bin`.
3. Hot chain mutations: if head/page movement makes straightforward traversal ineffective, a rare fallback scan reconstructs the tail-most value “as of snapshot” (tail-wins). Metric: `snapshot_fallback_scans`.

Overflow chains are resolved page-by-page using the same per-page decision (live vs frozen vs snapstore fallback).

Note on “raw fallback”:
- If a live page looks like v2 (valid MAGIC and version) but the KV/OVF header cannot be parsed, the reader still attempts frozen/snapstore and may last-resort use the live image best-effort. This is extremely rare and primarily defensive.

---

## Backup/Restore (full/incremental)

Backup:
- Taken at snapshot `S`:
  - Full: all pages “as of S”.
  - Incremental: only pages with `page_lsn ∈ (since_lsn, S]`.
- Output:
  - `pages.bin`: concatenated frames (same header as `freeze.bin`)
  - `dir.bin`: a copy of `<root>/dir` (if present)
  - `manifest.json`: summary (`snapshot_lsn`, `since_lsn`, `page_size`, counters)

Restore:
- Initialize target DB path if missing.
- Write pages from `pages.bin` back to their page_ids (raw).
- Optionally install `dir.bin`.
- Set `meta.last_lsn = max seen`, mark `clean_shutdown = true`.

CRC is verified on read; partial tails are handled as EOF.

---

## CLI and ENV

Commands:
- Snapshots / Backup / Restore:
  - `quiverdb backup --path ./db --out ./backup [--since-lsn N]`
  - `quiverdb restore --path ./dst --from ./backup [--verify]`
- Phase 2 snapshots and snapstore:
  - `quiverdb snapshots --path ./db [--json]` (lists `.snapshots/registry.json`)
  - `quiverdb snaprm --path ./db --id <snapshot_id>` (dec_ref + delete sidecar; mark ended)
  - `quiverdb snapcompact --path ./db [--json]` (compact snapstore)

Environment:
- Phase 1 / Core:
  - `P1_WAL_COALESCE_MS`, `P1_DATA_FSYNC`, `P1_PAGE_CACHE_PAGES`, `P1_OVF_THRESHOLD_BYTES`
- Phase 2 (snapshots/snapstore):
  - `P1_SNAP_PERSIST=0|1` — keep sidecar after end and track in `.snapshots/registry.json`
  - `P1_SNAP_DEDUP=0|1` — enable SnapStore dedup and sidecar `hashindex.bin`
  - `P1_SNAPSTORE_DIR=path` — custom SnapStore directory (absolute or relative to `<root>`)
- CDC (for completeness): `P1_SHIP_*`, `P1_APPLY_DECOMPRESS`

---

## Metrics

- `snapshots_active`
- `snapshot_freeze_frames`
- `snapshot_freeze_bytes`
- `backup_pages_emitted`
- `backup_bytes_emitted`
- `restore_pages_written`
- `restore_bytes_written`
- `snapshot_fallback_scans` (rare fallback when chains mutate hotly)
- Common metrics: WAL (appends/bytes/fsyncs/truncations), page cache (hits/misses), overflow chains (created/freed), orphan sweeps.

CLI: `quiverdb metrics [--json]`.

---

## Performance

- Writer:
  - A page is frozen at most once per snapshot interval (first overwrite/free after snapshot).
  - Dedup (Phase 2) reduces space: identical frames are stored once in SnapStore.
- Reader:
  - The fast path is live pages; frozen/snapstore access uses lightweight maps (index/hash).
  - The fallback scan is O(total_pages) and rare.
- Guidance:
  - Keep the number of long-living snapshots small.
  - Monitor `snapshot_freeze_*` and `snapshot_fallback_scans` under write-heavy load.

---

## Troubleshooting

- “Snapshot shows wrong/missing value under churn”:
  - Ensure you run Phase 1/2 builds: pages are frozen before free and fallback logic is present.
  - Check metrics: `snapshot_freeze_frames/bytes` should be > 0 on hot paths.
- “Restore reports CRC mismatch in pages.bin”:
  - Re-create the backup; optionally use `--verify` after restore.
- “Removing a snapshot does not shrink snapstore”:
  - Use `quiverdb snapcompact` to reclaim frames with `refcnt == 0`.
- “hashindex missing for fallback”:
  - Ensure `P1_SNAP_DEDUP=1` was enabled during freeze; sidecar `hashindex.bin` is written only in Phase 2 dedup mode.
- SnapStore directory:
  - Verify `P1_SNAPSTORE_DIR` (absolute vs relative to `<root>`).

---

## Examples

Rust (snapshot + read):
```rust
use anyhow::Result;
use QuiverDB::{Db, Directory, init_db};

fn main() -> Result<()> {
    let root = std::path::Path::new("./db");
    if !root.exists() { init_db(root, 4096)?; Directory::create(root, 128)?; }

    let mut db = Db::open(root)?;
    db.put(b"k", b"v0")?;

    let mut s = db.snapshot_begin()?;
    db.put(b"k", b"v1")?;

    let got = s.get(b"k")?.unwrap();
    assert_eq!(got.as_slice(), b"v0");
    db.snapshot_end(&mut s)?;
    Ok(())
}
```

CLI (full/incremental backup + restore):
```bash
quiverdb backup  --path ./db --out ./backup_full
quiverdb restore --path ./db_restored --from ./backup_full --verify

LSN=$(quiverdb cdc last-lsn --path ./db_restored)
quiverdb backup --path ./db --out ./backup_incr --since-lsn "$LSN"
quiverdb restore --path ./db_restored --from ./backup_incr --verify
```

Persisted snapshots + snapstore:
```bash
# Enable persisted snapshots and dedup
export P1_SNAP_PERSIST=1
export P1_SNAP_DEDUP=1
# Optional: put snapstore elsewhere
# export P1_SNAPSTORE_DIR=/data/quiver_snapstore

quiverdb snapshots --path ./db --json
quiverdb snaprm --path ./db --id <snapshot_id>
quiverdb snapcompact --path ./db
```