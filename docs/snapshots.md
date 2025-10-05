# QuiverDB Snapshots (v1.2 Phase 1)

Status: Phase 1 implemented (in-process snapshots).  
On-disk formats remain frozen: meta v3, page v2, WAL v1.

This document describes snapshot isolation for consistent reads by LSN, how page-level COW is implemented, sidecar formats, rust APIs, CLI for backup/restore, metrics, performance, and limitations.

Contents
- Overview
- Semantics and invariants
- Write path and COW (freeze)
- Sidecar formats (freeze.bin, index.bin)
- Reading under a snapshot (KV + overflow)
- Backup/Restore (full and incremental)
- API and CLI
- Metrics
- Performance notes
- Limitations (Phase 1)
- Examples
- Troubleshooting
- Roadmap (Phase 2)

---

## 1) Overview

- Snapshots provide a consistent read view of the database at a specific LSN, called snapshot_lsn.
- A snapshot is read-only and in-process (Phase 1). It does not survive process restarts.
- Writer remains single-writer; snapshots do not take any write locks.
- Consistency relies on:
    - LSN in v2 page headers,
    - page-level copy-on-write (freeze) of pages that a snapshot may need,
    - reading logic which selects, for each page, the version “as of” snapshot_lsn.

Why in-process? It keeps complexity low while we build the core capability without changing the frozen disk formats. Phase 2 adds persisted snapshots and a shared freeze store.

---

## 2) Semantics and invariants

- A snapshot exposes the database contents “as of” snapshot_lsn:
    - For a v2 page (KV_RH or OVERFLOW), the effective page image is the one with page_lsn ≤ snapshot_lsn.
    - If a v2 page in data files is newer (page_lsn > snapshot_lsn), the snapshot uses a frozen copy written before the page was overwritten or freed.
- Tail-wins in a bucket chain remains in effect at the snapshot boundary: if a key appears multiple times in a chain, the last (closer to the tail) is the authoritative one for get/scan.
- Directory heads are read live (not frozen). Page-level freeze guarantees correctness even if the live chain structure changes after snapshot_lsn:
    - If snapshot traversal hits a page newer than snapshot_lsn or a missing “old” page, the reader switches to a fallback scan which reconstructs the tail-most version “as of snapshot” by walking page links captured at snapshot_lsn (see “Reading”).
- No on-disk format changes were introduced.

---

## 3) Write path and COW (freeze)

The writer freezes pages that a snapshot might need before they are overwritten or freed.

- Pager.commit_page(page_id, new_page):
    - Before the write, if there are active snapshots with snapshot_lsn ≥ current page_lsn,
      the current page bytes are appended to the snapshot’s freeze store (if not already frozen).
- Freeing overflow chains (large values):
    - Before freeing any OVERFLOW page in the chain, the writer freezes that page for any active snapshot that could need it.
- Removing empty KV pages from chains (after deletes/compaction):
    - Before freeing the empty KV page, the writer freezes it if any snapshot might need it.

Freeze logic is triggered via SnapshotManager::freeze_if_needed(page_id, page_lsn, page_bytes), which:
- Checks if any active snapshot has snapshot_lsn ≥ page_lsn and the page hasn’t been frozen for that snapshot.
- Appends a frame to the snapshot’s freeze.bin and an index entry to index.bin.
- Records metrics: snapshot_freeze_frames and snapshot_freeze_bytes.

---

## 4) Sidecar formats (per-snapshot)

All integers are Little Endian (LE). Sidecar lives under:
- <root>/.snapshots/<snapshot_id>/

Files:
- freeze.bin: append-only frames of frozen page images
- index.bin: append-only mapping page_id → last offset in freeze.bin

Frame layout (freeze.bin):
- Header (24 bytes):
    - page_id u64
    - page_lsn u64
    - page_len u32 (= page_size)
    - crc32 u32 (CRC over header[0..20] + payload)
- Payload:
    - Full page image (page_size bytes)

Index entry (index.bin):
- [page_id u64][offset u64][page_lsn u64]
- The reader loads index.bin to build an in-memory HashMap<page_id, offset>.

Note:
- Freeze pages are written without fsync to keep latency low.
- CRC is always verified on read.

---

## 5) Reading under a snapshot

Core rule for a page_id:
- Read live page. If it’s v2 and page_lsn ≤ snapshot_lsn, use live.
- If it’s v2 and page_lsn > snapshot_lsn, use frozen (from index.bin + freeze.bin).
- If it’s not v2, use live.
- If live read fails (e.g., missing after crash), try frozen.

KV pages (KV_RH):
- Snapshot traverses bucket chain, page by page, selecting each page as-of snapshot_lsn as above.
- If a page encountered is not KV (unexpected) or an old page cannot be located, traversal of this chain stops to avoid incorrect reads.

Overflow chains:
- Each OVERFLOW page is resolved independently: apply the same rule (live if page_lsn ≤ snapshot_lsn, otherwise frozen) for every page in the overflow chain.
- This guarantees correct old value reconstruction for large values.

Fallback (rare, but robust):
- If a normal chain traversal can’t find the key (due to head movements after snapshot), snapshot falls back to a full-page scan “as-of snapshot_lsn”:
    - Collect candidate pages with the key and next_page_id (all as-of snapshot).
    - Compute the tail-most candidate by walking snapshot next_page_id links.
    - Return the tail-most value (resolving overflow as-of snapshot).

This fallback is O(total_pages) and only used when the normal traversal finds nothing. In practice it’s rare, because writer freezes pages before changing chain structure.

---

## 6) Backup/Restore (full or incremental)

Backup is implemented on top of snapshots:
- Begin a snapshot S.
- For each allocated page_id:
    - Select page bytes as-of S (live if page_lsn ≤ S, otherwise frozen).
    - Optionally filter by since_lsn: only (page_lsn in (since_lsn, S]] are included.
- Output files:
    - pages.bin: concatenated frames with the same header as freeze.bin frames
    - dir.bin: a byte-for-byte copy of <root>/dir (if present)
    - manifest.json: summary including snapshot_lsn, since_lsn, page_size, pages_emitted, bytes_emitted, dir_present, dir_bytes
- Index refresh:
    - If the frozen page is not yet visible in index.bin, backup retries a few times (with short sleep and index re-scan) to catch freshly written freeze frames (hot backup).

Restore:
- Initialize/ensure target DB path.
- Read pages.bin frames and write pages back to their page_id (raw writes).
- Optionally install dir.bin as <root>/dir.
- Set meta.last_lsn to the max of frames, mark clean_shutdown=true.

Incremental backup:
- Use since_lsn = N to include only pages with page_lsn ∈ (N, S].
- Apply on top of a full backup at or before N to get the target state at S.

No deduplication in Phase 1; that will be added in Phase 2.

---

## 7) API and CLI

Rust API:
- Begin/End snapshot
    - Db::snapshot_begin() -> SnapshotHandle
        - SnapshotHandle::lsn() -> u64
        - SnapshotHandle::get(key: &[u8]) -> Result<Option<Vec<u8>>>
        - SnapshotHandle::scan_all() / scan_prefix(prefix: &[u8])
        - SnapshotHandle::end()
- Transparent reads:
    - SnapshotHandle uses live/frozen pages and overflow chains at snapshot time.

Backup/Restore (Rust):
- backup::backup_to_dir(&db, &snap, out: &Path, since_lsn: Option<u64>)
- backup::restore_from_dir(dst_root: &Path, backup_dir: &Path)

CLI:
- Full or incremental backup
    - quiverdb backup --path ./db --out ./backup_dir [--since-lsn N]
- Restore
    - quiverdb restore --path ./restored --from ./backup_dir [--verify]
        - --verify runs a strict check after restore
- Note: snapshot begin/end CLI is optional for Phase 1 (snapshots are in-process). The backup command manages snapshot begin/end internally.

---

## 8) Metrics

Snapshot/Backup/Restore:
- snapshots_active
- snapshot_freeze_frames
- snapshot_freeze_bytes
- backup_pages_emitted
- backup_bytes_emitted
- restore_pages_written
- restore_bytes_written

WAL/Cache (for completeness):
- wal_appends_total, wal_fsync_calls, wal_truncations, etc.
- page_cache_hits/page_cache_misses

CLI:
- quiverdb metrics [--json]

---

## 9) Performance notes

- Writer overhead:
    - The first time a page is overwritten after snapshot_lsn, the old image is frozen: this is a one-time cost per (snapshot, page).
    - Freezing overflow and empty KV pages before free adds some write volume during churn (bounded by pages touched).
- Reader overhead:
    - Normal path is fast (live disks or frozen lookup via a hash map).
    - Fallback scan is O(total_pages) and kicks in only when normal traversal finds nothing for the key (rare).
- Practical guidance:
    - Keep long-living snapshots few (e.g., 1–4) to control freeze volume.
    - Use snapshots for backup windows or consistent reads that need isolation from concurrent writes.

---

## 10) Limitations (Phase 1)

- Snapshots are in-process and do not survive process restarts.
- Directory heads are not frozen; correctness is ensured by page-level freeze + fallback logic (rarely triggered).
- Backup has no dedup in Phase 1 (Phase 2 will add manifest+blobs with content-hash dedup).
- No streaming snapshot API yet; use backup_to_dir if you need to capture a consistent state while the writer runs.

---

## 11) Examples

Rust (snapshot isolation):

```rust
use anyhow::Result;
use QuiverDB::{Db, Directory, init_db};

fn main() -> Result<()> {
    let root = std::path::Path::new("./db");
    if !root.exists() {
        init_db(root, 4096)?;
        Directory::create(root, 128)?;
    }

    // Writer: prepare initial data
    let mut db = Db::open(root)?;
    db.put(b"k", b"v0")?;

    // Begin snapshot S
    let mut snap = db.snapshot_begin()?;
    let s_lsn = snap.lsn();

    // Update live values after snapshot
    db.put(b"k", b"v1")?;

    // Snapshot must still see old value
    let v = snap.get(b"k")?.unwrap();
    assert_eq!(v.as_slice(), b"v0");

    // End snapshot
    db.snapshot_end(&mut snap)?;
    Ok(())
}
```

Rust (backup/restore):

```rust
use anyhow::Result;
use std::path::Path;
use QuiverDB::{Db, Directory, init_db};
use QuiverDB::backup::{backup_to_dir, restore_from_dir};

fn main() -> Result<()> {
    let root = Path::new("./db");
    let out = Path::new("./backup_out");
    let dst = Path::new("./db_restored");

    if !root.exists() {
        init_db(root, 4096)?;
        Directory::create(root, 128)?;
    }

    // Put some data
    {
        let mut db = Db::open(root)?;
        db.put(b"a", b"1")?;
        db.put(b"b", b"2")?;
    }

    // Snapshot + backup (full)
    {
        let mut db = Db::open(root)?;
        let mut snap = db.snapshot_begin()?;
        backup_to_dir(&db, &snap, out, None)?;
        db.snapshot_end(&mut snap)?;
    }

    // Restore
    restore_from_dir(dst, out)?;

    // Verify
    let db_r = Db::open(dst)?;
    assert_eq!(db_r.get(b"a")?.as_deref(), Some(b"1".as_ref()));
    assert_eq!(db_r.get(b"b")?.as_deref(), Some(b"2".as_ref()));
    Ok(())
}
```

CLI:

```bash
# Full backup
quiverdb backup --path ./db --out ./backup

# Incremental backup since a checkpoint LSN
# (You can get a follower checkpoint via `quiverdb cdc last-lsn --path <follower>`)
quiverdb backup --path ./db --out ./backup_incr --since-lsn 12345

# Restore (optional verify runs strict check)
quiverdb restore --path ./db_restored --from ./backup --verify
```

---

## 12) Troubleshooting

- “Snapshot sees wrong/none value under heavy churn”:
    - Ensure you’re using v1.2+ code: writer freezes KV and OVERFLOW pages before free; reader uses fallback when chain moved.
    - Check metrics: snapshot_freeze_frames/bytes should be > 0 for hot paths.
- “snapshot overflow length mismatch”:
    - Fixed in reader by applying LSN-gating to OVERFLOW pages and freezing overflow pages before free.
- “Backup misses a page when writer is busy”:
    - backup_to_dir retries index refresh a few times; ensure the backup window includes short waits for heavy churn.
- Restore says “CRC mismatch in pages.bin”:
    - Corrupted archive; re-create backup. Use `--verify` after restore to validate integrity.
- Performance issues under long snapshots:
    - Limit the number/length of active snapshots. Monitor snapshot_freeze_bytes.

---

## 13) Roadmap (Phase 2)

- Persisted snapshot registry:
    - Snapshots survive restarts; managed sidecar store with reference counting and GC.
- Shared freeze store (snapstore) with dedup and compaction:
    - Store frames once and reference snapshots to them.
- Backup dedup format:
    - Manifest (page_id → content hash) + blobs.bin with unique content chunks.
- Streaming backup formats (tar/zstd) and verification tools.
- Optional snapshot begin/end CLI for persisted snapshots.

---

This Phase 1 design delivers snapshot isolation and hot backup without changing frozen on-disk formats. It combines page-level LSN, COW via freeze, and a robust reader that resolves both KV and overflow pages “as of LSN”, with a rare fallback for hot chain mutations.