# Changelog

All notable changes to this project are documented in this file.

On-disk formats remain frozen across 1.x:
- Meta v3, Page v2, WAL v1 (no migrations; forward/backward compatible)

Dates use ISO format (YYYY-MM-DD).


## [1.2.0] – 2025-10-06

Phase 1: Snapshot Isolation and Backup/Restore (no dedup yet). Formats unchanged.

Added
- Snapshot Isolation (in-process, as-of LSN)
  - Db::snapshot_begin() -> SnapshotHandle { lsn }
  - SnapshotHandle::get/scan_all/scan_prefix — consistent view at snapshot_lsn
  - SnapshotHandle::end() — drops sidecar
- Page-level COW (freeze store)
  - Before overwriting/freeing a v2 page whose LSN may be needed by a live snapshot, the current image is copied into sidecar
  - Sidecar: <root>/.snapshots/<id> with:
    - freeze.bin: frames [page_id u64][page_lsn u64][page_len u32][crc32 u32] + payload(page_size)
    - index.bin: [page_id u64][offset u64][page_lsn u64]
- Backup/Restore (on top of snapshots)
  - Full backup (all pages as-of snapshot_lsn)
  - Incremental backup (pages with page_lsn in (since_lsn, snapshot_lsn])
  - Restore writes pages back, installs dir.bin (if present), sets last_lsn and clean_shutdown=true
  - CLI:
    - quiverdb backup --path ./db --out ./backup [--since-lsn N]
    - quiverdb restore --path ./dst --from ./backup [--verify]
  - Rust API: backup::backup_to_dir(&db, &snap, out_dir, since_lsn), backup::restore_from_dir(dst_root, backup_dir)
- Snapshot/backup/restore metrics
  - snapshots_active
  - snapshot_freeze_frames, snapshot_freeze_bytes
  - backup_pages_emitted, backup_bytes_emitted
  - restore_pages_written, restore_bytes_written

Changed/Improved
- Snapshot readers handle hot chain mutations:
  - Per-page “live vs frozen” selection by LSN; overflow chains resolved per page
  - Fallback scan reconstructs tail-wins view when chain heads move after the snapshot (rare)
- CDC apply path optimization:
  - wal-apply and cdc replay perform LSN-gating before ensure_allocated (avoids unnecessary allocations)
  - Note: the same optimization for startup WAL replay is planned for 1.2.1

Docs
- New docs/snapshots.md (semantics, COW details, sidecar formats, backup/restore)
- API/CDC guides updated; README updated to v1.2 Phase 1 (no dedup yet)

Compatibility
- On-disk formats unchanged: meta v3, page v2, WAL v1
- Backups created by 1.2 can be restored by 1.2+ (CRC-checked)

Upgrade notes
- Phase 1 snapshots are in-process only (not persisted across restarts)
- Backup deduplication and persisted snapshot registry are planned in a later phase
- For CDC streams, ensure wal-apply uses matching decompression when wal-ship compresses (gzip/zstd)


## [1.1.5] – 2025-10-05

Added
- CDC ship sink=file://path — write a full WAL stream (header + frames) to a file
- CDC ship batching (ENV):
  - P1_SHIP_FLUSH_EVERY=N — flush every N frames (default 1)
  - P1_SHIP_FLUSH_BYTES=B — flush when ≥ B bytes have been written since the last flush (default 0 = disabled)
- Inclusive since-lsn (ENV):
  - P1_SHIP_SINCE_INCLUSIVE=1|true|yes|on — treat --since-lsn as lsn >= N (default is > N)
- JSON modes in CLI:
  - status: P1_STATUS_JSON=1
  - dbstats: P1_DBSTATS_JSON=1

Changed/Improved
- Apply path: LSN-gating before ensure_allocated in wal-apply and cdc replay
- CDC docs updated (file sink, batching env vars, inclusive since-lsn)

Refactor
- CDC split into modules: ship/tail/apply/record/replay/last_lsn
- Db split: maintenance (sweep/print_stats) and KV put-in-chain moved to separate modules
- Pager: page cache moved into pager/cache.rs

Tests
- New integration test cdc_file_sink: ship to file + apply from file

Compatibility
- On-disk formats unchanged (meta v3, page v2, WAL v1)


## [1.1.0] – 2025-10-05

Formats unchanged (meta v3, page v2, WAL v1)

Added
- Configuration and builder
  - QuiverConfig and DbBuilder; explicit open_with_config/open_ro_with_config
  - Tunables: wal_coalesce_ms, data_fsync, page_cache_pages, ovf_threshold_bytes
- In‑process subscriptions
  - Db::subscribe_prefix(prefix, callback) → SubscriptionHandle (Drop = unsubscribe)
  - Event { key, value: Option<Vec<u8>>, lsn } (value=None for delete)
- Scans
  - Db::scan_all() and Db::scan_prefix(prefix) with tail‑wins semantics
  - CLI: dbscan [--prefix X] [--json]
- CDC tooling
  - wal-ship: --since-lsn N (send frames with lsn > N), sink=tcp://host:port
  - cdc last-lsn: print follower’s last LSN (resume helper)
  - cdc record: deterministic wire-format slices with optional --from-lsn/--to-lsn
  - cdc replay: idempotent apply from file/stdin with optional LSN filters
- Metrics CLI
  - metrics --json and metrics-reset

Changed
- WAL group-commit: shared registry to coalesce fsync across appenders
- Db stats include a metrics snapshot

Hardening
- CDC/wal-apply tolerant to partial tails, ignores unknown types (forward-compatible)
- LSN-gating prevents applying older frames over newer v2 pages
- Directory updates remain atomic (tmp+rename) with CRC validated on open
- Repair conservatively frees orphan overflow chains

Docs
- Updated API/CDC guides; README quick start examples

Compatibility
- No on-disk format changes; backward/forward compatibility preserved


## [1.0.0] – 2025-10-05

Initial 1.x with frozen formats (meta v3, page v2, WAL v1).

- Reliability
  - WAL replay: CRC verification, LSN-gating for v2 pages, tolerant to partial tails, ignores unknown record types
  - Directory: atomic updates via tmp+rename, CRC in header
- Operations
  - Read-only readers (Db::open_ro): shared lock, no replay, no meta changes
  - check/repair: CRC/IO scan, overflow orphan reporting, --strict mode; conservative orphan freeing
  - metrics + CLI commands (metrics, metrics-reset)
  - WAL group-commit (fsync coalescing), P1_DATA_FSYNC option, page cache (optional)
- CDC
  - wal-tail (JSONL), wal-ship (binary), wal-apply (idempotent apply with LSN gating)


—  
For details and examples, see docs/api.md, docs/cdc.md, docs/format.md, and docs/snapshots.md.