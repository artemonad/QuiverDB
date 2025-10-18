# Changelog

All notable changes to this project are documented in this file.  
Dates use ISO format (YYYY-MM-DD).

## [2.2.0] – 2025-10-18

Added
- SnapStore dir override (P1_SNAPSTORE_DIR)
  - SnapStore may live outside DB root. Absolute path is used as-is; relative is resolved against DB root.
  - Manifests now stored in <snapstore_dir>/manifests (manifest.rs uses the same resolver).
- Snapshot delete
  - API: SnapshotManager::delete_persisted(root, id)
  - CLI: quiverdb snapshot-delete --path <db_root> --id <snapshot_id>
  - Behavior: dec-ref all referenced objects (objects removed when refcount reaches 0), then remove manifest.

Changed/Improved
- CDC (apply)
  - PSK streams now require HELLO (WAL header + stream_id) by default; fallback without HELLO only with P1_CDC_ALLOW_NO_HELLO=1 (warn).
  - Strict monotonic seq: P1_CDC_SEQ_STRICT=1 → seq regression is an error (non-strict: warn+skip).
  - HEADS_UPDATE payload validation: invalid len (len%12!=0) → error with P1_CDC_HEADS_STRICT=1, otherwise warn+skip.
  - PSK stream source anti-mix unified with file path via verify_and_store_stream_id.
- Status JSON
  - JSON branch rewritten with serde_json::json! (no manual string concatenation/escaping).
- WAL PSK/TLS diagnostics
  - More context in error messages for framing/mac/CA/PFX/SNI parsing.
- Build/tooling
  - Added recursion limit attribute in bin/quiverdb to support deep json! trees in status output.

Removed
- Unused dependency crc32fast (CRC32C already handled by crc32c crate).

Fixed/Cleanups
- Tests: cleaned unused imports/vars in CDC stream-id tests.
- Dead code: removed unused Db::read_overflow_chain helper.
- Minor warnings suppressed/cleaned across codebase.

Compatibility
- On-disk and wire formats unchanged:
  - Meta v4, Page v3 (KV_RH3/OVERFLOW3), Directory v2, WAL v2.

Upgrade notes
- No migrations required.
- New env toggles:
  - P1_SNAPSTORE_DIR — SnapStore location (absolute or relative to DB root).
  - P1_CDC_SEQ_STRICT — strict seq monotonicity.
  - P1_CDC_HEADS_STRICT — strict HEADS_UPDATE payload validation.
  - P1_CDC_ALLOW_NO_HELLO — allow PSK apply without initial HELLO (not recommended; dev only).

---

## [2.1.0] – 2025-10-16

Security
- TDE (AES‑GCM) integrity-only trailer
  - Introduced compute_aead_tag_for_page() and refactored AEAD tag update/verify to a single code path with constant-time compare.
  - Clarified semantics: tag-only (integrity), page payload is not encrypted.
- Epoch‑aware CRC fallback (TDE on)
  - CRC fallback is allowed only for pages with page_lsn < since_lsn of the latest epoch in KeyJournal; pages with lsn ≥ since_lsn strictly reject fallback.

Added
- In‑memory keydir with record offsets (pid, off)
  - New fast-path for get()/exists(): direct read of the exact record by offset (no intra-page scan), with TTL/tombstone semantics and correct fallback.
  - New API: MemKeyLoc { pid, off }, Db::mem_keydir_get_loc(), and prefix/for_each variants with offsets.
- KV helpers
  - kv_read_record_at_checked(page, off, data_end): safe record reader for known offsets.
  - kv_for_each_record_with_off(page, f(off, k, v, exp, flags)): packed-aware iteration with offsets.
- WAL mid-header gating
  - WAL reader now skips a mid-stream “P2WAL001” header only if the previous record was TRUNCATE.
- Overflow chain guard
  - OVF_MAX_CHAIN_PAGES_GUARD (default 1_000_000) as a shared safety limit for long chains; used in reader and sweep_orphan_overflow.
- Segment writer buffer tuning
  - ENV P1_SEG_WRITE_BUF_MB (default 16) to control BufWriter capacity per segment during batch writes.
- Bench CLI/features
  - --big-batch-size N: batch big puts (one WAL commit per batch).
  - --codec {none|zstd}: initialize DB with codec_default for OVERFLOW3.
  - Bench profile now sets P1_PAGE_CACHE_OVF=1 and P1_PREALLOC_PAGES=16384.

Changed/Improved
- Bloom MMAP safety
  - MMAP now maps the entire file from offset=0 (page-aligned); indexing accounts for header size. test() is compatible with both “body-only” and “full-file” mmaps.
- get()/exists() performance
  - Use keydir (pid, off) to directly read the record; on TTL expiry or mismatch, fall back to the usual head→tail scan.
- Page module exports
  - Removed re-exports of deprecated/unsafe helpers from page/ and page::kv (kv_read_record no longer re-exported); modern safe helpers are exported instead.
- TLS CA parsing
  - Kept a robust manual PEM parser for CERTIFICATE/TRUSTED CERTIFICATE blocks (no reliance on private fields of external crates).
- Maintenance
  - sweep_orphan_overflow uses OVF_MAX_CHAIN_PAGES_GUARD for chain traversal.

Fixed
- Bloom delta-update metrics double count
  - record_bloom_update(..) no longer emitted twice (only inside BloomSidecar::update_bucket_bits; delete-only batches still record 0-byte freshness updates).
- Potential mmap offset crash on some platforms
  - Switching to full-file mapping removes dependency on non page-aligned offsets.
- WAL reader robustness
  - Avoids accidentally “eating” bytes that resemble MAGIC mid-stream unless preceded by TRUNCATE.

Environment
- New: P1_SEG_WRITE_BUF_MB (segment writer buffer, MiB; default 16)
- Bench profile additionally sets:
  - P1_PAGE_CACHE_OVF=1 (allow caching OVERFLOW pages)
  - P1_PREALLOC_PAGES=16384 (hot preallocation on the last touched segment)

CLI
- quiverdb_bench:
  - --big-batch-size N: batch big value writes
  - --codec {none|zstd}: initialize DB with desired OVERFLOW codec
- No format migration tools needed for 2.1.

Compatibility
- On‑disk and wire formats unchanged:
  - Meta v4, Page v3 (KV_RH3/OVERFLOW3), Directory v2, WAL v2.
- API changes are additive; removed only deprecated re-exports (use safe KV helpers instead).

Performance notes
- big_put: batching (+zstd, preallocation, larger segment buffer) yields multi‑x improvements.
- get/exists: faster p50/p90 due to direct record access via keydir offsets; falls back correctly under TTL/tombstones.

Upgrade notes
- No data migrations required.
- If using TDE, verify epoch journal is present for epoch‑aware fallback.
- Consider setting P1_SEG_WRITE_BUF_MB and P1_PREALLOC_PAGES for heavy batch workloads; enable P1_PAGE_CACHE_OVF to speed up big_get.

---

## [2.0.0] – 2025-10-13

2.0 (GA). New on‑disk/wire formats, streamlined write/read path, and production‑ready compaction.

Breaking (by design)
- New on‑disk formats (not compatible with 1.x):
  - Meta v4, Page v3 (KV_RH3 and OVERFLOW3), Directory v2 (single shard in 2.0).
- New wire format:
  - WAL v2 (P2WAL001) with CRC32C and BEGIN/IMAGE/COMMIT markers (PAGE_DELTA reserved).

Added
- Fixed 16‑byte page trailer
  - Default: CRC32C (Castagnoli), digest stored in low 4 bytes; remaining 12 bytes are zero.
- Real batch commit
  - Pager::commit_pages_batch: one BEGIN, many PAGE_IMAGE, one COMMIT, single fsync; then data segments write.
  - Db::batch builds KV/OVERFLOW pages in memory and commits once; directory heads updated afterwards.
- Read‑side TTL
  - KV field expires_at_sec (u32, absolute Unix time); 0 = immortal.
  - Readers skip expired records; tombstone (vflags bit 0 = 1) has priority.
  - Metric ttl_skipped counts TTL‑based skips.
- OVERFLOW3 page‑level compression
  - codec_id: 0=none, 1=zstd (data‑area only; header/trailer unchanged).
  - chunk_len is bytes stored on page (compressed length when codec!=0).
- WAL HEADS_UPDATE frame (type=6)
  - Atomic directory updates bundled into WAL batch between IMAGE… and COMMIT; consumers gate by LSN.
- Strict read guard (optional)
  - New ENV P1_READ_BEYOND_ALLOC_STRICT=1|true|yes|on to forbid reads beyond logical allocation (page_id >= next_page_id).

Changed/Improved
- Compaction (major): single‑scan head→tail with KV‑packing
  - Processes each page “newest→oldest” (packed‑aware), selects first valid per key (tombstone/TTL semantics).
  - Writes compacted result using KvPagePacker (multiple records per KV page) → drastically shorter chains.
  - Fewer pages written and fewer future page reads on queries.
- mem_keydir (fast‑path index) rebuild
  - Rebuilt in one pass (head→tail), storing only tombstones (NO_PAGE) and skipping present keys (miss).
  - Aligns with compaction/get semantics.
- WAL apply and startup replay
  - LSN‑gating before ensure_allocated; partial tails tolerated; forward‑compatible unknown types ignored.

Removed
- get() tracing (P1_GET_TRACE, P1_GET_TRACE_KEY_HEX_PREFIX, P1_GET_TRACE_STDERR)

Bugfixes
- KV single‑record pages: kv_find_record_by_key now returns the record only on exact key match
  - Previously, it could return a wrong value when the page had no slot table.
- Linear fallbacks limited to real data area
  - All linear scans now use [KV_HDR_MIN .. min(data_end, data_start)), avoiding scanning the free/zeroed area before the slot table.
  - Fixes phantom empty keys and false candidates in compaction/mem_keydir.
- Optional strict read “beyond tail”
  - When P1_READ_BEYOND_ALLOC_STRICT is enabled, read_page() errors if page_id >= next_page_id (instead of lenient read).

Compatibility
- 2.0 on‑disk/wire formats are not compatible with 1.x. Use the offline converter.
- Default page checksum is CRC32C with a fixed 16‑byte trailer.
- WAL v2 adds HEADS_UPDATE (type=6); consumers must LSN‑gate directory changes.
- KV_RH3 data‑area compression is not enabled in 2.0 GA; OVERFLOW3 compression is implemented.

Upgrade notes
- Stop writers on the source 1.x database and run offline conversion:
  - quiverdb convert --from ./db_v1 --to ./db2 [--page-size 65536] [--codec zstd]
- After upgrading, consider rebuilding Bloom for best exists()/get(not‑found) performance:
  - quiverdb bloom --path ./db2
- For stricter IO semantics in dev/prod, you can enable:
  - P1_READ_BEYOND_ALLOC_STRICT=1  (forbid reads past logical allocation)
  - P1_ZERO_CHECKSUM_STRICT=1      (forbid zero CRC trailers in CRC mode)
  - P1_TDE_STRICT=1                (forbid CRC fallback when AEAD tag fails)

---

## [1.2.5] – 2025-10-07

Phase 2 finalized (persisted snapshots + SnapStore), polish, and release readiness.

Added
- SnapStore directory override
  - P1_SNAPSTORE_DIR supports absolute paths or relative-to-DB-root paths.
  - SnapStore exposes dir_path() for diagnostics/tests.
- CLI: checkpoint
  - quiverdb checkpoint — manually truncates WAL to header (requires data_fsync=true).
  - Under the hood uses Wal::truncate_to_header().
- Tests
  - snapstore_custom_dir: validates P1_SNAPSTORE_DIR (relative and absolute).
  - checkpoint: validates WAL manual truncate-to-header.

Changed/Improved
- SnapshotManager::new_with_flags(root, snap_persist, snap_dedup)
  - Phase 2 flags (persist/dedup) now can be provided via config (preferred).
  - SnapshotManager::new(root) kept for backward compatibility (reads ENV).
- Db::open_with_config now uses SnapshotManager::new_with_flags(...) to honor QuiverConfig flags.
- Docs
  - README updated (Phase 2 overview).
  - docs/snapshots.md rewritten in English (Phase 1 + Phase 2).
  - API guide updated with Wal::truncate_to_header().

Fixed
- SnapStore refcount when the same content is frozen for multiple snapshots in one operation
  - First ss.put(page_bytes) increments to 1; additional snapshots call ss.add_ref(hash).
  - Prevents accidental frame loss on compact after removing only one of multiple referencing snapshots.

Compatibility
- On-disk formats unchanged (meta v3, page v2, WAL v1).
- CLI/API additions are backward-compatible; existing tooling continues to work.

Upgrade notes
- If you plan to keep persisted snapshots and use SnapStore, consider setting P1_SNAP_DEDUP=1 and (optionally) P1_SNAPSTORE_DIR.
- checkpoint should be used only when data_fsync=true to ensure safety.

---

## [1.2.2] – 2025-10-06

Hardening and polish for Phase 1 snapshots and startup replay. Formats unchanged.

Added
- Metrics
  - snapshot_fallback_scans — counts rare “fallback scans” under snapshots (when a hot chain mutation forces reconstructing tail-wins).
  - CLI: quiverdb metrics now prints snapshot_fallback_scans (text and JSON).
- Structured logs
  - log + env_logger (CLI initialization).
  - Replay/backup/restore now emit informative logs (debug/info/warn).

Changed/Improved
- Startup WAL replay optimization:
  - Perform LSN‑gating before ensure_allocated (symmetry with wal-apply/cdc replay). Avoids unnecessary allocations for old frames.
- Backup/Restore:
  - Switched from println!/eprintln! to structured logs; kept the same behavior and CRC checks.

Fixed
- Snapshot correctness under churn:
  - Freeze KV pages before free in all chain‑cleanup paths (db_kv::put_in_chain), ensuring snapshots can still read old page images even when empty pages are cut out of chains.
- Minor warnings cleanup and consistent imports (no functional changes).

Docs
- Mentioned snapshot_fallback_scans and structured logs in the 1.2 Phase 1 context.

Compatibility
- On-disk formats unchanged (meta v3, page v2, WAL v1).
- CLI/API unchanged, except additional metrics fields in “metrics” output.

Upgrade notes
- If you tail logs, enable with e.g. RUST_LOG=info quiverdb ... (debug for detailed replay/backups).
- No data migrations required.

---

## [1.2.0] – 2025-10-06

Phase 1: Snapshot Isolation and Backup/Restore (no dedup yet). Formats unchanged.

Added
- Snapshot Isolation (in-process, as-of LSN)
  - Db::snapshot_begin() -> SnapshotHandle { lsn }
  - SnapshotHandle::get/scan_all/scan_prefix — consistent view at snapshot_lsn
  - SnapshotHandle::end() — drops sidecar
- Page-level COW (freeze store)
  - Before overwriting/freeing a v2 page whose LSN may be needed by a live snapshot, the current image is frozen into a sidecar store
  - Sidecar: <root>/.snapshots/<id> with:
    - freeze.bin: frames [page_id u64][page_lsn u64][page_len u32][crc32 u32] + payload(page_size)
    - index.bin: [page_id u64][offset u64][page_lsn u64]
- Backup/Restore (on top of snapshots)
  - Full and incremental backup (pages with page_lsn in (since_lsn, snapshot_lsn])
  - Archive layout: pages.bin (frames), dir.bin (optional), manifest.json (summary)
  - Restore writes pages back, installs dir.bin (if present), sets last_lsn and clean_shutdown
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
  - Note: same optimization for startup WAL replay landed in 1.2.2

Docs
- New docs/snapshots.md (semantics, COW details, sidecar formats, backup/restore)
- API/CDC guides updated; README updated to v1.2 Phase 1 (no dedup yet)

Compatibility
- On-disk formats unchanged: meta v3, page v2, WAL v1
- Backups created by 1.2 can be restored by 1.2+ (CRC-checked)

Upgrade notes
- Phase 1 snapshots are in-process only (not persisted across restarts)
- Backup deduplication and persisted snapshot registry are planned in a later phase

---

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