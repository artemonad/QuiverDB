# QuiverDB 2.0 — API Guide

Audience: Rust developers embedding QuiverDB or building tooling around it.  
Status: 2.0 (GA). Stable on‑disk/wire formats:
- On‑disk: meta v4, page v3 (KV_RH3, OVERFLOW3), directory v2
- Wire: WAL v2 (P2WAL001, CRC32C; types: 1=BEGIN, 2=PAGE_IMAGE, 4=COMMIT, 5=TRUNCATE, 6=HEADS_UPDATE; 3=DELTA reserved)

Contents
- 1) Overview
- 2) Opening, locks and process model
- 3) Configuration (QuiverConfig / DbBuilder)
- 4) Write path (put/del/batch)
- 5) Read path (get/exists/scan)
- 6) Compaction and maintenance
- 7) Overflow values and compression
- 8) WAL v2 and CDC hooks
- 9) Bloom side‑car
- 10) Metrics
- 11) Errors and troubleshooting
- 12) Examples

---

## 1) Overview

QuiverDB is an embedded key–value store with:
- Page v3 (KV_RH3 and OVERFLOW3) and per‑page trailer (16 bytes; CRC32C by default)
- Directory v2 (single shard) with CRC32C and atomic updates
- WAL v2 (P2WAL001) with CRC32C and BEGIN/IMAGE/COMMIT markers
- Read‑side TTL; tombstones delete keys
- NEW 2.0 GA: single‑scan compaction (head→tail) with KV‑packing (multiple KV per page)

Writer emphasizes “one real batch”:
- Db::batch() builds all KV/OVERFLOW pages in memory and commits them once:
  - WAL: one BEGIN, many PAGE_IMAGE, one HEADS_UPDATE, one COMMIT, one fsync
  - Then pages are written to segment files; directory heads updated

Readers:
- Db::get() walks a bucket chain head→tail and returns the first valid record:
  - Valid = not tombstone, not expired (expires_at_sec == 0 or now < expires_at_sec)

---

## 2) Opening, locks and process model

- Writer (exclusive lock):
  - Db::open(root)
  - Replays WAL v2 on startup if meta.clean_shutdown == false
  - Sets clean_shutdown=true on clean drop/commit cycles

- Reader (shared lock):
  - Db::open_ro(root)
  - Never replays WAL; never mutates meta

Lock file path: <root>/LOCK (advisory locks via fs2). Windows parent directory fsync on rename is best‑effort.

Process model:
- One writer process at a time (exclusive lock). Many readers (shared lock).
- Page cache and Bloom cache are process‑wide.

---

## 3) Configuration (QuiverConfig / DbBuilder)

Programmatic configuration:
- QuiverConfig::from_env() — backward‑compatible env reader
- DbBuilder — fluent overrides on top of defaults

Key fields:
- wal_coalesce_ms: fsync coalescing (group‑commit)
- data_fsync: fsync data segments on commit (default false; durability via WAL)
- page_cache_pages: process‑wide page cache size (default 4096; 0 disables)
- ovf_threshold_bytes: explicit overflow threshold (None → ps/4)
- TDE: tde_enabled (bool) + tde_kid (Option<String>)

Notable ENV toggles (read once per process):
- P1_WAL_DISABLE_FSYNC=1 — disable WAL fsync (bench/dev only)
- P1_WAL_COALESCE_MS=N
- P1_DATA_FSYNC=0|1
- P1_PAGE_CACHE_PAGES=N, P1_PAGE_CACHE_OVF=1
- P1_PACK_THRESHOLD_BYTES=N
- P1_READ_BEYOND_ALLOC_STRICT=1 — forbid reads beyond logical allocation
- P1_ZERO_CHECKSUM_STRICT=1
- P1_TDE_STRICT=1

---

## 4) Write path (put/del/batch)

Single operation (legacy path):
- Db::put(key, value)
- Db::del(key)

Under the hood (single page):
1) Assign LSN = meta.last_lsn + 1 and write to page header
2) Update page trailer (CRC32C by default; AEAD tag when TDE enabled)
3) WAL v2: BEGIN(lsn) → PAGE_IMAGE(lsn) → COMMIT(lsn); fsync WAL
4) Write page to segment; maybe truncate WAL; meta.last_lsn = lsn (in memory)

Batch (recommended):
- Db::batch(|b| { b.put(..); b.del(..); ... })
  - Builds KV pages (and OVERFLOW3 chains) in memory; small KV are packed into pages
  - Pager::commit_pages_batch_with_heads:
    - BEGIN(start) → IMAGE(lsn_i, pid_i, payload_i) × N → HEADS_UPDATE(last_lsn, updates) → COMMIT(last_lsn) — one fsync
    - Writes all pages to segments; updates meta.last_lsn once
  - Directory heads are installed after successful batch

Notes:
- 2.0 GA packing: multiple KV per page using KV_RH3 slot table
- Overflow values are stored as placeholders pointing to OVERFLOW3 chains

---

## 5) Read path (get/exists/scan)

- Db::get(key: &[u8]) -> Result<Option<Vec<u8>>>
  - Traverse from head to tail; accept the first valid record:
    - Tombstone wins (immediate None)
    - TTL read‑side: expires_at_sec == 0 (immortal) or now < expires_at_sec
    - Value may be inline or an OVERFLOW3 placeholder (expanded transparently)

- Db::exists(key: &[u8]) -> Result<bool>
  - Fast‑path: in‑memory keydir (if present when opened RO)
  - Fast‑path: Bloom (fresh filters only), negative → quick false
  - Otherwise chain traversal head→tail (same tombstone/TTL semantics)

- Db::scan_stream(prefix: Option<&[u8]>, cb: impl FnMut(&[u8], &[u8]))
  - Streaming traversal producing final pairs (tail‑wins, TTL/tombstones)
  - When prefix is provided, only matching keys are yielded

---

## 6) Compaction and maintenance

Single‑scan compaction (2.0):
- One pass head→tail; per page “newest→oldest”
- First valid record (not tombstone, not expired) per key is selected
- Compacted output is written using KvPagePacker (multiple KV per page) → shorter chains
- Overflow placeholders are preserved as‑is (no large value rewrite)
- After commit, Bloom side‑car can be delta‑updated for the bucket (best‑effort)

APIs:
- Db::compact_bucket(bucket: u32) -> CompactBucketReport
- Db::compact_all() -> CompactSummary
- Db::vacuum_all(): compact_all + sweep orphan OVERFLOW pages
- Db::auto_maintenance(max_buckets, do_sweep) -> AutoMaintSummary
- Db::sweep_orphan_overflow() -> pages_freed

---

## 7) Overflow values and compression

Big values are stored in OVERFLOW3 chains:
- KV value holds a placeholder (tag=0x01 OVF_CHAIN) with [total_len u64][head_pid u64]
- Readers fetch the chain page‑by‑page using next_page_id
- Compression:
  - Each OVERFLOW3 page declares codec_id (0=none, 1=zstd)
  - chunk_len is the number of bytes stored on that page (compressed when codec!=0)
  - Readers decompress page‑by‑page (streaming) and verify final length

---

## 8) WAL v2 and CDC hooks

WAL v2 header: P2WAL001 + reserved (16 bytes total).

Record header (28 bytes, LE):
- type u8: 1=BEGIN, 2=PAGE_IMAGE, 3=PAGE_DELTA (reserved), 4=COMMIT, 5=TRUNCATE, 6=HEADS_UPDATE
- flags u8, reserved u16
- lsn u64, page_id u64 (0 for non‑page records), len u32
- crc32c u32 over header[0..crc] + payload

Crash recovery (writer‑open):
- If meta.clean_shutdown=false: replay WAL v2
  - Verify CRC32C; stop on partial tails; ignore unknown/DELTA
  - LSN‑gating: PAGE_IMAGE applies only if wal_lsn > page_lsn
  - Truncate WAL to header and set meta.clean_shutdown = true

CDC apply:
- Same idempotence rules as crash recovery
- HEADS_UPDATE is LSN‑gated via a persisted last_heads_lsn marker

---

## 9) Bloom side‑car

- Optional side‑car (P2BLM01) providing fast negative hints for exists/get(not‑found)
- is_fresh_for_db compares last_lsn in Bloom header with DB’s last_lsn
- Memory modes: RAM or MMAP (ENV P1_BLOOM_MMAP=1)
- Per‑process LRU cache of bucket bitmaps (P1_BLOOM_CACHE_BUCKETS)
- Batch/mutation paths can delta‑update Bloom (best‑effort) and set last_lsn

---

## 10) Metrics

QuiverDB::metrics exposes process‑wide counters/gauges:
- WAL: wal_appends_total, wal_bytes_written, wal_fsync_calls, wal_truncations, wal_pending_max_lsn, wal_flushed_lsn, threshold flush counters
- Page cache: page_cache_hits, page_cache_misses, page_cache_len, evictions_total
- Read‑side: ttl_skipped; keydir_hits/misses
- Packing: pack_pages, pack_records, pack_pages_single
- Bloom: bloom_tests, bloom_negative, bloom_positive, bloom_skipped_stale, bloom_updates_total, bloom_update_bytes

Prometheus exporter:
- Binary quiverdb_metrics exposes /metrics endpoint

---

## 11) Errors and troubleshooting

- anyhow::Result is used across public APIs
- Common failures:
  - LOCK acquisition failed — another writer holds exclusive lock
  - Page checksum mismatch / AEAD tag verify failed (TDE)
  - Bad WAL magic or CRC — WAL will be truncated to header on recovery
- Strict guards (optional):
  - P1_READ_BEYOND_ALLOC_STRICT=1 — read_page errors if page_id >= next_page_id
  - P1_ZERO_CHECKSUM_STRICT=1 — zero CRC trailer is considered invalid
  - P1_TDE_STRICT=1 — AEAD failures do not fallback to CRC

---

## 12) Examples

Initialize, write with a batch, read:
```rust
use anyhow::Result;
use QuiverDB::Db;

fn main() -> Result<()> {
    let root = std::path::Path::new("./db2");
    if !root.exists() { Db::init(root, 65536, 128)?; }

    // batch write
    {
        let mut db = Db::open(root)?;
        db.batch(|b| {
            b.put(b"alpha", b"1")?;
            b.put(b"big", vec![0xAB; 100_000].as_slice())?;
            Ok(())
        })?;
    }

    // TTL-aware get
    {
        let db = Db::open_ro(root)?;
        let v = db.get(b"alpha")?;
        println!("alpha: {:?}", v);
    }

    Ok(())
}
```

Streaming scan from CLI:
```bash
quiverdb scan --path ./db2 --stream --json
```

Vacuum:
```bash
quiverdb vacuum --path ./db2
```

CDC ship/apply (file):
```bash
quiverdb cdc-ship --path ./db2 --to file://./wal.bin
quiverdb cdc-apply --path ./follower --from file://./wal.bin
```