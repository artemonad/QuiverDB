<!-- README.md -->
# QuiverDB 2.1

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/artemonad/QuiverDB?display_name=tag&sort=semver)](https://github.com/artemonad/QuiverDB/releases/latest)
[![GitHub Stars](https://img.shields.io/github/stars/artemonad/QuiverDB?style=social)](https://github.com/artemonad/QuiverDB/stargazers)

Embedded Rust key–value database with per‑page Robin Hood indexing, WAL, a CRC‑validated directory of bucket heads, and overflow chains for large values.

Status: 2.1. Stable on‑disk/wire formats:
- On‑disk: meta v4, page v3 (KV_RH3, OVERFLOW3), directory v2
- Wire: WAL v2 (P2WAL001, CRC32C; types: 1=BEGIN, 2=PAGE_IMAGE, 4=COMMIT, 5=TRUNCATE, 6=HEADS_UPDATE; 3=DELTA reserved)

Highlights
- Fixed 16‑byte page trailer
  - Default: CRC32C (Castagnoli), digest stored in low 4 bytes; remaining 12 bytes are zero.
  - 2.1: optional AES‑GCM trailer (integrity‑only, tag‑only; page payload is not encrypted).
- WAL v2 with HEADS_UPDATE in batch (directory updates are LSN‑gated)
- Real batch commit (one fsync per batch)
- Read‑side TTL and tombstones
- OVERFLOW3 with per‑page zstd compression (optional)
- Compaction (2.0): single‑scan head→tail + KV‑packing (multiple records per KV page)

What’s new in 2.1
- Security/robustness
  - TDE AEAD tag‑only refactor: single compute path, constant‑time verification; AAD = "P2AEAD01" || page[0..16].
  - Epoch‑aware CRC fallback (TDE on): fallback allowed only for page_lsn < since_lsn of the last KeyJournal epoch; current epoch strictly rejects fallback.
  - WAL reader: mid‑stream WAL header (“P2WAL001”) is skipped only after TRUNCATE markers.
- Performance
  - In‑memory keydir now stores record offsets (pid, off) → get()/exists() can read exact records without per‑page scans; TTL/tombstone semantics preserved with correct fallback.
  - Segment writer buffer is configurable: ENV P1_SEG_WRITE_BUF_MB (default 16) for larger sequential writes during batch commit.
  - Bench profile enables P1_PAGE_CACHE_OVF=1 (cache OVERFLOW pages) and P1_PREALLOC_PAGES=16384 (hot preallocation).
  - Bench supports --big-batch-size for batched big puts and --codec zstd to initialize DB with zstd for OVERFLOW3.
- Bloom side‑car
  - Safe MMAP: full‑file mapping from offset=0 (page‑aligned), indexing accounts for header size. Compatible with previous body‑only mapping.

---

## Quick start (CLI)

Initialize:
```bash
quiverdb init --path ./db2 --page-size 65536 --buckets 128
```

Put/Get/Del:
```bash
quiverdb put --path ./db2 --key alpha --value 1
quiverdb get --path ./db2 --key alpha
quiverdb del --path ./db2 --key alpha
```

Batch (single WAL batch, one fsync):
```bash
cat > ops.json <<'JSON'
[
  {"op":"put","key":"alpha","value":"1"},
  {"op":"put","key":"bin","value":"hex:deadbeef"},
  {"op":"del","key":"alpha"}
]
JSON
quiverdb batch --path ./db2 --ops-file ./ops.json
```

Scan (stream/JSON):
```bash
# human stream
quiverdb scan --path ./db2 --stream
# JSONL stream
quiverdb scan --path ./db2 --stream --json
# prefix
quiverdb scan --path ./db2 --prefix a --stream
```

Maintenance:
```bash
# Compact all buckets (single-scan + packing)
quiverdb compact --path ./db2

# Vacuum: compact all + sweep orphan OVERFLOW pages
quiverdb vacuum --path ./db2

# Auto-maint: compact up to N non-empty buckets (+ optional sweep)
quiverdb auto-maint --path ./db2 --max-buckets 32 --sweep

# Sweep orphan OVERFLOW pages only
quiverdb sweep --path ./db2
```

Status / Doctor / Bloom:
```bash
quiverdb status --path ./db2
quiverdb doctor --path ./db2
quiverdb bloom --path ./db2
```

Checkpoint (truncate WAL to header; writer-only):
```bash
quiverdb checkpoint --path ./db2
```

CDC:
```bash
# Ship WAL v2 frames to a file
quiverdb cdc-ship --path ./db2 --to file://./wal-stream.bin

# Apply from a file stream into follower
quiverdb cdc-apply --path ./follower --from file://./wal-stream.bin
```

---

## Quick start (Rust API)

```rust
use anyhow::Result;
use QuiverDB::Db;

fn main() -> Result<()> {
    let root = std::path::Path::new("./db2");
    if !root.exists() {
        std::fs::create_dir_all(root)?;
        Db::init(root, 65536, 128)?;
    }

    // Writer: batch API (one BEGIN/COMMIT, one fsync)
    {
        let mut db = Db::open(root)?;
        db.batch(|b| {
            b.put(b"alpha", b"1")?;
            b.put(b"big", vec![0xAB; 100_000].as_slice())?; // OVERFLOW3; zstd if enabled
            Ok(())
        })?;
    }

    // Reader (TTL-aware, tombstone-priority)
    {
        let db_ro = Db::open_ro(root)?;
        if let Some(v) = db_ro.get(b"alpha")? {
            assert_eq!(v, b"1");
        }
    }

    Ok(())
}
```

---

## Compaction (single‑scan + packing)

- One pass head→tail; per page “newest→oldest”
- Selects first valid record per key (tombstone wins; TTL read‑side)
- Writes compacted data using KvPagePacker (multiple records per page)
- Overflow values are not expanded; placeholders are preserved as‑is

CLI:
```bash
# One bucket
quiverdb compact --path ./db2 --bucket 42
# All buckets
quiverdb compact --path ./db2
# Vacuum (compact all + sweep orphans)
quiverdb vacuum --path ./db2
```

Tip: After heavy maintenance, refresh Bloom:
```bash
quiverdb bloom --path ./db2
```

---

## TTL semantics (read‑side)

- expires_at_sec (u32, absolute Unix time; 0 = immortal)
- Skip records with now >= expires_at_sec
- Tombstone (vflags bit 0 = 1) has priority
- Metric ttl_skipped counts TTL‑based skips

---

## CDC and WAL v2 (P2WAL001)

- CRC32C on header-before-crc + payload
- Record types: 1=BEGIN, 2=PAGE_IMAGE, 4=COMMIT, 5=TRUNCATE, 6=HEADS_UPDATE; 3=DELTA reserved
- Unknown types ignored; partial tails treated as EOF
- HEADS_UPDATE is LSN‑gated (apply only when wal_lsn > last_heads_lsn)
- Reader tolerates mid‑stream WAL header only after TRUNCATE markers

---

## Format overview (2.x)

- Page v3
  - KV_RH3: [klen u16][vlen u32][expires_at_sec u32][vflags u8][key][value]
    - Slot table (6‑byte slots) supports multiple records per page
  - OVERFLOW3: codec_id (0=none, 1=zstd), chunk_len is bytes stored on page
  - 16‑byte trailer: CRC32C by default or AES‑GCM tag (integrity‑only)
- Meta v4
  - codec_default (0=none, 1=zstd), checksum_kind (0=crc32, 1=crc32c default)
- Directory v2
  - Single shard (dir‑000) with CRC32C and atomic tmp+rename (in‑place mode for dev/bench)
- WAL v2
  - P2WAL001 with CRC32C, BEGIN/IMAGE/COMMIT, TRUNCATE, HEADS_UPDATE (type=6)

---

## Configuration

Programmatic:
- QuiverConfig::from_env() and DbBuilder for explicit overrides

Common ENV toggles:
- Performance
  - P1_WAL_DISABLE_FSYNC=1 — disable WAL fsyncs (bench/dev)
  - P1_WAL_COALESCE_MS=N — group‑commit window
  - P1_DATA_FSYNC=0|1 — fsync data segments on commit (default 0)
  - P1_PAGE_CACHE_PAGES=N — process‑wide page cache (default 4096)
  - P1_PAGE_CACHE_OVF=1 — allow caching OVERFLOW pages
  - P1_PREALLOC_PAGES=N — hot preallocation on the last touched segment
  - P1_SEG_WRITE_BUF_MB=N — segment writer buffer (MiB; default 16)
  - P1_PACK_THRESHOLD_BYTES=N — small value threshold for packing
- Integrity/Security
  - P1_READ_BEYOND_ALLOC_STRICT=1 — forbid reads beyond logical allocation
  - P1_ZERO_CHECKSUM_STRICT=1 — forbid zero CRC trailers in CRC mode
  - P1_TDE_STRICT=1 — forbid CRC fallback when AEAD tag fails (TDE on)
- Bloom
  - P1_BLOOM_MMAP=1 — use mmap for bloom.bin (full‑file mapping from offset 0)
  - P1_BLOOM_CACHE_BUCKETS=N — per‑process Bloom LRU cache
- CDC PSK/TLS
  - P1_CDC_PSK_HEX / P1_CDC_PSK_BASE64 / P1_CDC_PSK — pre‑shared key
  - P1_CDC_SEQ_STRICT=1 — strict monotonic seq on apply
  - P1_TLS_* — TLS/mTLS client options

Note: many toggles are read once per process; prefer programmatic config for long‑running apps.

---

## Metrics

Process‑wide counters and gauges (WAL, page cache, Bloom, TTL, packing, etc.).
Prometheus exporter:
```bash
quiverdb_metrics --addr 0.0.0.0:9898 --path ./db2
```

---

## Troubleshooting

- “AEAD tag verify failed” or “page checksum mismatch”
  - TDE: tag-only mode enforces integrity; with TDE strict mode CRC fallback is disabled.
  - Without TDE, enable ZERO_CHECKSUM_STRICT to treat zero CRC as invalid.
- “page N not allocated (next_page_id=…)”
  - Enable P1_READ_BEYOND_ALLOC_STRICT=0 (dev) or advance allocation via WAL/replay
- CDC apply not reflecting updates
  - Check LSN progression; HEADS_UPDATE requires LSN gating; verify last_heads_lsn on follower
  - Mid‑stream WAL header skipping occurs only after TRUNCATE
- Bloom not used
  - Filter may be stale (different last_lsn or buckets); rebuild with quiverdb bloom

---

## Build / Install

Build:
```bash
cargo build --release
```
Binaries:
- target/release/quiverdb
- target/release/quiverdb_metrics
- target/release/quiverdb_bench

Use as a library:
```toml
[dependencies]
QuiverDB = { path = "./QuiverDB" }
```

---

<!-- docs/api.md -->
# QuiverDB 2.1 — API Guide (Addendum)

This addendum documents the 2.1 changes. Base semantics from 2.0 remain valid.

Scope
- On‑disk/wire formats unchanged: meta v4, page v3 (KV_RH3/OVERFLOW3), directory v2, WAL v2.
- New helper APIs and fast paths for read performance; security refinements for TDE.

## 1) In‑memory keydir with offsets

- Keydir entries now store (pid, off), where off is the byte offset of the newest valid record on the KV page.
- Db::open_ro() rebuilds keydir by scanning head→tail and per‑page newest→oldest using kv_for_each_record_with_off().
- get()/exists() fast‑path:
  - If a key maps to NO_PAGE — immediate not‑found (tombstone).
  - Otherwise, read the page and use kv_read_record_at_checked(page, off, data_end) to read the exact record.
  - TTL/tombstone semantics applied; if expired/mismatch, fall back to a regular chain scan (head→tail).

APIs
- MemKeyLoc { pid: u64, off: u32 }
- Db::mem_keydir_get_loc(bucket, key) -> Option<MemKeyLoc>

KV helpers
- kv_for_each_record_with_off(page, f(off, k, v, exp, flags))
- kv_read_record_at_checked(page, off, data_end)

## 2) TDE (AES‑GCM) — integrity‑only trailer

- AEAD tag‑only: page payload is not encrypted; the 16‑byte trailer is an AES‑GCM tag over the page with zeroed trailer; AAD = "P2AEAD01" || page[0..16].
- Epoch‑aware CRC fallback:
  - With TDE enabled, if AEAD tag verification fails and strict mode is off, CRC fallback is allowed only for pages with page_lsn < since_lsn of the latest KeyJournal epoch.
  - Pages with lsn ≥ since_lsn strictly reject fallback.

## 3) WAL reader mid‑header gating

- Reader tolerates an extra WAL file header (“P2WAL001”) in the middle of the stream only after a TRUNCATE record.
- Partial tails are still treated as EOF.

## 4) Segment writer buffer (batch)

- Env P1_SEG_WRITE_BUF_MB (default 16) configures BufWriter capacity during grouped segment writes in commit_pages_batch(_with_heads).

## 5) Bench improvements

- --big-batch-size N: batch big puts; produces one WAL batch per chunk of N items.
- --codec {none|zstd}: initialize the DB with codec_default for OVERFLOW3.
- Bench profile now sets:
  - P1_PAGE_CACHE_OVF=1 — cache OVERFLOW pages
  - P1_PREALLOC_PAGES=16384 — preallocate last segment in larger chunks

---

<!-- docs/cdc.md -->
# QuiverDB 2.0/2.1 — CDC Guide (WAL v2)

This guide covers WAL v2 (P2WAL001) and producer/consumer behavior. 2.1 refines reader safety around mid‑stream headers; the wire format is unchanged.

Key points
- WAL v2 is an append‑only stream of page images (and in future, page deltas), with CRC32C and monotonically increasing LSN.
- Idempotence via LSN gating:
  - Apply PAGE_IMAGE only when wal_lsn > current_page_lsn in the page header.
  - Apply HEADS_UPDATE only when wal_lsn > last_heads_lsn persisted on the follower.
- Unknown record types are ignored; partial tails are treated as EOF.

2.1 refinement: mid‑stream WAL header
- After a TRUNCATE marker, producers may resend the 16‑byte global header mid‑stream. Consumers must tolerate and continue.
- Consumers now skip such mid‑stream headers only if the previous record was TRUNCATE.

Types
- 1=BEGIN, 2=PAGE_IMAGE, 3=PAGE_DELTA (reserved), 4=COMMIT, 5=TRUNCATE, 6=HEADS_UPDATE

CRC
- CRC32C(header[0..crc] + payload). Consumers must verify CRC32C on complete records.

---

<!-- docs/format.md -->
# QuiverDB 2.x — On‑Disk and Wire Formats

Status
- Stable formats:
  - On‑disk: meta v4, page v3 (KV_RH3 and OVERFLOW3), directory v2 (single shard)
  - Wire: WAL v2 (P2WAL001)

Endianness: all integers are Little Endian (LE)

Page trailer (16 bytes)
- CRC32C (default):
  - Compute digest over the entire page with trailer zeroed.
  - Store digest (LE u32) at trailer[0..4], zero trailer[4..16].
- AES‑GCM tag (TDE on, integrity‑only):
  - Trailer stores a 16‑byte AEAD tag; page payload is not encrypted.
  - AAD = "P2AEAD01" || page[0..16] (magic/version/type/page_id).
  - Nonce is derived from (page_id, lsn).
  - Strict mode (P1_TDE_STRICT=1) disallows CRC fallback.

Epoch‑aware CRC fallback (TDE)
- With TDE enabled, if tag verification fails and strict mode is off:
  - CRC fallback is allowed only for pages with page_lsn < since_lsn of the latest KeyJournal epoch.
  - Pages with lsn ≥ since_lsn reject fallback.

Directory v2 (P2DIR02)
- “dir-000” with CRC32C, atomic tmp+rename updates (in‑place dev mode available)
- Heads array: buckets × u64 (LE), NO_PAGE=u64::MAX

WAL v2 (P2WAL001)
- Global header (16 bytes), record header (28 bytes), CRC32C
- BEGIN/IMAGE/COMMIT/HEADS_UPDATE/TRUNCATE types
- Consumers ignore unknown types and treat partial tails as EOF
- 2.1 reader refinement: only skip mid‑stream headers immediately following TRUNCATE

KV_RH3 (v3)
- Record encoding: [klen u16][vlen u32][expires_at_sec u32][vflags u8][key][value]
- Slot table (6‑byte slots) supports multiple records per page
- Readers scan per page newest→oldest; tombstones/TTL handled read‑side

OVERFLOW3 (v3)
- codec_id (0=none, 1=zstd), chunk_len bytes on page (compressed when codec!=0)
- Chains linked by next_page_id up to NO_PAGE
- Readers decompress page‑by‑page, verify final length

Meta v4
- page_size, hash_kind, last_lsn, clean_shutdown, codec_default (0=none, 1=zstd), checksum_kind (CRC32C default)

---