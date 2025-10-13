# QuiverDB 2.0

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/artemonad/QuiverDB?display_name=tag&sort=semver)](https://github.com/artemonad/QuiverDB/releases/latest)
[![GitHub Stars](https://img.shields.io/github/stars/artemonad/QuiverDB?style=social)](https://github.com/artemonad/QuiverDB/stargazers)

Embedded Rust key–value database with per‑page Robin Hood indexing, WAL, a CRC‑validated directory of bucket heads, and overflow chains for large values.

Status: 2.0 (GA). Stable on‑disk/wire formats:
- On‑disk: meta v4, page v3 (KV_RH3, OVERFLOW3), directory v2
- Wire: WAL v2 (P2WAL001, CRC32C; types: 1=BEGIN, 2=PAGE_IMAGE, 4=COMMIT, 5=TRUNCATE, 6=HEADS_UPDATE; 3=DELTA reserved)

Highlights
- Fixed 16‑byte page trailer checksum (CRC32C by default)
- WAL v2 with HEADS_UPDATE in batch (directory updates are LSN‑gated)
- Real batch commit (one fsync per batch)
- Read‑side TTL and tombstones
- OVERFLOW3 with per‑page zstd compression (optional)
- Compaction: single‑scan head→tail + KV‑packing (multiple records per KV page)

What’s new in 2.0 GA
- Compaction is now single‑scan and packs many KV per page → much shorter chains
- Linear fallbacks scan only real data (up to data_start) — no phantom records
- KV single‑record lookup fixed (exact key match only)
- Optional strict guard: P1_READ_BEYOND_ALLOC_STRICT forbids reads beyond logical allocation

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

CDC (preview-ready):
```bash
# Ship WAL v2 frames to a file
quiverdb cdc-ship --path ./db2 --to file://./wal-stream.bin

# Apply from a file stream into follower
quiverdb cdc-apply --path ./follower --from file://./wal-stream.bin
```

Convert 1.x → 2.0 (offline):
```bash
quiverdb convert --from ./db_v1 --to ./db2 --page-size 65536 --codec zstd
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
- Startup replay/apply performs LSN‑gating before ensure_allocated

---

## Format overview (2.0)

- Page v3
  - KV_RH3: [klen u16][vlen u32][expires_at_sec u32][vflags u8][key][value]
    - Slot table (6‑byte slots) supports multiple records per page
  - OVERFLOW3: codec_id (0=none, 1=zstd), chunk_len is bytes stored on page
  - 16‑byte trailer checksum (CRC32C; low 4 bytes carry digest)
- Meta v4
  - codec_default (0=none, 1=zstd), checksum_kind (0=crc32, 1=crc32c default)
- Directory v2
  - Single shard (dir‑000) with CRC32C and atomic tmp+rename
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
  - P1_PACK_THRESHOLD_BYTES=N — small value threshold for packing
- Integrity/Security
  - P1_READ_BEYOND_ALLOC_STRICT=1 — forbid reads beyond logical allocation
  - P1_ZERO_CHECKSUM_STRICT=1 — forbid zero CRC trailers in CRC mode
  - P1_TDE_STRICT=1 — forbid CRC fallback when AEAD tag fails (TDE on)
- Bloom
  - P1_BLOOM_MMAP=1 — use mmap for bloom.bin
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

- “page checksum mismatch” or “AEAD tag verify failed”
  - Data corruption or wrong TDE key; in AEAD strict mode CRC fallback is disabled
- “page N not allocated (next_page_id=…)”
  - Enable P1_READ_BEYOND_ALLOC_STRICT=0 (dev) or advance allocation via WAL/replay
- CDC apply not reflecting updates
  - Check LSN progression; HEADS_UPDATE requires LSN gating; verify last_heads_lsn on follower
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

## Changelog & License

- Changelog: [CHANGELOG.md](CHANGELOG.md)
- License: MIT — see [LICENSE](LICENSE)