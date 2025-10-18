# QuiverDB 2.2

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Release](https://img.shields.io/github/v/release/artemonad/QuiverDB?display_name=tag&sort=semver)](https://github.com/artemonad/QuiverDB/releases/latest)
[![GitHub Stars](https://img.shields.io/github/stars/artemonad/QuiverDB?style=social)](https://github.com/artemonad/QuiverDB/stargazers)

Embedded Rust key–value database with per‑page Robin Hood indexing, WAL, a CRC‑validated directory of bucket heads, and overflow chains for large values.

Status: 2.2. Stable on‑disk/wire formats:
- On‑disk: meta v4, page v3 (KV_RH3, OVERFLOW3), directory v2
- Wire: WAL v2 (P2WAL001, CRC32C; types: 1=BEGIN, 2=PAGE_IMAGE, 4=COMMIT, 5=TRUNCATE, 6=HEADS_UPDATE; 3=DELTA reserved)

Highlights
- Fixed 16‑byte page trailer
  - Default: CRC32C (Castagnoli), digest stored in low 4 bytes; remaining 12 bytes are zero.
  - Optional AES‑GCM trailer (integrity‑only, tag‑only; page payload is not encrypted).
- WAL v2 with HEADS_UPDATE in batch (directory updates are LSN‑gated)
- Real batch commit (one fsync per batch)
- Read‑side TTL and tombstones
- OVERFLOW3 with per‑page zstd compression (optional)
- Compaction: single‑scan head→tail + KV‑packing (multiple records per KV page)

What’s new in 2.2
- SnapStore dir override
  - P1_SNAPSTORE_DIR lets you place SnapStore (objects/refs/manifests) outside the DB root.
  - Relative path is resolved against DB root; absolute path is used as‑is.
- Snapshot delete
  - API: SnapshotManager::delete_persisted(root, id) — dec‑ref objects, remove manifest.
  - CLI: quiverdb snapshot-delete --path ./db --id <snapshot_id>.
- CDC apply hardening
  - PSK streams require HELLO (WAL header + stream_id) by default.
  - Strict monotonic seq and strict HEADS_UPDATE payload validation toggles.
  - Env:
    - P1_CDC_SEQ_STRICT=1 — seq regression is an error (non‑strict: warn+skip).
    - P1_CDC_HEADS_STRICT=1 — invalid HEADS_UPDATE payload length is an error (non‑strict: warn+skip).
    - P1_CDC_ALLOW_NO_HELLO=1 — allow no‑HELLO fallback (dev only; warns).

What was new in 2.1 (kept in 2.2)
- TDE (AES‑GCM) integrity‑only trailer; constant‑time verify; AAD="P2AEAD01"||page[0..16].
- Epoch‑aware CRC fallback (TDE on): fallback allowed only for page_lsn < since_lsn of the latest KeyJournal epoch; current epoch strictly rejects fallback.
- In‑memory keydir with offsets (pid, off) → get/exists read exact records (no per‑page scan), with correct TTL/tombstone fallback.
- Bloom MMAP safety: full‑file mapping from offset=0 (page‑aligned).

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
quiverdb status --path ./db2 --json
quiverdb doctor --path ./db2
quiverdb bloom --path ./db2
```

WAL/CDC:
```bash
# Ship WAL v2 frames to a file
quiverdb cdc-ship --path ./db2 --to file://./wal-stream.bin

# Apply from a file stream into follower
quiverdb cdc-apply --path ./follower --from file://./wal-stream.bin
```

Snapshots (persisted, 2.2):
```bash
# Create snapshot (SnapStore + manifest v2)
quiverdb snapshot-create --path ./db2 --message "baseline" --label prod --label v2

# List/inspect
quiverdb snapshot-list --path ./db2
quiverdb snapshot-inspect --path ./db2 --id <snapshot_id> --json

# Restore DB from snapshot into a new root
quiverdb snapshot-restore --path ./dst --src ./db2 --id <snapshot_id> --verify

# Delete snapshot (dec-ref objects + remove manifest)
quiverdb snapshot-delete --path ./db2 --id <snapshot_id>
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

## SnapStore (2.2)

- Content‑addressed store for pages (SHA‑256), with refcounts.
- Location override via P1_SNAPSTORE_DIR:
  - Absolute path: used as‑is.
  - Relative path: resolved against DB root.
  - Default (unset/empty): <db_root>/.snapstore.

Manifests (v2)
- Stored in <snapstore_dir>/manifests/<id>.json.
- Contains DB “frame” (page_size/next_page_id/lsn), heads (bucket→head_pid), and page object bindings.

Snapshot lifecycle
- Create: SnapshotManager::create_persisted(&db_ro, message, labels, parent) -> id
- Delete: SnapshotManager::delete_persisted(root, id)
- Restore: restore_from_id(src_root, dst_root, id, verify)

CLI recap:
```bash
# Override location (absolute)
P1_SNAPSTORE_DIR=/mnt/snapstore quiverdb snapshot-create --path ./db
# Override location (relative to DB root)
P1_SNAPSTORE_DIR=.cache/snapstore quiverdb snapshot-list --path ./db
```

---

## Compaction (single‑scan + packing)

- One pass head→tail; per page “newest→oldest”.
- Selects first valid record per key (tombstone wins; TTL read‑side).
- Writes compacted data using KvPagePacker (multiple records per page).
- Overflow values are not expanded; placeholders are preserved as‑is.

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

- expires_at_sec (u32, absolute Unix time; 0 = immortal).
- Skip records with now >= expires_at_sec.
- Tombstone (vflags bit 0 = 1) has priority.
- Metric ttl_skipped counts TTL‑based skips.

---

## CDC and WAL v2 (P2WAL001)

- CRC32C on header-before-crc + payload.
- Record types: 1=BEGIN, 2=PAGE_IMAGE, 3=PAGE_DELTA (reserved), 4=COMMIT, 5=TRUNCATE, 6=HEADS_UPDATE.
- Unknown types ignored; partial tails treated as EOF.
- HEADS_UPDATE is LSN‑gated (apply only when wal_lsn > last_heads_lsn).

2.2 apply hardening
- HELLO (WAL header + stream_id) is required for PSK streams by default.
- Strict seq and strict HEADS_UPDATE (payload len multiple of 12) via env toggles:
  - P1_CDC_SEQ_STRICT=1
  - P1_CDC_HEADS_STRICT=1
  - P1_CDC_ALLOW_NO_HELLO=1 (development only)

---

## Format overview (2.x)

- Page v3
  - KV_RH3: [klen u16][vlen u32][expires_at_sec u32][vflags u8][key][value]
    - Slot table (6‑byte slots) supports multiple records per page.
  - OVERFLOW3: codec_id (0=none, 1=zstd), chunk_len bytes on page (compressed when codec!=0).
  - 16‑byte trailer: CRC32C by default or AES‑GCM tag (integrity‑only).
- Meta v4
  - page_size, hash_kind, last_lsn, clean_shutdown, codec_default (0=none, 1=zstd), checksum_kind (CRC32C default).
- Directory v2
  - Single shard (dir‑000) with CRC32C and atomic tmp+rename (in‑place mode for dev/bench).
- WAL v2
  - P2WAL001 with CRC32C, BEGIN/IMAGE/COMMIT, TRUNCATE, HEADS_UPDATE (type=6).

---

## Configuration

Programmatic:
- QuiverConfig::from_env() and DbBuilder for explicit overrides.

Common ENV toggles:
- Performance
  - P1_WAL_DISABLE_FSYNC=1 — disable WAL fsyncs (bench/dev).
  - P1_WAL_COALESCE_MS=N — group‑commit window.
  - P1_DATA_FSYNC=0|1 — fsync data segments on commit (default 0).
  - P1_PAGE_CACHE_PAGES=N — process‑wide page cache (default 4096).
  - P1_PAGE_CACHE_OVF=1 — allow caching OVERFLOW pages.
  - P1_PREALLOC_PAGES=N — hot preallocation on the last touched segment.
  - P1_SEG_WRITE_BUF_MB=N — segment writer buffer (MiB; default 16).
  - P1_PACK_THRESHOLD_BYTES=N — small value threshold for packing.
- Integrity/Security
  - P1_READ_BEYOND_ALLOC_STRICT=1 — forbid reads beyond logical allocation.
  - P1_ZERO_CHECKSUM_STRICT=1 — forbid zero CRC trailers in CRC mode.
  - P1_TDE_STRICT=1 — forbid CRC fallback when AEAD tag fails (TDE on).
- CDC
  - P1_CDC_SEQ_STRICT=1 — strict monotonic seq on apply.
  - P1_CDC_HEADS_STRICT=1 — strict HEADS_UPDATE payload validation.
  - P1_CDC_ALLOW_NO_HELLO=1 — allow PSK apply without HELLO (dev only).
- Bloom
  - P1_BLOOM_MMAP=1 — use mmap for bloom.bin (full‑file mapping from offset 0).
  - P1_BLOOM_CACHE_BUCKETS=N — per‑process Bloom LRU cache.
- SnapStore
  - P1_SNAPSTORE_DIR — absolute (as‑is) or relative (to DB root) path for SnapStore.

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
  - TDE: tag‑only mode enforces integrity; with TDE strict mode CRC fallback is disabled.
  - Without TDE, enable ZERO_CHECKSUM_STRICT to treat zero CRC as invalid.
- “page N not allocated (next_page_id=…)”
  - Enable P1_READ_BEYOND_ALLOC_STRICT=0 (dev) or advance allocation via WAL/replay.
- “CDC: missing HELLO”
  - PSK streams must start with a WAL header; set P1_CDC_ALLOW_NO_HELLO=1 for dev fallback (not recommended).
- “CDC: seq regression”
  - Enable P1_CDC_SEQ_STRICT=1 to fail on regressions instead of warn+skip frames.
- “CDC: invalid HEADS_UPDATE payload len”
  - Enable P1_CDC_HEADS_STRICT=1 to treat invalid payloads as errors; otherwise they are skipped with a warning.
- “Bloom not used”
  - Filter may be stale (different last_lsn or buckets); rebuild with quiverdb bloom.

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