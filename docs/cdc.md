# QuiverDB 2.0 — CDC Guide (WAL v2)

Audience: operators and developers building change capture/replication on QuiverDB 2.0.  
Status: 2.0 (GA). CDC is defined around WAL v2 (P2WAL001). The guide covers the wire format and producer/consumer behavior. A minimal reference apply is implemented in the library (writer startup replay and cdc-apply).

Contents
- 1) Overview
- 2) Wire format (P2WAL001)
- 3) Producer behavior (ship)
- 4) Consumer behavior (apply)
- 5) Crash recovery vs CDC
- 6) Batching and HEADS_UPDATE
- 7) Compression
- 8) Transport (PSK/TLS) and framing
- 9) Compatibility and migration
- 10) Troubleshooting
- Appendix — reference parsers (pseudo‑Rust)

---

## 1) Overview

- WAL v2 is an append‑only stream of page images (and in future, page deltas), protected with CRC32C and carrying a monotonically increasing LSN.
- Records are idempotent at the consumer via LSN gating:
  - Apply a PAGE_IMAGE only when wal_lsn > current_page_lsn stored in the page header.
  - Apply HEADS_UPDATE (directory updates) only when wal_lsn > last_heads_lsn persisted on the follower.
- Unknown record types are ignored (forward‑compatible).
- Partial tails (short record header/payload) are treated as EOF on the stream.
- 2.0 GA does not emit PAGE_DELTA; type=3 is reserved and must be ignored by consumers.

---

## 2) Wire format (P2WAL001)

All integers are Little Endian.

Global header (16 bytes)
- 0..7:   MAGIC = "P2WAL001"
- 8..11:  u32 reserved
- 12..15: u32 reserved

Record header (28 bytes)
- 0:      u8  type
- 1:      u8  flags
- 2..3:   u16 reserved
- 4..11:  u64 lsn
- 12..19: u64 page_id (0 for non‑page records)
- 20..23: u32 len (payload length)
- 24..27: u32 crc32c (over header[0..24] + payload)

Types
- 1 = BEGIN
- 2 = PAGE_IMAGE (payload = full page v3 as written to disk)
- 3 = PAGE_DELTA (reserved; ignore in 2.0)
- 4 = COMMIT
- 5 = TRUNCATE (rotation marker on streaming)
- 6 = HEADS_UPDATE (payload = repeated [bucket u32][head_pid u64])

CRC
- crc32c(header[0..24] + payload). Consumers must verify CRC32C on complete records.

Mid‑stream headers
- After a TRUNCATE marker, producers may resend the 16‑byte global header mid‑stream. Consumers must tolerate and continue.

Partial tails
- Short record header/payload at the end of the file/stream is considered EOF.

---

## 3) Producer behavior (ship)

A conforming ship implementation should:
- Emit the 16‑byte global header once at stream start (and optionally after TRUNCATE).
- Write each record as [28‑byte header][payload bytes] with valid CRC32C.
- Never split a header or a payload across frames (partial writes will be seen as EOF by consumers).
- On rotation/truncation of the source file, optionally emit a TRUNCATE record followed by the global header and continue.
- For Db::batch() writes, Pager emits one WAL batch (BEGIN, N × PAGE_IMAGE, HEADS_UPDATE, COMMIT). Ship may stream the records as they appear on disk; additional buffering is optional.

File sink
- The simplest ship is “copy WAL to a file” with all frames present (header+records). CLI: quiverdb cdc-ship --to file://path

---

## 4) Consumer behavior (apply)

A conforming apply implementation should:
- Read and validate the 16‑byte global header at the beginning (and skip mid‑stream repetitions after TRUNCATE).
- Loop records:
  - If a short read occurs (partial header or payload), treat as EOF and stop.
  - Verify CRC32C for complete records; mismatch indicates corruption (stop and report).
  - If type=PAGE_IMAGE:
    - Parse the page header (v3) from the payload bytes.
    - Apply only when wal_lsn > page_lsn (LSN gating). Ensure page allocation, then write the payload bytes at the correct segment offset.
  - If type=HEADS_UPDATE:
    - Apply only when wal_lsn > last_heads_lsn persisted on follower.
    - Parse payload as repeated LE tuples (bucket u32, head_pid u64); update directory heads atomically.
    - Persist new last_heads_lsn marker for resume idempotence.
  - If type=TRUNCATE: ignore (marker only).
  - If type=BEGIN/COMMIT: ignore (markers only).
  - Unknown types: ignore (forward‑compatible).
- Track max wal_lsn seen in the stream and update meta.last_lsn best‑effort on clean stop.
- Do not change meta.clean_shutdown (writers handle this at open/close).
- Optional strict read guard on the follower:
  - P1_READ_BEYOND_ALLOC_STRICT=1 forbids reading pages beyond logical allocation (page_id >= next_page_id) unless allocation is advanced by apply.

Notes:
- OVERFLOW3 pages are applied “as-is”; compression is per‑page (codec_id). No special handling in apply beyond writing the page image.

---

## 5) Crash recovery vs CDC

- Writer open (Db::open) uses on‑disk WAL v2 replay:
  - If meta.clean_shutdown=false: replay P2WAL001 from the file, applying PAGE_IMAGEs idempotently (LSN gating), then truncate WAL to header and mark clean_shutdown=true.
- CDC apply should not modify clean_shutdown; it may update meta.last_lsn best‑effort to aid resume logic.
- A follower fully synchronized by CDC should still start cleanly: writer open truncates WAL to the header (fast path).

---

## 6) Batching and HEADS_UPDATE

- BEGIN and COMMIT bracket batches produced by Pager::commit_pages_batch_with_heads().
- HEADS_UPDATE carries directory changes for all buckets affected in the batch and must be LSN‑gated on followers via last_heads_lsn.
- Consumers can optionally coalesce writes and fsync around batch boundaries, but correctness relies solely on LSN gating.

HEADS_UPDATE payload (repeated)
- [bucket u32][head_pid u64] (LE), repeated len/12 times
- No bucket appears more than once in a single update; if it does, the last one wins

---

## 7) Compression

- WAL PAGE_IMAGE contains the exact v3 page bytes. If a page type uses compression at page level (OVERFLOW3 with codec_id=1=zstd), the payload is compressed data stored inside the page — the WAL stream itself is not compressed.
- Stream compression (e.g., transport zstd/gzip) is optional and layered above P2WAL001; apply must decompress the stream before parsing P2WAL001.

---

## 8) Transport (PSK/TLS) and framing

PSK framing
- Framing: header(44) = [len u32][seq u64][mac[32]] followed by payload[len]
- MAC = HMAC‑SHA256 over MAGIC||seq||payload with the PSK
- ENV:
  - P1_CDC_PSK_HEX / P1_CDC_PSK_BASE64 / P1_CDC_PSK — pre‑shared key (≥16 bytes)
  - P1_CDC_SEQ_STRICT=1 — strict monotonic seq on apply (seq < last_seq → error)
- Apply should persist last_seq to allow idempotent resume.

TLS/mTLS (client)
- ENV:
  - P1_TLS_DOMAIN (SNI override)
  - P1_TLS_CA_FILE (PEM with one or more CERTIFICATE blocks)
  - P1_TLS_CLIENT_PFX / P1_TLS_CLIENT_PFX_PASSWORD (PKCS#12 identity for mTLS)

---

## 9) Compatibility and migration

- WAL v2 is not compatible with WAL v1 streams/tools.
- 2.0 on‑disk formats (meta v4/page v3/dir v2) are not compatible with 1.x. Use the offline converter:
  - quiverdb convert --from ./db_v1 --to ./db2 [--page-size 65536] [--codec zstd]
- PAGE_DELTA is reserved for a future 2.x release; consumers must ignore it.

---

## 10) Troubleshooting

- “Bad WAL magic”
  - Source file/stream is not a P2WAL001 WAL; ensure you’re shipping the correct data
- “CRC mismatch”
  - Indicates stream or storage corruption. Stop and re‑establish the stream or inspect the transport
- “EOF mid‑stream”
  - Likely a partial tail (producer hasn’t finished writing a full record). Treat as EOF and resume later
- “Follower not reflecting updates”
  - Check LSN progression on the producer
  - Verify the consumer is not skipping PAGE_IMAGEs due to equal/greater page_lsn (LSN gating)
  - Ensure the consumer has write permissions and can allocate pages
  - Verify HEADS_UPDATE is applied only when wal_lsn > last_heads_lsn

---

## Appendix — reference parsers (pseudo‑Rust)

Reader (file)
```rust
read_wal_header(); // "P2WAL001"
while let Some(rec) = read_next_record()? {
    if !crc_ok(&rec) { error!("crc mismatch"); break; }
    match rec.type {
        BEGIN|COMMIT|TRUNCATE => { /* ignore */ }
        PAGE_IMAGE => apply_page_image_if_lsn_greater(rec.lsn, rec.page_id, rec.payload),
        HEADS_UPDATE => apply_heads_if_lsn_greater(rec.lsn, rec.payload),
        _ => { /* ignore unknown */ }
    }
}
```

PSK‑framed stream (apply loop)
```rust
let mut last_seq = load_last_seq();
while let Some((seq, payload)) = read_next_framed_psk()? {
    if seq <= last_seq { continue; }
    let rec = parse_wal_record(payload)?;
    // crc guard
    if !crc_ok(&rec) { error!("crc mismatch"); break; }
    // idempotent apply
    match rec.type {
        PAGE_IMAGE => apply_if_lsn_greater(...),
        HEADS_UPDATE => apply_heads_if_lsn_greater(...),
        _ => {}
    }
    last_seq = seq; store_last_seq(last_seq);
}
```