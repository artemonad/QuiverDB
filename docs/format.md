# QuiverDB 2.0 — On‑Disk and Wire Formats (GA)

Status
- 2.0 (GA). Stable formats:
  - On‑disk: meta v4, page v3 (KV_RH3 and OVERFLOW3), directory v2 (single shard)
  - Wire: WAL v2 (P2WAL001)
- Endianness: all integers are Little Endian (LE)
- Checksums/trailer:
  - Fixed 16‑byte page trailer
  - Default checksum is CRC32C (Castagnoli): low 4 bytes store digest (LE), remaining 12 bytes are zero
  - With TDE enabled: trailer stores AEAD tag (AES‑256‑GCM), no CRC; AAD = "P2AEAD01" || page[0..16]; nonce derived from (page_id, lsn)

Contents
- 1) Meta (v4)
- 2) Pages (v3): common, KV_RH3, OVERFLOW3, value placeholders
- 3) Directory (v2)
- 4) WAL (v2)
- 5) Reader semantics (TTL/tombstone, tail‑wins)
- 6) Compaction (single‑scan + packing)
- 7) TDE (AEAD‑tag trailer)
- 8) Compatibility and migration

---

## 1) Meta (v4) — file <root>/meta

Magic: "P2DBMETA" (8 bytes)

Layout (LE):
- u32 version         = 4
- u32 page_size       (power of two; 4 KiB..1 MiB)
- u32 flags           (format flags; reserved)
- u64 next_page_id
- u32 hash_kind       (1 = xxhash64(seed=0))
- u64 last_lsn
- u8  clean_shutdown  (1 = clean; 0 = unclean)
- u16 codec_default   (0=none, 1=zstd, 2=lz4)
- u8  checksum_kind   (0=crc32, 1=crc32c [default], 2=blake3_128 reserved)

Atomicity:
- Write via tmp+rename and fsync parent directory (best effort on Windows)

---

## 2) Page (v3) — common header/trailer

Page size = page_size (ps). Trailer at the very end:
- trailer region = ps‑16 .. ps‑1 (16 bytes)
- For CRC mode:
  - Compute digest over the entire page with trailer zeroed
  - Store digest (LE u32) at page[ps‑16..ps‑12], zero the remaining 12 bytes
- For TDE (AEAD tag):
  - Trailer is a 16‑byte AES‑GCM tag; no CRC is used

Common header prefix (first 16 bytes):
- 0..3:   MAGIC4 = "P2PG"
- 4..5:   u16 version = 3
- 6..7:   u16 type    (2=KV_RH3; 3=OVERFLOW3)
- 8..15:  u64 page_id

Checksum rule (CRC mode)
- Zero trailer, compute CRC32C over page bytes, store digest (u32 LE) at trailer[0..4], zero trailer[4..16]

AEAD rule (TDE mode)
- AAD = "P2AEAD01" || page[0..16] (magic/version/type/page_id)
- Nonce = derive_gcm_nonce(page_id, lsn) (low 48 bits from each)
- Tag computed over page with trailer zeroed; store 16‑byte tag in trailer

---

## 2.1 KV_RH3 (type=2)

Header (after common prefix):
- u32 data_start      (first free byte in data area)
- u32 table_slots     (slot capacity at the page tail)
- u32 used_slots
- u32 flags           (reserved)
- u64 next_page_id
- u64 lsn
- u16 codec_id        (0=none, 1=zstd, 2=lz4 — reserved for KV compression; not used in 2.0 GA)

Data area and slot table:
- Slot size = 6 bytes: [off u32][fp u8][dist u8]
- Slot table starts at: table_start = ps - trailer_len - table_slots*6
- Data area ends at: data_end = table_start (for table_slots > 0) or ps - trailer_len (for table_slots == 0)
- data_start ≤ data_end must hold
- In 2.0 GA, writer supports multiple KV per page using slot table (packing)

KV record encoding in data area:
- [klen u16][vlen u32][expires_at_sec u32][vflags u8][key][value]
- vflags: bit 0 = tombstone
- expires_at_sec: absolute Unix time; 0 means “immortal”

Reader notes:
- Packed‑aware lookup uses slot table in reverse order (newest→oldest) to prefer newer records on the same page
- Linear fallbacks must only scan real data [KV_HDR_MIN .. min(data_end, data_start)), never the free area

---

## 2.2 OVERFLOW3 (type=3)

Header (after common prefix):
- u32 chunk_len       (bytes stored on this page; compressed length when codec_id!=0)
- u32 reserved
- u64 next_page_id    (u64::MAX means end of chain)
- u64 lsn
- u16 codec_id        (0=none, 1=zstd, 2=lz4 — reserved)

Payload:
- Bytes [header_min .. header_min + chunk_len)
- header_min = 64 bytes for v3
- If codec_id != 0, payload is compressed (zstd in 2.0)
- Consumers decompress page‑by‑page and concatenate uncompressed bytes to reconstruct the original value

Capacity:
- chunk_len ≤ ps - header_min - trailer_len

Chain:
- Pages linked via next_page_id to NO_PAGE (u64::MAX)
- Reader must verify total uncompressed length against expected_len from the KV placeholder

---

## 2.3 Value placeholders (TLV in KV values)

Encoding:
- [tag u8][len u8][payload …]

Tags:
- 0x01 OVF_CHAIN
  - payload: [total_len u64][head_pid u64]
  - Semantics: value stored across an OVERFLOW3 chain starting at head_pid with total_len bytes

---

## 3) Directory (v2) — file <root>/dir-000

File layout:
- MAGIC8  = "P2DIR02\0"
- u32 version = 2
- u32 buckets_in_shard
- u32 crc32c  (CRC over [version u32][buckets u32] + heads bytes)
- heads: array of u64 (one per bucket); NO_PAGE=u64::MAX

Atomic update:
- Write tmp file with header (CRC computed) and heads, fsync, rename over shard, fsync parent dir (best effort on Windows)

---

## 4) WAL (v2) — P2WAL001

Global header (16 bytes):
- MAGIC8 = "P2WAL001"
- u32 reserved
- u32 reserved

Record header (LE, 28 bytes):
- u8  type
- u8  flags
- u16 reserved
- u64 lsn
- u64 page_id        (0 for non‑page records)
- u32 len            (payload length)
- u32 crc32c         (over header[0..crc] + payload)

Types:
- 1 = BEGIN
- 2 = PAGE_IMAGE (payload = full page v3; may contain compressed OVERFLOW3 data)
- 3 = PAGE_DELTA (reserved for future)
- 4 = COMMIT
- 5 = TRUNCATE
- 6 = HEADS_UPDATE (payload = repeated [bucket u32][head_pid u64])

Rules:
- Idempotence:
  - PAGE_IMAGE applies only if wal_lsn > page_lsn (LSN gating)
  - HEADS_UPDATE applies only if wal_lsn > last_heads_lsn persisted by consumer
- Unknown types are ignored (forward‑compatible)
- Partial tails are treated as EOF (normal stop)
- CRC32C verification is mandatory for complete records

Streaming:
- TRUNCATE may be followed by a repeated global header mid‑stream; consumers must tolerate and continue

---

## 5) Reader semantics: TTL, tombstone and tail‑wins

- Tombstone (vflags bit 0 = 1) has priority: deletes the key regardless of TTL
- TTL (expires_at_sec u32):
  - 0 — no TTL (immortal)
  - When current Unix time >= expires_at_sec, the record is considered expired and must be ignored on reads
- Tail‑wins:
  - Readers traverse from head to tail and accept the first valid (non‑tombstone, non‑expired) record for a key
  - Packed pages are scanned newest→oldest (reverse slot order)

---

## 6) Compaction (2.0 GA)

- Single‑scan head→tail; per page iterate newest→oldest
- For each key, the first valid record is selected (tombstone/TTL semantics)
- Output written via KvPagePacker (multiple records per KV page) → much shorter chains and fewer future IOs
- Overflow placeholders are preserved (no large value rewrite)
- Directory heads are updated atomically; Bloom side‑car may be delta‑updated (best effort)

---

## 7) TDE (AEAD‑tag trailer)

- When TDE is enabled, per‑page trailer stores a 16‑byte AES‑GCM tag
- AAD = "P2AEAD01" || page[0..16] (magic/version/type/page_id)
- Nonce = derive_gcm_nonce(page_id, lsn) (low 48 bits of each)
- No CRC is used with TDE; strict mode (P1_TDE_STRICT=1) disallows CRC fallback

---

## 8) Compatibility and migration

- 2.0 on‑disk formats are not compatible with 1.x; use offline converter:
  - quiverdb convert --from ./db_v1 --to ./db2 [--page-size 65536] [--codec zstd]
- WAL v2 is not compatible with WAL v1 tools
- PAGE_DELTA (type=3) is reserved; consumers must ignore it
- KV_RH3 data‑area compression is not enabled in 2.0 GA; OVERFLOW3 compression is implemented
- Default checksum policy is CRC32C with a fixed 16‑byte trailer in non‑TDE mode