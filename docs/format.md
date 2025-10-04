# QuiverDB On-Disk Format (v1.0.0)

This document specifies the on-disk formats: meta v3, page v2, WAL v1, directory, and free-list. All values are little-endian. Formats are frozen in v1.0.0.

Abbreviations: LE = little-endian, u32/u64 = 32/64-bit unsigned.


## 1) Meta (v3)

File: `<root>/meta`
- Magic: `P1DBMETA` (8 bytes)
- Layout (LE):
    - `u32 version`        — 3
    - `u32 page_size`      — power of two, >= 4096, <= 65535 (16-bit offsets in pages)
    - `u32 flags`          — reserved (bit 0 for TDE in future)
    - `u64 next_page_id`   — number of allocated pages so far (next id to allocate)
    - `u32 hash_kind`      — key hashing (1 = xxhash64(seed=0))
    - `u64 last_lsn`       — last committed WAL LSN
    - `u8  clean_shutdown` — 1 = clean, 0 = unclean (writer updates this)
- Atomicity: written via temp file + rename; parent directory fsync on Unix (best-effort on Windows).


## 2) Page (v2)

All pages are `page_size` bytes. Header is 64 bytes:

Header (64 bytes):
- `MAGIC4`                 — `P1PG`
- `u16 version`            — 2
- `u16 type`               — `2` (KV_RH) or `3` (OVERFLOW)
- `u32 pad`                — reserved
- `u64 page_id`
- `u16 data_start`         — first free byte in the data area
- `u16 table_slots`        — number of slots allocated at the end of the page
- `u16 used_slots`
- `u16 flags`              — reserved
- `u64 next_page_id`       — page chaining (KV_RH: next in bucket chain; OVERFLOW: next in overflow chain)
- `u64 lsn`                — page LSN (used for WAL apply gating)
- `u64 seed`               — per-page seed (KV_RH hashing)
- `u32 crc32`              — CRC of the whole page with this field zeroed for calculation

CRC: computed over the whole page with the 4-byte crc field zeroed.

Types:
- KV_RH (2):
    - Data area (beginning at `data_start`): records `[klen u16][vlen u16][key bytes][value bytes]`.
    - Slot table (trailing area): array of 4-byte slots `[off u16][fp u8][dist u8]`.
    - Robin Hood probing: insert/update/delete logic maintains `dist`.
- OVERFLOW (3):
    - `data_start` re-used as chunk length (`u16 chunk_len`).
    - The page stores `chunk_len` bytes at `[64..64+chunk_len]`.
    - Chains via `next_page_id`.


## 3) Overflow placeholder (in KV values)

When a value is stored in overflow pages, the KV entry stores a fixed 18-byte placeholder:
- `[u8 tag=0xFF][u8 pad=0x00][u64 total_len][u64 head_page_id]`

The reader resolves it by walking the overflow chain starting from `head_page_id` for `total_len` bytes.


## 4) WAL (v1)

File: `<root>/wal-000001.log`

Header (16 bytes):
- `WAL_MAGIC8`  — `P1WAL001`
- `u32 reserved`
- `u32 reserved`

Record header (28 bytes):
- `u8  type`      — `1` = PAGE_IMAGE; `2` = TRUNCATE (streaming ship only)
- `u8  flags`
- `u16 reserved`
- `u64 lsn`       — WAL LSN for ordering and gating
- `u64 page_id`   — target page id
- `u32 len`       — payload length (bytes)
- `u32 crc32`     — CRC32 over `[header bytes up to crc32] + payload`

Payload:
- For `PAGE_IMAGE`: the full page image (`page_size` bytes).
- For `TRUNCATE`: empty payload (len = 0), used by ship stream to signal rotate/truncate.

Replay rules:
- Verify CRC.
- Stop on partial tail / short read (treat as normal EOF).
- Ignore unknown `type` values (forward-compatible).
- Apply only if `wal_lsn > page_lsn` for v2 pages (KV_RH/OVERFLOW).
- Truncate WAL back to header after successful replay.
- Update `meta.last_lsn` best-effort.

Streaming CDC:
- `wal-ship` can send a mid-stream WAL header (after `TRUNCATE`); `wal-apply` tolerates it and continues.


## 5) Directory (buckets)

File: `<root>/dir`

Header (24 bytes):
- `DIR_MAGIC8` — `P1DIR001`
- `u32 version`  — 1
- `u32 buckets`  — number of buckets
- `u64 reserved` — stores CRC32 in low 32 bits

Heads:
- Array of `u64` (LE), one per bucket. `NO_PAGE = u64::MAX` marks empty.

CRC:
- CRC32 is computed over `[version u32][buckets u32] + heads bytes` and stored in `reserved` low 32 bits.
- `Directory::open` validates CRC when non-zero.
- `set_head(bucket, page_id)` updates directory via tmp+rename and re-writes CRC.


## 6) Free-list

File: `<root>/free`

Header (24 bytes):
- `FREE_MAGIC8` — `P1FREE01`
- `u32 version` — 1
- `u32 count`   — best-effort count (not authoritative)
- `u64 reserved`

Tail:
- Sequence of `u64` page IDs appended/removed by push/pop.

Truth of `count`:
- Actual count is `(len(file) - FREE_HDR_SIZE) / 8`. The header `count` is updated best-effort only.


## 7) Endianness, alignment, sizes

- All integers are little-endian.
- Page size: power-of-two, 4096..=65535 (due to 16-bit offsets in-page).
- All headers and arrays are byte-packed; no additional padding on disk beyond typedef layouts above.


## 8) LSN-gating specifics

- KV_RH and OVERFLOW pages embed `lsn` in page header.
- WAL records carry `wal_lsn`.
- Apply a page image only if `wal_lsn > page_lsn`.
- If a page is not recognized as v2 or no `lsn` could be parsed, apply conservatively (i.e., do not gate).
- After replay, truncate WAL and update `meta.last_lsn` best-effort.


## 9) Integrity tools

- `check --strict` returns an error when:
    - directory fails to open/validate (bad magic/version/CRC),
    - page read reports CRC/IO errors,
    - overflow orphans are found (unreachable from directory heads and not in free-list).
- `repair` conservatively frees non-reachable overflow pages (adds to free-list).