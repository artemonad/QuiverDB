# QuiverDB CDC Guide (v1.1.5)

Audience: operators and developers building change capture/replication on top of QuiverDB.

Status: v1.1.5 (formats frozen: meta v3, page v2, WAL v1)

QuiverDB exposes CDC around its Write-Ahead Log (WAL). CDC streams full page images with CRC and monotonic LSN. Consumers apply frames idempotently with LSN-gating on v2 pages.


## 1) Overview

- WAL frames carry:
  - type (PAGE_IMAGE or TRUNCATE in streams),
  - monotonic LSN (u64),
  - page_id (u64) for page images,
  - payload length,
  - CRC32 over header-before-crc + payload.
- Idempotency via LSN-gating:
  - Apply a v2 page image only if wal_lsn > page_lsn.
  - Unknown record types are ignored (forward-compatible).
- Partial tails and rotations:
  - Short reads (partial record header/payload) are treated as normal EOF.
  - Stream may include TRUNCATE markers and mid-stream WAL headers after rotation; consumers must skip mid-stream headers and continue.
- Built-in sinks:
  - stdout (default), TCP (sink=tcp://host:port), file (sink=file://path).
- Deterministic record/replay:
  - cdc record extracts a slice of the wire-format WAL to a file; cdc replay applies it idempotently (with optional LSN filters).


## 2) Commands

- Tail WAL as JSONL (observability):
  - quiverdb wal-tail --path ./db [--follow]
- Ship WAL stream (producer):
  - quiverdb wal-ship --path ./db [--follow] [--since-lsn N] [--sink=tcp://host:port]
  - quiverdb wal-ship --path ./db [--follow] [--since-lsn N] [--sink=file://./out.bin]
  - When --sink is omitted, the stream is written to stdout.
- Apply stream on follower (consumer):
  - quiverdb wal-apply --path ./follower  (reads from stdin)
- Deterministic record/replay:
  - quiverdb cdc record --path ./leader --out ./slice.bin [--from-lsn X] [--to-lsn Y]
  - quiverdb cdc replay --path ./follower [--input ./slice.bin] [--from-lsn X] [--to-lsn Y]
- Follower checkpoint (resume):
  - quiverdb cdc last-lsn --path ./follower


## 3) Wire formats

All integers are Little Endian (LE).

3.1 JSONL (wal-tail)
- Each line: JSON object with common fields:
  - type: "page_image" | "unknown"
  - lsn: u64
  - page_id: u64 (for page_image)
  - len: u32 (payload bytes)
  - crc32: u32
- Events: when the underlying file truncates, wal-tail prints {"event":"truncate", ...}.
- Example:
  {"type":"page_image","lsn":42,"page_id":7,"len":4096,"crc32":1234567890}

3.2 Binary stream (wal-ship)
- Stream layout:
  - Initial 16-byte WAL header
  - Zero or more records, each:
    - 28-byte record header
    - payload (len bytes)
- WAL header (16 bytes, LE):
  - 0..7:   WAL_MAGIC8 = "P1WAL001"
  - 8..11:  u32 reserved
  - 12..15: u32 reserved
- Record header (28 bytes, LE):
  - 0:   u8  type            (1 = PAGE_IMAGE, 2 = TRUNCATE)
  - 1:   u8  flags
  - 2..3: u16 reserved
  - 4..11: u64 lsn
  - 12..19: u64 page_id      (0 for non-page_image types)
  - 20..23: u32 len          (payload length)
  - 24..27: u32 crc32        (over header[0..24] + payload)
- Payload:
  - PAGE_IMAGE: full page image bytes (page_size).
  - TRUNCATE: empty (len=0), a marker used by ship to signal rotation/truncation.
- Rotation/truncate:
  - On rotation, wal-ship sends a TRUNCATE record and then re-sends the 16-byte WAL header mid-stream. Consumers must skip mid-stream headers and continue.

3.3 TCP sink and resume by LSN
- wal-ship can connect to a TCP sink directly and resume from a given LSN:
  - Leader (producer): quiverdb wal-ship --path ./leader --since-lsn N --sink=tcp://follower:9999 --follow
  - Follower (consumer): nc -l -p 9999 | quiverdb wal-apply --path ./follower
- Get follower’s checkpoint:
  - quiverdb cdc last-lsn --path ./follower
- since-lsn semantics:
  - Default: frames with wal_lsn > N (exclusive).
  - Inclusive mode (wal_lsn >= N) is available via ENV (see section 7).


## 4) wal-apply (follower applier)

Basic flow:
1. Read the initial 16-byte WAL header (required at the beginning).
2. Loop:
  - Read 8 bytes. If equal to WAL_MAGIC, read next 8 bytes (mid-stream header) and continue.
  - Otherwise, read the remaining 20 bytes of the record header (total 28).
  - Read payload (len bytes).
  - Short reads (partial header/payload) are treated as normal EOF; exit cleanly.
  - Verify CRC (header-before-crc + payload). Mismatch in a complete frame is an error.
  - If type = PAGE_IMAGE:
    - If the target page exists, read it and compare page LSN.
    - Apply only if new_lsn > cur_lsn (LSN-gating).
    - Allocate (ensure_allocated) only if you are actually applying.
  - If type = TRUNCATE: ignore (used only to synchronize rotation).
  - If type is unknown: ignore (forward-compatible).
3. Track the maximum wal_lsn seen and update follower meta.last_lsn best-effort at the end.

Idempotency:
- Replaying the same frames is safe due to the LSN-gating on v2 pages.
- Unknown record types are ignored.

Partial tails:
- Short reads for a record header or payload are treated as normal EOF (producer not finished yet). No error.

Error handling:
- CRC mismatch within a complete frame aborts with error (stream corruption).
- Unknown record types do not error; they are skipped.


## 5) End-to-end examples

5.1 Local replication (stdin/stdout)
- Leader → Follower on the same host:
  quiverdb wal-ship --path ./leader --follow | quiverdb wal-apply --path ./follower
- Stop the producer or consumer; wal-apply finishes gracefully on EOF.

5.2 Built-in TCP ship with resume
- On the follower (listener):
  nc -l -p 9999 | quiverdb wal-apply --path ./follower
- On the leader (producer):
  LSN=$(quiverdb cdc last-lsn --path ./follower)
  quiverdb wal-ship --path ./leader --since-lsn $LSN --sink=tcp://follower_host:9999 --follow

5.3 Inspecting frames in JSON
- Human-friendly:
  quiverdb wal-tail --path ./db --follow | jq .

5.4 Deterministic record/replay (files)
- Record a slice of the wire stream (LSN filtered):
  quiverdb cdc record --path ./leader --out ./slice.bin --from-lsn 100 --to-lsn 200
- Replay from a file (or stdin) on a follower:
  quiverdb cdc replay --path ./follower --input ./slice.bin --from-lsn 100 --to-lsn 200

5.5 File sink
- Produce a full WAL stream to a file (ship writes header + frames):
  quiverdb wal-ship --path ./leader --sink=file://./full.bin
- Apply the saved stream later:
  cat ./full.bin | quiverdb wal-apply --path ./follower


## 6) Resuming and checkpoints

- wal-apply is idempotent and does not require a start LSN (replaying old frames is harmless).
- For network efficiency:
  - Compute follower’s last LSN: quiverdb cdc last-lsn --path ./follower
  - Pass it as --since-lsn to wal-ship (exclusive by default): only frames with lsn > follower_lsn are sent.
  - Inclusive mode (lsn >= follower_lsn) is available via ENV (see below).
- meta.last_lsn on the follower is updated best-effort after apply; you can persist it externally.


## 7) Performance and environment

Group-commit (WAL fsync coalescing):
- P1_WAL_COALESCE_MS (default 3 ms) delays fsync to batch consecutive commits.

Data fsync policy:
- P1_DATA_FSYNC=1 (default): data segments are fsync’d on every commit, and WAL can be truncated periodically (rotate).
- P1_DATA_FSYNC=0: rely solely on WAL durability; commit does not truncate WAL. WAL truncates during replay after an unclean shutdown.

CDC ship batching and filters:
- P1_SHIP_FLUSH_EVERY=N
  - Flush every N frames (default 1 — same behavior as before).
- P1_SHIP_FLUSH_BYTES=B
  - Flush when at least B bytes have been written since the previous flush (default 0 — disabled).
- P1_SHIP_SINCE_INCLUSIVE=1|true|yes|on
  - Treat --since-lsn N as inclusive (ship frames with wal_lsn >= N). Default is exclusive (wal_lsn > N).

Bandwidth and compression:
- CDC uses full page images; bandwidth ~ page_size per updated page (+ headers).
- Consider compression:
  - wal-ship ... | gzip -1 | nc ...
  - zstd -q --adapt is another option.


## 8) Sinks and security

Sinks:
- stdout (default),
- TCP: sink=tcp://host:port (uses a direct TCP connection; no TLS),
- File: sink=file://path (overwrites the file; includes the WAL header).
  Security:
- Use TLS-capable tunnels for WAN (stunnel, SSH, VPN) or implement a TLS TCP sink as needed.
- Run wal-apply under a supervisor (systemd, runit) with restart on failure.
- Monitor the leader with wal-tail for activity confirmation (e.g., push to logs/metrics).
- Restrict filesystem permissions of the database path and WAL file.


## 9) Troubleshooting

- wal-apply stops “early”:
  - Often a partial tail: the producer hasn’t emitted a full frame yet. This is normal on --follow. Reconnect or keep the producer running.
- CRC mismatch in wal-apply:
  - Indicates stream corruption or a bug; restart and inspect the transport layer.
- Unknown record types in wal-tail:
  - Forward-compatibility: older clients print "type":"unknown" and skip them.
- Follower not reflecting updates:
  - Verify wal-ship --path points at the correct leader.
  - Check LSN progression on the leader (wal-tail).
  - Ensure follower can write (not opened in RO by another process).
- since-lsn behavior:
  - Default is exclusive (> N). Set P1_SHIP_SINCE_INCLUSIVE=1 to make it inclusive (>= N).


## 10) Wire summary (for implementers)

All integers are Little Endian (LE).

- WAL header (16 bytes):
  - 0..7:   WAL_MAGIC = "P1WAL001"
  - 8..11:  u32 reserved
  - 12..15: u32 reserved
- Record header (28 bytes):
  - 0:   u8  type
  - 1:   u8  flags
  - 2..3:  u16 reserved
  - 4..11: u64 lsn
  - 12..19: u64 page_id
  - 20..23: u32 len
  - 24..27: u32 crc32  (CRC over header[0..24] + payload)
- Payload:
  - PAGE_IMAGE: page bytes
  - TRUNCATE: empty (streaming only)
- Consumers must skip mid-stream headers (WAL_MAGIC encountered while reading records), e.g., right after a TRUNCATE record.