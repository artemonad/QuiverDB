# CDC Guide (wal-tail / wal-ship / wal-apply)

Version: v1.0.0 (formats frozen: meta v3, page v2, WAL v1)
Audience: operators and developers building change capture/replication on top of QuiverDB.

QuiverDB provides simple CDC tooling around its Write-Ahead Log (WAL):
- wal-tail: JSONL view of WAL frames for logging/inspection.
- wal-ship: binary stream of raw WAL frames for replication.
- wal-apply: follower-side applier that consumes a WAL stream and applies page images idempotently.


## 1) Overview

- WAL contains full page-image records, each with CRC and monotonic LSN.
- CDC leverages the WAL to replicate page images to a follower database.
- The follower applies frames idempotently using LSN-gating on v2 pages (KV_RH and OVERFLOW).
- Streams are robust to partial tails and mid-stream WAL headers (after rotate/truncate).


## 2) Commands

- Tail as JSONL (for inspection/logging):

  ```quiverdb wal-tail --path ./db --follow```

- Ship binary WAL stream (producer):

  ```quiverdb wal-ship --path ./db --follow```

- Apply stream on the follower (consumer):

  ```quiverdb wal-ship --path ./leader --follow | quiverdb wal-apply --path ./follower```


## 3) Wire formats

### 3.1 JSONL (wal-tail)

Each line is a JSON object with common fields:
- type: "page_image" | "unknown" (future types)
- lsn: u64 (monotonic)
- len: u32 (payload length)
- page_id: u64 (for page_image)
- crc32: u32 (stored CRC in record header)
- Example:

  {"type":"page_image","lsn":42,"page_id":7,"len":4096,"crc32":1234567890}

wal-tail follows the WAL file like `tail -f`, handling file growth and reporting events (e.g., internal truncate notice printed as {"event":"truncate", ...} in the tool).


### 3.2 Binary stream (wal-ship)

The ship stream reproduces the WAL file structure:
- First, a 16-byte WAL header.
- Then a sequence of records, each as:
    - 28-byte record header
    - payload (len bytes)

WAL header (16 bytes, LE):
- WAL_MAGIC8 = "P1WAL001"
- u32 reserved
- u32 reserved

Record header (28 bytes, LE):
- u8  type            (1 = PAGE_IMAGE, 2 = TRUNCATE)
- u8  flags
- u16 reserved
- u64 lsn
- u64 page_id         (0 for non-page_image types)
- u32 len             (payload length)
- u32 crc32           (CRC over [header bytes before crc32] + payload)

Payload:
- For PAGE_IMAGE: full page image bytes (page_size).
- For TRUNCATE: empty (len = 0), a marker used by ship for rotate/truncate.

Rotation/truncate handling:
- When the WAL file truncates (e.g., checkpoint), `wal-ship` sends a TRUNCATE record and then re-sends the 16-byte WAL header. Consumers must tolerate mid-stream header and continue.


## 4) wal-apply (follower applier)

Basic flow:
1. Read the initial 16-byte WAL header (required at start).
2. Loop:
    - Read 8 bytes. If equal to WAL_MAGIC (mid-stream header), read 8 more bytes and continue.
    - Otherwise, read the remaining 20 bytes of the record header (total 28 bytes).
    - Read payload (len bytes).
    - If a short read occurs (partial tail): treat as normal EOF; stop gracefully.
    - Verify CRC (header-before-crc + payload). Mismatch = abort (stream corruption).
    - If type = PAGE_IMAGE:
        - Ensure the target page is allocated (`ensure_allocated`).
        - If payload looks like a v2 page (KV_RH or OVERFLOW), extract its page LSN (new_lsn).
        - Read current page (if any) and extract its LSN (cur_lsn).
        - Apply only if `new_lsn > cur_lsn` (LSN-gating).
    - If type = TRUNCATE: ignore (used only to synchronize rotation).
    - If type is unknown: ignore (forward-compatible).
3. Track the maximum LSN seen and update follower `meta.last_lsn` best-effort after finish.

Idempotency:
- Re-applying the same frames is safe due to LSN-gating.
- Unknown frames are ignored.

Partial tails:
- A short read for header or payload is treated as a normal EOF (stream ended mid-frame). No error.

Error handling:
- CRC mismatch within a complete frame results in an error (stream corruption).
- Unknown types do not error; they are skipped.


## 5) End-to-end examples

### 5.1 Local replication (stdin/stdout)

Leader → Follower on the same host:

    quiverdb wal-ship --path ./leader --follow | quiverdb wal-apply --path ./follower

Stop with Ctrl+C on either side; wal-apply will finish gracefully on EOF.

### 5.2 Over TCP (using netcat / socat)

On the leader (producer):

    quiverdb wal-ship --path ./leader --follow | nc -l -p 9999

On the follower (consumer):

    nc <leader_host> 9999 | quiverdb wal-apply --path ./follower

Production-grade deployments should use a TLS-capable transport (e.g., stunnel, ssh tunnel, or a custom TCP service).

### 5.3 Inspecting frames in JSON

    quiverdb wal-tail --path ./db --follow | jq .

Useful for debugging and building observability around WAL activity.


## 6) Resuming and checkpoints

- wal-apply does not require an explicit “start-from-LSN”: it is idempotent. You can replay from an older point; LSN-gating prevents regressions.
- The follower’s `meta.last_lsn` is updated best-effort after apply — you can persist it externally for your own checkpointing logic.
- If you need to filter frames by LSN before transmission, add a middleware on the leader side that compares record `lsn` and forwards only `lsn > follower_lsn` (custom tooling).


## 7) Performance notes

- Group-commit (WAL fsync coalescing) reduces fsync load:
    - `P1_WAL_COALESCE_MS` (default 3 ms) controls the delay before fsync.
- Data fsync policy:
    - `P1_DATA_FSYNC=1` (default): commit pages fsync data segments; WAL may be truncated after commit.
    - `P1_DATA_FSYNC=0`: rely on WAL durability; do not truncate WAL on commit, truncate during replay.
- Because CDC uses full page images, bandwidth roughly equals page_size per updated page (plus headers). Consider compressing the stream:
    - `wal-ship ... | gzip -1 | nc ...`
    - `zstd -q --adapt` is another option.


## 8) Security and deployment tips

- Use TLS tunnels or SSH for WAN transport.
- Consider running wal-apply in a supervised service (systemd, runit, etc.) with restart on failure.
- Monitor with `wal-tail` to verify frame emission on leader side.


## 9) Troubleshooting

- wal-apply stops “early”:
    - Usually a partial tail: the producer hasn’t emitted a full frame yet. This is normal; reconnect or keep the producer running with `--follow`.
- CRC mismatch in wal-apply:
    - Indicates stream corruption in transit or a bug; restart and investigate the transport.
- Unknown record types appear in wal-tail:
    - Forward-compatibility: older clients will print `"type":"unknown"...` and skip them.
- Follower does not reflect updates:
    - Make sure wal-ship is pointed at the correct path.
    - Check that LSN is increasing on the leader (wal-tail).
    - Confirm that the follower can write (it must not be opened in RO mode by another process).


## 10) Wire summary (for implementers)

All integers are Little Endian (LE).

WAL header (16 bytes):
- 0..=7:  WAL_MAGIC = "P1WAL001"
- 8..=11: u32 reserved
- 12..=15: u32 reserved

Record header (28 bytes):
- 0:   u8  type
- 1:   u8  flags
- 2..=3:  u16 reserved
- 4..=11: u64 lsn
- 12..=19: u64 page_id
- 20..=23: u32 len
- 24..=27: u32 crc32  (over header[0..24] + payload[0..len])

Payload:
- PAGE_IMAGE: page bytes
- TRUNCATE: empty

Mid-stream WAL header is allowed and must be skipped by consumers (e.g., after TRUNCATE).