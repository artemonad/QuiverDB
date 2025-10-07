# quiverdb-follower (v1.3 alpha)

Minimal follower/applier daemon for QuiverDB WAL streaming.

- Applies incoming WAL wire streams with idempotent LSN-gating
- Single-writer safety via a file lock on the follower DB path
- Two modes:
    - Server (listen): follower listens for leader’s TCP connection (recommended)
    - Client (connect): follower connects to a TCP server emitting the WAL stream
- Optional decompression (none | gzip | zstd) to match leader’s wal-ship
- Optional checkpoint file to persist follower’s last LSN after each session
- Configuration via CLI flags and/or TOML (CLI > config > defaults)
- Structured logs (env_logger)

On-disk formats remain frozen across 1.x: meta v3, page v2, WAL v1.

Contents
- Overview
- Build
- Quick start
- Modes: listen (server) and connect (client)
- Configuration (CLI and TOML)
- Compression and compatibility
- Checkpoint and resume
- Examples (leader ↔ follower)
- Logs and observability
- Systemd unit (example)
- Troubleshooting
- Notes and roadmap

---

## Overview

quiverdb-follower consumes a WAL wire stream produced by a QuiverDB leader (e.g., via quiverdb wal-ship) and applies it continuously to a follower database.

- Idempotent apply:
    - v2 pages (KV_RH/OVERFLOW) are applied only when wal_lsn > page_lsn (LSN-gating).
    - Unknown record types are ignored (forward-compatible).
    - Partial tails are treated as normal EOF (session ends cleanly).
- Single-writer safety:
    - The daemon acquires an exclusive file lock on the follower DB path and holds it for the entire lifetime.
- Persistence:
    - Optionally writes the current follower’s last LSN (from meta) into a checkpoint file after each session; useful for resuming leader’s streaming from the right LSN.

---

## Build

From the repo root (workspace):
```bash
cargo build --release -p quiverdb-follower
```

Binary path:
```
target/release/quiverdb-follower
```

---

## Quick start

Follower (server mode, gzip):
```bash
quiverdb-follower --path ./follower_db --listen 0.0.0.0:9999 --decompress gzip --checkpoint-file ./last_lsn
```

Leader (ship with gzip; resume from checkpoint):
```bash
LSN=$(cat ./last_lsn 2>/dev/null || echo 0)
quiverdb wal-ship --path ./leader_db --since-lsn $LSN --sink tcp://<follower-host>:9999 --follow --compress gzip
```

Defaults:
- If neither --listen nor --connect is specified, the follower defaults to server mode listening on 0.0.0.0:9999.
- --decompress defaults to none.

---

## Modes: listen (server) and connect (client)

- Server mode (recommended):
    - Follower listens on a TCP address.
    - Leader connects with wal-ship sink=tcp://<follower-host:port>.
    - Command: quiverdb-follower --listen 0.0.0.0:9999

- Client mode:
    - Follower connects to a TCP server emitting a WAL stream.
    - Note: the built-in wal-ship acts as a client to sinks (not a server); a server/relay is required for client mode.
    - Client mode includes reconnect/backoff logic.
    - Command: quiverdb-follower --connect leader.host:9999

Both modes support optional decompression (must match leader’s compression).

---

## Configuration (CLI and TOML)

CLI flags:
- --path <dir> — follower DB root (exclusive lock is held for the daemon lifetime)
- --listen <addr> — server mode (mutually exclusive with --connect)
- --connect <addr> — client mode (mutually exclusive with --listen)
- --decompress <none|gzip|zstd> — must match leader’s --compress
- --reconnect-ms <ms> — client mode: initial backoff (default 1000)
- --backoff-cap-ms <ms> — client mode: backoff cap (default 15000)
- --checkpoint-file <path> — write follower’s last LSN after each session
- --config <file.toml> — TOML configuration (values are merged; CLI > config > defaults)

TOML example (follower.toml):
```toml
# Follower DB path (can also be passed via --path; CLI overrides config)
# path = "./follower_db"

# Choose exactly one of listen/connect:
listen = "0.0.0.0:9999"
# connect = "leader.host:9999"

# Stream compression (must match leader's wal-ship compress): "none" | "gzip" | "zstd"
decompress = "gzip"

# Client reconnect/backoff (only used when connect is set)
reconnect_ms = 1000
backoff_cap_ms = 15000

# Optional checkpoint file for follower's last LSN
checkpoint_file = "./last_lsn"
```

Config resolution:
- Effective value = CLI flag if present; else config value; else default.

---

## Compression and compatibility

- Supported decompression: none, gzip, zstd.
- Follower’s --decompress must match leader’s wal-ship --compress.
- WAL wire stream details (v1):
    - Initial 16-byte WAL header.
    - Records with CRC; unknown types ignored; partial tails treated as EOF.
    - TRUNCATE record and mid-stream headers are tolerated (rotation friendly).

---

## Checkpoint and resume

If --checkpoint-file is provided, the daemon writes the current follower’s last LSN (meta.last_lsn) after each streaming session. The leader can then resume efficiently:

```bash
LSN=$(cat ./last_lsn 2>/dev/null || echo 0)
quiverdb wal-ship --path ./leader_db --since-lsn $LSN --sink tcp://<follower-host>:9999 --follow
```

This avoids re-sending already applied frames and reduces bandwidth.

---

## Examples (leader ↔ follower)

Server mode (gzip):
```bash
# follower
quiverdb-follower --path ./follower_db --listen 0.0.0.0:9999 --decompress gzip --checkpoint-file ./last_lsn
# leader
LSN=$(cat ./last_lsn 2>/dev/null || echo 0)
quiverdb wal-ship --path ./leader_db --since-lsn $LSN --sink tcp://<follower-host>:9999 --follow --compress gzip
```

No compression:
```bash
# follower
quiverdb-follower --path ./follower_db --listen 0.0.0.0:9999 --decompress none
# leader
quiverdb wal-ship --path ./leader_db --since-lsn 0 --sink tcp://<follower-host>:9999 --follow
```

Client mode (follower connects):
```bash
quiverdb-follower --path ./follower_db --connect leader.host:9999 --decompress zstd
```
Requires a TCP server on the leader side (a relay or custom emitter) serving the WAL wire format.

---

## Logs and observability

- Logs: env_logger (set RUST_LOG to info/debug):
```bash
RUST_LOG=info quiverdb-follower --path ./follower_db --listen 0.0.0.0:9999
```

- Typical info logs:
    - “listening on …”, “accepted from …”, “connected to …”
    - “checkpoint last_lsn=…” when checkpoint file is used
    - Warnings on session errors and reconnects

Prometheus export or additional observability can be layered in future versions.

---

## Systemd unit (example)

`/etc/systemd/system/quiverdb-follower.service`:
```ini
[Unit]
Description=QuiverDB follower (WAL apply daemon)
Wants=network-online.target
After=network-online.target

[Service]
Type=simple
WorkingDirectory=/var/lib/quiverdb-follower
ExecStart=/usr/local/bin/quiverdb-follower --config /etc/quiverdb/follower.toml --path /var/lib/quiverdb-follower
Restart=always
RestartSec=2s
Environment=RUST_LOG=info

# Optional hardening (adjust to your environment)
NoNewPrivileges=true
ProtectSystem=full
ProtectHome=true
PrivateTmp=true

[Install]
WantedBy=multi-user.target
```

`/etc/quiverdb/follower.toml`:
```toml
path = "/var/lib/quiverdb-follower"
listen = "0.0.0.0:9999"
decompress = "zstd"
checkpoint_file = "/var/lib/quiverdb-follower/last_lsn"
```

---

## Troubleshooting

- Connection issues:
    - Server mode: ensure follower is reachable and leader uses sink=tcp://host:port.
    - Client mode: verify follower can reach the server and that the server emits a valid WAL stream.
    - Use logs (RUST_LOG=debug) to see connect/retry details.

- Decompression mismatch:
    - Follower’s --decompress must equal leader’s --compress (none|gzip|zstd).

- WAL not progressing:
    - Confirm leader’s --path points to the correct DB.
    - Check follower DB lock (no other writer should hold it).
    - Verify follower’s checkpoint file updates (if used) and meta.last_lsn progresses.

- Lock errors:
    - Only one writer (the follower daemon) can open the follower DB. Stop other writers or readers holding exclusive locks.

- Disk safety:
    - The follower relies on WAL semantics; the leader should keep data_fsync=true for strict durability guarantees on the source.

---

## Notes and roadmap

- v1.3 alpha focuses on a robust streaming apply loop with reconnect/backoff and a clear operational model (server mode recommended).
- Planned (incremental, non-breaking):
    - TLS (rustls) and simple token authentication for secure transport.
    - Metrics endpoint (Prometheus) for follower-specific stats.
    - Filters (prefix/bucket) for partial replication (experimental).
    - Streaming backup/restore (tar+zstd) as a first-class CLI alternative to directory backups.

QuiverDB disk formats remain frozen across the 1.x series (meta v3, page v2, WAL v1).