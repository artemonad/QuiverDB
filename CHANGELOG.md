# Changelog

All notable changes to this project are documented in this file.

## [1.1.0] – 2025-10-05

Formats: unchanged (meta v3, page v2, WAL v1 remain frozen)

Added
- Configuration and builder
  - QuiverConfig and DbBuilder with explicit open_with_config/open_ro_with_config
  - Tunables: wal_coalesce_ms, data_fsync, page_cache_pages, ovf_threshold_bytes
  - Builder precedence over environment variables (env still supported)
- In‑process subscriptions
  - Db::subscribe_prefix(prefix, callback) → SubscriptionHandle (Drop = unsubscribe)
  - Event { key, value: Option<Vec<u8>>, lsn }
  - Synchronous callbacks invoked after successful commit (put/update → Some, delete → None)
- Scans
  - Db::scan_all() and Db::scan_prefix(prefix) with tail‑wins semantics
  - CLI: dbscan [--prefix X] [--json]
- CDC tooling
  - wal-ship: --since-lsn N (send frames with lsn > N), --sink=tcp://host:port (built-in TCP sink)
  - cdc last-lsn: print follower’s last LSN (resume helper)
  - cdc record: save deterministic wire-format slices with optional --from-lsn/--to-lsn
  - cdc replay: apply deterministic slices (or stdin) with optional LSN filters
- Metrics CLI
  - metrics --json and metrics-reset

Changed
- WAL writer group-commit: shared registry coalesces fsync across appenders in the same process
- Db stats print includes a metrics snapshot for convenience

Hardening
- CDC/wal-apply remains tolerant to partial tails, ignores unknown record types (forward-compatible)
- LSN-gating on v2 pages prevents applying older frames over newer pages
- Directory updates remain atomic (tmp+rename) with CRC validated on open
- Repair frees conservative orphan overflow chains

Compatibility
- No on-disk format changes; backward/forward compatibility preserved
- Existing databases from 1.0.0 work without migration

Docs
- Updated API guide (v1.1.0): config/builder, scans, subscriptions, CLI mapping
- Updated CDC guide: TCP sink, since-lsn, record/replay, last-lsn
- README refreshed with 1.1 highlights and examples

Upgrade notes
- If you use env-only configuration, behavior remains the same
- To switch to explicit config, use DbBuilder and open_with_config/open_ro_with_config
- Subscription callbacks run synchronously in the writer thread; keep handlers fast

## [1.0.0] – 2025-10-05

- Freeze on-disk formats: meta v3, page v2 (RH/Overflow), WAL v1.
- Reliability:
  - WAL replay: CRC verification, LSN-gating for v2 pages, tolerant to partial tails, ignores unknown record types (forward-compatible).
  - Directory: atomic updates (tmp+rename), CRC in header.
- Operations:
  - Read-only readers (`Db::open_ro`): shared lock, no replay, no meta changes.
  - `check/repair`: CRC/IO scan, overflow orphan reporting, `--strict` mode for CI; conservative orphan freeing.
  - Metrics + CLI commands (`metrics`, `metrics-reset`).
  - WAL group-commit (fsync coalescing), `P1_DATA_FSYNC` option, page cache (optional).
- CDC:
  - `wal-tail` (JSONL), `wal-ship` (binary), `wal-apply` (idempotent apply with LSN gating).

## [0.9] – Release preparations

- `check/repair`, strict mode, metrics, additional integration tests.

## [0.8] – Crash-consistency polishing

- Directory: `tmp+rename`, CRC.
- Fault-injection scenarios, faster cold start.

## [0.7] – Multi-process readers

- `Db::open_ro` (shared lock), no replay/meta changes.

## [0.6] – Space lifecycle

- Free-list, overflow chains, orphan sweeping.

## [0.5] – CDC utilities

- `wal-tail`, `wal-ship`, `wal-apply` (partial-tail tolerance, mid-stream header handling).

## [0.4] – Performance without complexity

- WAL group-commit, simple page cache, basic metrics.

## [0.3] – RH-only and base reliability

- v2 pages (Robin Hood), meta v3, WAL replay with LSN-gating.