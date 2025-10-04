# Changelog

All notable changes to this project are documented in this file.

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