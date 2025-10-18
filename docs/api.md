# QuiverDB 2.2 — API Guide (Addendum)

This addendum documents the 2.2 changes. Base semantics from 2.0/2.1 remain valid.

Scope
- On‑disk/wire formats unchanged: meta v4, page v3 (KV_RH3/OVERFLOW3), directory v2, WAL v2.
- New features are additive and backward‑compatible at the API level.

Contents
- 1) SnapStore location override (P1_SNAPSTORE_DIR)
- 2) Snapshot lifecycle (create/list/inspect/delete/restore)
- 3) CDC apply hardening (HELLO/seq/HEADS_UPDATE)
- 4) Status JSON (serde_json)
- 5) CLI recap
- 6) Compatibility and migration

---

## 1) SnapStore location override (P1_SNAPSTORE_DIR)

By default, SnapStore lives in <db_root>/.snapstore. In 2.2, you can override this path:

- Absolute path — used as‑is:
  - P1_SNAPSTORE_DIR=/mnt/snapstore → /mnt/snapstore/{objects,refs,manifests}
- Relative path — resolved against DB root:
  - P1_SNAPSTORE_DIR=.cache/snapstore → <db_root>/.cache/snapstore/{objects,refs,manifests}
- Unset or empty — falls back to <db_root>/.snapstore

A single resolver is used consistently by SnapStore and the manifest I/O. No API changes are required in your code: the existing calls will honor the new location.

Rust API
- SnapStore::open_or_create(root): picks up P1_SNAPSTORE_DIR.
- Manifests are written/read in <snapstore_dir>/manifests.

Examples
```bash
# Absolute override
P1_SNAPSTORE_DIR=/mnt/snapstore quiverdb snapshot-create --path ./db

# Relative (to DB root)
P1_SNAPSTORE_DIR=.cache/snapstore quiverdb snapshot-list --path ./db
```

---

## 2) Snapshot lifecycle (create/list/inspect/delete/restore)

The persisted snapshot flow is:

- Create a snapshot: freeze all “live” pages to the SnapStore (content‑addressed by SHA‑256) and write a manifest v2 (JSON) under <snapstore_dir>/manifests/<id>.json.
- Delete a snapshot: dec‑ref all objects referenced by the manifest (objects are removed when refcount reaches 0), then remove the manifest.
- Restore a DB: read a manifest, copy referenced pages back into the target DB, install directory heads, set last_lsn/next_page_id and clean_shutdown=true, and truncate WAL to header.

Rust API
- Create: SnapshotManager::create_persisted(&db_ro, message, &labels, parent) -> id
- Delete: SnapshotManager::delete_persisted(root, id) -> Result<()>
- Restore: snapstore::restore_from_id(src_root, dst_root, id, verify) -> Result<()>
  - verify=true performs a page_size check on restored pages.

CLI
- quiverdb snapshot-create --path ./db --message "baseline" --label prod --label v2
- quiverdb snapshot-list --path ./db [--json]
- quiverdb snapshot-inspect --path ./db --id <snapshot_id> [--json]
- quiverdb snapshot-restore --path ./dst --src ./db --id <snapshot_id> [--verify]
- quiverdb snapshot-delete --path ./db --id <snapshot_id>

Manifest v2 (summary)
- meta: version=2, id, parent, created_unix_ms, message, labels, lsn, page_size, next_page_id, buckets, hash_kind, codec_default.
- heads: pairs (bucket u32, head_pid u64).
- objects: list (page_id u64, hash_hex String, bytes u64).
- Stored at <snapstore_dir>/manifests/<id>.json.

Notes
- SnapStore stores objects under <snapstore_dir>/objects/<hh>/<rest>, with refcount in <snapstore_dir>/refs/<hash>.ref (LE u64).
- delete_persisted only drops objects whose refcount reaches 0; other snapshots referencing them remain intact.

---

## 3) CDC apply hardening (HELLO/seq/HEADS_UPDATE)

2.2 refines the default behavior of CDC apply (PSK streams) and adds strict toggles:

- PSK streams require HELLO (WAL file header “P2WAL001” + stream_id) as the first frame.
  - If HELLO is missing, apply fails by default.
  - Dev fallback: P1_CDC_ALLOW_NO_HELLO=1 — accept streams without HELLO (logs a warning and disables anti‑mix), not recommended for production.

- Strict monotonic seq for framed PSK streams:
  - P1_CDC_SEQ_STRICT=1 — any seq regression (seq <= last_seq) is an error.
  - By default (strict off), apply logs a warning and skips the regressed frame.

- HEADS_UPDATE payload validation:
  - Payload must be len > 0 and multiple of 12 bytes ([bucket u32][head_pid u64] tuples).
  - P1_CDC_HEADS_STRICT=1 — invalid payload length is an error.
  - By default (strict off), apply logs a warning and skips the invalid update.

- Anti‑mix stream source:
  - For both file and PSK sources, stream_id is validated/stored via a single verify_and_store_stream_id helper.
  - On mismatch, apply fails.

Wire format (unchanged)
- WAL v2 (P2WAL001): CRC32C, record types 1=BEGIN, 2=PAGE_IMAGE, 4=COMMIT, 5=TRUNCATE, 6=HEADS_UPDATE; 3=DELTA reserved.
- Consumers ignore unknown types and treat partial tails as EOF.
- Reader tolerates a mid‑stream WAL header only immediately after a TRUNCATE record.

---

## 4) Status JSON (serde_json)

The status command’s JSON branch is implemented with serde_json, not manual string concatenation. It prints a single JSON object with sections:
- meta, tde (with epochs), acceleration (mem_keydir), bloom (with freshness and cache stats), directory, and metrics (WAL, page cache, keydir, overflow, snapshots/backup/restore, TTL, Bloom, packing, lazy compaction).

CLI
```bash
quiverdb status --path ./db --json
```

---

## 5) CLI recap

Core
- init, put, get, del, batch, scan, status, doctor, sweep, compact, vacuum, bloom, checkpoint
  CDC
- cdc-ship, cdc-apply
  Snapshots (2.2)
- snapshot-create, snapshot-list, snapshot-inspect, snapshot-restore, snapshot-delete

---

## 6) Compatibility and migration

- No on‑disk or wire format changes in 2.2.
- Upgrading from 2.1 requires no data migration.
- If you want stricter CDC behavior:
  - Require HELLO for PSK (default in 2.2).
  - Set P1_CDC_SEQ_STRICT=1 and/or P1_CDC_HEADS_STRICT=1.
- If you want to place snapshots outside DB root:
  - Set P1_SNAPSTORE_DIR to an absolute or relative path before using snapshot CLI/API.

Environment toggles (2.2 additions)
- P1_SNAPSTORE_DIR — SnapStore directory (absolute or relative to DB root).
- P1_CDC_SEQ_STRICT=1 — strict PSK seq monotonicity.
- P1_CDC_HEADS_STRICT=1 — HEADS_UPDATE payload length validation as error.
- P1_CDC_ALLOW_NO_HELLO=1 — allow PSK apply without initial HELLO (development only).