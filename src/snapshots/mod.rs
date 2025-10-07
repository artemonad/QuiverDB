//! Snapshots module split into submodules:
//! - manager.rs: SnapshotManager (begin/end, COW/freeze_if_needed, SnapStore integration).
//! - handle.rs: SnapshotHandle (get/scan, page/overflow resolution, fallback).
//! - io.rs: freeze/index I/O helpers (write_freeze_frame_to_dir, build_freeze_index, read_frozen_page_at_offset).
//! - registry.rs: persisted snapshots registry (.snapshots/registry.json) — best-effort.
//! - store.rs: content-addressed snapstore (Phase 2).
//!
//! External API surface (unchanged):
//! - SnapshotManager
//! - SnapshotHandle
//! - snapshots_root()

pub mod store;

mod registry;
mod io;
mod manager;
mod handle;

pub use manager::SnapshotManager;
pub use handle::{SnapshotHandle, snapshots_root};

