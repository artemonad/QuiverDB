//! Snapshot manager: begin/end, COW/freeze_if_needed, SnapStore integration (Phase 2).
//!
//! Constructors:
//! - SnapshotManager::new(root): reads env P1_SNAP_PERSIST / P1_SNAP_DEDUP (backward-compatible).
//! - SnapshotManager::new_with_flags(root, snap_persist, snap_dedup): explicit flags.
//! - SnapshotManager::new_with_options(root, snap_persist, snap_dedup, snapstore_dir):
//!     preferred from config; optionally sets P1_SNAPSTORE_DIR for SnapStore.

use anyhow::{anyhow, Context, Result};
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::db::Db;
use crate::meta::read_meta;
use crate::metrics::{record_snapshot_begin, record_snapshot_end, record_snapshot_freeze_frame};

// local sidecar helpers
use super::io::{write_freeze_frame_to_dir, append_hash_index_entry};
use super::registry::{registry_add, registry_mark_ended};
use super::store::SnapStore;
use super::handle::SnapshotHandle;

// ENV helper
fn env_bool(name: &str) -> bool {
    std::env::var(name)
        .ok()
        .map(|s| s.trim().to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

// State of a single active snapshot
#[derive(Debug)]
struct SnapshotState {
    pub id: String,
    pub lsn: u64,
    pub freeze_dir: PathBuf,        // <root>/.snapshots/<id>/
    pub frozen_pages: HashSet<u64>, // pages already frozen for this snapshot
    pub ended: bool,
    // in-memory index (page_id -> offset) for freeze.bin (best-effort)
    pub index_offsets: HashMap<u64, u64>,
}

/// In-process snapshot manager.
/// Phase 1: sidecar per snapshot; Phase 2: optional persisted registry + SnapStore (dedup).
pub struct SnapshotManager {
    root: PathBuf,
    next_ctr: u64,
    active: HashMap<String, SnapshotState>,
    pub max_snapshot_lsn: u64,

    // Phase 2 options
    snapstore: Option<SnapStore>,
    snap_persist: bool,           // keep sidecar on end/Drop and track registry
    snap_dedup: bool,             // write frames into SnapStore (best-effort)
    snapstore_dir: Option<String> // optional custom SnapStore directory (mirrors P1_SNAPSTORE_DIR)
}

impl SnapshotManager {
    /// Backward-compatible constructor: reads flags from ENV.
    /// - P1_SNAP_PERSIST=0|1
    /// - P1_SNAP_DEDUP=0|1
    pub fn new(root: &Path) -> Self {
        let persist = env_bool("P1_SNAP_PERSIST");
        let dedup = env_bool("P1_SNAP_DEDUP");
        Self::new_with_options(root, persist, dedup, None)
    }

    /// Constructor with explicit flags (wrapper).
    pub fn new_with_flags(root: &Path, snap_persist: bool, snap_dedup: bool) -> Self {
        Self::new_with_options(root, snap_persist, snap_dedup, None)
    }

    /// Preferred constructor: explicit flags and optional snapstore_dir.
    /// If snapstore_dir is Some(..), sets P1_SNAPSTORE_DIR to ensure SnapStore uses the same directory
    /// across all code paths (manager/handle).
    pub fn new_with_options(
        root: &Path,
        snap_persist: bool,
        snap_dedup: bool,
        snapstore_dir: Option<String>,
    ) -> Self {
        if let Some(dir) = &snapstore_dir {
            let d = dir.trim();
            if !d.is_empty() {
                // Set env for consistency across process (SnapStore::open reads env)
                std::env::set_var("P1_SNAPSTORE_DIR", d);
            }
        }
        Self {
            root: root.to_path_buf(),
            next_ctr: 1,
            active: HashMap::new(),
            max_snapshot_lsn: 0,
            snapstore: None,
            snap_persist,
            snap_dedup,
            snapstore_dir,
        }
    }

    /// Lazy open SnapStore (best-effort).
    fn ensure_store(&mut self) -> Option<&mut SnapStore> {
        if self.snapstore.is_none() {
            let ps = match read_meta(&self.root) {
                Ok(m) => m.page_size,
                Err(_) => return None,
            };
            match SnapStore::open(&self.root, ps) {
                Ok(ss) => self.snapstore = Some(ss),
                Err(_) => return None,
            }
        }
        self.snapstore.as_mut()
    }

    /// Begin a snapshot at current last_lsn.
    pub fn begin(&mut self, db: &Db) -> Result<SnapshotHandle> {
        let lsn = db.pager.meta.last_lsn;

        let pid = std::process::id();
        let ts = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos();
        let ctr = self.next_ctr;
        self.next_ctr = self.next_ctr.wrapping_add(1);
        let id = format!("{}-{}-{}", pid, ts, ctr);

        // Create sidecar directory
        let freeze_dir = self.root.join(".snapshots").join(&id);
        fs::create_dir_all(&freeze_dir)
            .with_context(|| format!("create snapshot dir {}", freeze_dir.display()))?;

        // Register
        self.active.insert(
            id.clone(),
            SnapshotState {
                id: id.clone(),
                lsn,
                freeze_dir: freeze_dir.clone(),
                frozen_pages: HashSet::new(),
                ended: false,
                index_offsets: HashMap::new(),
            },
        );
        self.recompute_max_lsn();

        // Persisted mode: add to registry (best-effort)
        if self.snap_persist {
            let _ = registry_add(&self.root, &id, lsn);
        }

        record_snapshot_begin();

        let keep_sidecar = self.snap_persist;
        Ok(SnapshotHandle::new(
            self.root.clone(),
            id,
            lsn,
            freeze_dir,
            keep_sidecar,
        ))
    }

    /// End a snapshot; persisted mode: keep sidecar and mark ended in registry.
    pub fn end(&mut self, id: &str) -> Result<()> {
        if let Some(st) = self.active.get_mut(id) {
            if !st.ended {
                st.ended = true;
                if self.snap_persist {
                    let _ = registry_mark_ended(&self.root, id);
                } else {
                    let _ = fs::remove_dir_all(&st.freeze_dir);
                }
            }
            self.active.remove(id);
            self.recompute_max_lsn();

            record_snapshot_end();
            Ok(())
        } else {
            Err(anyhow!("snapshot '{}' is not active", id))
        }
    }

    /// Recompute max snapshot LSN across active snapshots.
    fn recompute_max_lsn(&mut self) {
        self.max_snapshot_lsn = self.active.values().map(|s| s.lsn).max().unwrap_or(0);
    }

    /// Mark page frozen for a snapshot.
    pub fn mark_frozen(&mut self, id: &str, page_id: u64) {
        if let Some(st) = self.active.get_mut(id) {
            st.frozen_pages.insert(page_id);
        }
    }

    /// Check if page was frozen for a snapshot.
    pub fn is_frozen(&self, id: &str, page_id: u64) -> bool {
        self.active
            .get(id)
            .map(|s| s.frozen_pages.contains(&page_id))
            .unwrap_or(false)
    }

    /// Freeze current page image for all snapshots that might need it (snapshot_lsn ≥ page_lsn).
    /// Writes sidecar frames; if dedup is enabled — also puts to SnapStore and appends hashindex.
    pub fn freeze_if_needed(
        &mut self,
        page_id: u64,
        page_lsn: u64,
        page_bytes: &[u8],
    ) -> Result<()> {
        if self.max_snapshot_lsn < page_lsn {
            return Ok(());
        }

        let mut to_freeze: Vec<String> = Vec::new();
        for (id, st) in self.active.iter() {
            if st.ended {
                continue;
            }
            if st.lsn >= page_lsn && !st.frozen_pages.contains(&page_id) {
                to_freeze.push(id.clone());
            }
        }

        if to_freeze.is_empty() {
            return Ok(());
        }

        // Write sidecar frames
        let mut froze_dirs: Vec<PathBuf> = Vec::new();

        for id in to_freeze {
            if let Some(st) = self.active.get_mut(&id) {
                let offset = write_freeze_frame_to_dir(&st.freeze_dir, page_id, page_lsn, page_bytes)
                    .with_context(|| format!("write freeze frame for snapshot {}", st.id))?;
                record_snapshot_freeze_frame(page_bytes.len());
                st.index_offsets.insert(page_id, offset);
                st.frozen_pages.insert(page_id);
                froze_dirs.push(st.freeze_dir.clone());
            }
        }

        // Phase 2: dedup in SnapStore (best-effort).
        // Important: if content is frozen for multiple snapshots, increase refcount accordingly:
        // ss.put(page_bytes) -> +1, then for remaining sidecars ss.add_ref(hash).
        if self.snap_dedup && !froze_dirs.is_empty() {
            if let Some(ss) = self.ensure_store() {
                if let Ok((hash, _off)) = ss.put(page_bytes) {
                    let extra_refs = froze_dirs.len().saturating_sub(1);
                    for _ in 0..extra_refs {
                        let _ = ss.add_ref(hash);
                    }
                    for dir in froze_dirs {
                        let _ = append_hash_index_entry(&dir, page_id, hash, page_lsn);
                    }
                }
            }
        }

        Ok(())
    }
}