use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::{Path, PathBuf};

use crate::consts::{NO_PAGE, PAGE_HDR_V2_SIZE, PAGE_MAGIC};
use crate::dir::Directory;
use crate::meta::read_meta;
use crate::metrics::record_snapshot_fallback_scan;
use crate::pager::Pager;
use crate::page_ovf::ovf_header_read;
use crate::page_rh::{rh_header_read, rh_kv_list, rh_kv_lookup, rh_page_is_kv};

// I/O helpers for sidecar
use super::io::{build_freeze_index, read_frozen_page_at_offset};
use super::io::build_hash_index;
// SnapStore (Phase 2)
use super::store::SnapStore;

/// Snapshot handle (Phase 1/2): read-only view "as of LSN".
pub struct SnapshotHandle {
    root: PathBuf,
    pub id: String,
    pub lsn: u64,
    pub(crate) freeze_dir: PathBuf,
    ended: bool,
    // Lazy cache of index.bin: page_id -> offset in freeze.bin
    index_cache: RefCell<Option<HashMap<u64, u64>>>,
    // Phase 2: keep sidecar on end/drop (persisted snapshots)
    keep_sidecar: bool,

    // Phase 2: lazy snapstore and hash-index cache
    store: RefCell<Option<SnapStore>>,
    hash_idx_cache: RefCell<Option<HashMap<u64, u64>>>, // page_id -> hash
}

impl SnapshotHandle {
    /// Internal constructor (called by SnapshotManager).
    pub(crate) fn new(
        root: PathBuf,
        id: String,
        lsn: u64,
        freeze_dir: PathBuf,
        keep_sidecar: bool,
    ) -> Self {
        Self {
            root,
            id,
            lsn,
            freeze_dir,
            ended: false,
            index_cache: RefCell::new(None),
            keep_sidecar,
            store: RefCell::new(None),
            hash_idx_cache: RefCell::new(None),
        }
    }

    /// Snapshot LSN.
    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    /// Get a value at snapshot LSN (consistent view).
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let dir = Directory::open(&self.root)?;
        let pager = Pager::open(&self.root)?;

        let bucket = dir.bucket_of_key(key);
        let mut pid = dir.head(bucket)?;
        if pid == NO_PAGE {
            return self.fallback_get_by_scan(&pager, key);
        }

        let ps = pager.meta.page_size as usize;
        let mut best: Option<Vec<u8>> = None;

        while pid != NO_PAGE {
            let page = match self.page_bytes_at_snapshot(&pager, pid, ps)? {
                Some(b) => b,
                None => break,
            };

            if !rh_page_is_kv(&page) {
                break;
            }
            let h = rh_header_read(&page)?;
            if let Some(v) = rh_kv_lookup(&page, dir.hash_kind, key)? {
                let real = if v.len() == 18 && v[0] == 0xFF {
                    let total_len = LittleEndian::read_u64(&v[2..10]) as usize;
                    let head_pid = LittleEndian::read_u64(&v[10..18]);
                    self.read_overflow_chain_at_snapshot(&pager, head_pid, ps, total_len)?
                } else {
                    v
                };
                best = Some(real);
            }
            pid = h.next_page_id;
        }

        if best.is_none() {
            return self.fallback_get_by_scan(&pager, key);
        }
        Ok(best)
    }

    /// Scan entire DB at snapshot LSN.
    pub fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix_impl(None)
    }

    /// Scan by prefix at snapshot LSN.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix_impl(Some(prefix))
    }

    /// Finish snapshot. Phase 2: keep sidecar when keep_sidecar=true.
    pub fn end(&mut self) -> Result<()> {
        if self.ended {
            return Ok(());
        }
        if !self.keep_sidecar {
            let _ = fs::remove_dir_all(&self.freeze_dir);
        }
        self.ended = true;
        Ok(())
    }

    // ----------------- internal helpers -----------------

    fn scan_prefix_impl(&self, prefix: Option<&[u8]>) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let dir = Directory::open(&self.root)?;
        let pager = Pager::open(&self.root)?;
        let ps = pager.meta.page_size as usize;

        let mut out_map: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        for b in 0..dir.bucket_count {
            let mut pid = dir.head(b)?;
            while pid != NO_PAGE {
                let page = match self.page_bytes_at_snapshot(&pager, pid, ps)? {
                    Some(b) => b,
                    None => break,
                };
                if !rh_page_is_kv(&page) {
                    break;
                }
                let h = rh_header_read(&page)?;

                if let Ok(items) = rh_kv_list(&page) {
                    for (k, v) in items {
                        if let Some(pref) = prefix {
                            if !k.starts_with(pref) {
                                continue;
                            }
                        }
                        let value = if v.len() == 18 && v[0] == 0xFF {
                            let total_len = LittleEndian::read_u64(&v[2..10]) as usize;
                            let head_pid = LittleEndian::read_u64(&v[10..18]);
                            self.read_overflow_chain_at_snapshot(&pager, head_pid, ps, total_len)?
                        } else {
                            v.to_vec()
                        };
                        // tail-wins: запись ближе к хвосту перезапишет более ранние
                        out_map.insert(k.to_vec(), value);
                    }
                }

                pid = h.next_page_id;
            }
        }

        let mut out = Vec::with_capacity(out_map.len());
        for (k, v) in out_map {
            out.push((k, v));
        }
        Ok(out)
    }

    /// Pick page bytes as-of snapshot LSN (live vs frozen).
    fn page_bytes_at_snapshot(
        &self,
        pager: &Pager,
        page_id: u64,
        page_size: usize,
    ) -> Result<Option<Vec<u8>>> {
        let mut buf = vec![0u8; page_size];
        match pager.read_page(page_id, &mut buf) {
            Ok(()) => {
                // v2?
                let is_v2 = buf.len() >= 8
                    && &buf[..4] == PAGE_MAGIC
                    && LittleEndian::read_u16(&buf[4..6]) >= 2;

                // RH page
                if let Ok(h) = rh_header_read(&buf) {
                    if h.lsn <= self.lsn {
                        return Ok(Some(buf));
                    }
                    if let Some(bytes) =
                        self.read_frozen_page_cached(page_id, page_size, false)?
                    {
                        return Ok(Some(bytes));
                    }
                    if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, true)? {
                        return Ok(Some(bytes));
                    }
                    // Phase 2 fallback: try SnapStore by hash
                    if let Some(bytes) = self.read_from_snapstore_for_page(page_id, page_size)? {
                        return Ok(Some(bytes));
                    }
                    return Ok(None);
                }
                // OVERFLOW page
                if let Ok(hovf) = ovf_header_read(&buf) {
                    if hovf.lsn <= self.lsn {
                        return Ok(Some(buf));
                    }
                    if let Some(bytes) =
                        self.read_frozen_page_cached(page_id, page_size, false)?
                    {
                        return Ok(Some(bytes));
                    }
                    if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, true)? {
                        return Ok(Some(bytes));
                    }
                    // Phase 2 fallback: try SnapStore by hash
                    if let Some(bytes) = self.read_from_snapstore_for_page(page_id, page_size)? {
                        return Ok(Some(bytes));
                    }
                    return Ok(None);
                }

                // RAW FALLBACK for unrecognized v2 page
                if is_v2 {
                    if let Some(bytes) =
                        self.read_frozen_page_cached(page_id, page_size, false)?
                    {
                        return Ok(Some(bytes));
                    }
                    if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, true)? {
                        return Ok(Some(bytes));
                    }
                    // Also try SnapStore if sidecar had no entry
                    if let Some(bytes) = self.read_from_snapstore_for_page(page_id, page_size)? {
                        return Ok(Some(bytes));
                    }
                    return Ok(Some(buf)); // best-effort live
                }

                // Non-v2 — use live
                Ok(Some(buf))
            }
            Err(_) => {
                if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, false)? {
                    return Ok(Some(bytes));
                }
                if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, true)? {
                    return Ok(Some(bytes));
                }
                // Phase 2 fallback: try SnapStore by hash
                if let Some(bytes) = self.read_from_snapstore_for_page(page_id, page_size)? {
                    return Ok(Some(bytes));
                }
                Ok(None)
            }
        }
    }

    /// Read overflow chain as-of snapshot LSN.
    fn read_overflow_chain_at_snapshot(
        &self,
        pager: &Pager,
        mut pid: u64,
        page_size: usize,
        expected_len: usize,
    ) -> Result<Vec<u8>> {
        let mut out = Vec::with_capacity(expected_len);
        let mut safety = 0usize;

        while pid != NO_PAGE {
            safety += 1;
            if safety > 1_000_000 {
                return Err(anyhow!("overflow chain too long or loop detected"));
            }

            let page = match self.page_bytes_at_snapshot(pager, pid, page_size)? {
                Some(b) => b,
                None => break,
            };

            let h = ovf_header_read(&page)
                .with_context(|| format!("expected overflow page at pid={}", pid))?;
            let take = h.chunk_len as usize;
            if PAGE_HDR_V2_SIZE + take > page.len() {
                return Err(anyhow!(
                    "overflow chunk_len={} too big for page (pid={})",
                    take,
                    pid
                ));
            }
            out.extend_from_slice(&page[PAGE_HDR_V2_SIZE..PAGE_HDR_V2_SIZE + take]);
            pid = h.next_page_id;
        }

        if out.len() != expected_len {
            return Err(anyhow!(
                "snapshot overflow length mismatch: got {}, expected {}",
                out.len(),
                expected_len
            ));
        }
        Ok(out)
    }

    /// Read frozen page from sidecar with lazy index cache (refresh optionally).
    fn read_frozen_page_cached(
        &self,
        page_id: u64,
        page_size: usize,
        refresh_index: bool,
    ) -> Result<Option<Vec<u8>>> {
        if refresh_index {
            let idx = build_freeze_index(&self.freeze_dir)?;
            *self.index_cache.borrow_mut() = Some(idx);
        } else if self.index_cache.borrow().is_none() {
            let idx = build_freeze_index(&self.freeze_dir)?;
            *self.index_cache.borrow_mut() = Some(idx);
        }

        let idx_ref = self.index_cache.borrow();
        let idx = idx_ref.as_ref().unwrap();

        let off = match idx.get(&page_id) {
            Some(&o) => o,
            None => return Ok(None),
        };
        read_frozen_page_at_offset(&self.freeze_dir, off, page_id, page_size)
    }

    /// Slow fallback: full scan as-of snapshot LSN (tail-wins reconstruction).
    fn fallback_get_by_scan(&self, pager: &Pager, key: &[u8]) -> Result<Option<Vec<u8>>> {
        record_snapshot_fallback_scan();

        let ps = pager.meta.page_size as usize;
        let total = pager.meta.next_page_id;

        let dir = Directory::open(&self.root)?;
        let hk = dir.hash_kind;

        let mut contains: HashSet<u64> = HashSet::new();
        let mut next_map: HashMap<u64, u64> = HashMap::new();
        let mut raw_map: HashMap<u64, Vec<u8>> = HashMap::new();

        for pid in 0..total {
            let page = match self.page_bytes_at_snapshot(pager, pid, ps)? {
                Some(b) => b,
                None => continue,
            };
            if !rh_page_is_kv(&page) {
                continue;
            }
            let h = match rh_header_read(&page) {
                Ok(hh) => hh,
                Err(_) => continue,
            };
            next_map.insert(pid, h.next_page_id);

            if let Some(v) = rh_kv_lookup(&page, hk, key)? {
                contains.insert(pid);
                raw_map.insert(pid, v.to_vec());
            }
        }

        if contains.is_empty() {
            return Ok(None);
        }

        fn tail_from(start: u64, contains: &HashSet<u64>, next_map: &HashMap<u64, u64>) -> u64 {
            let mut tail = start;
            let mut cur = *next_map.get(&start).unwrap_or(&NO_PAGE);
            let mut guard = 0usize;
            while cur != NO_PAGE && guard < 1_000_000 {
                if contains.contains(&cur) {
                    tail = cur;
                }
                cur = *next_map.get(&cur).unwrap_or(&NO_PAGE);
                guard += 1;
            }
            tail
        }

        let mut final_tail: Option<u64> = None;
        for &c in &contains {
            let t = tail_from(c, &contains, &next_map);
            final_tail = match final_tail {
                None => Some(t),
                Some(prev) => {
                    if prev == t {
                        Some(prev)
                    } else {
                        let mut cur = *next_map.get(&prev).unwrap_or(&NO_PAGE);
                        let mut reaches = false;
                        let mut guard = 0usize;
                        while cur != NO_PAGE && guard < 1_000_000 {
                            if cur == t {
                                reaches = true;
                                break;
                            }
                            cur = *next_map.get(&cur).unwrap_or(&NO_PAGE);
                            guard += 1;
                        }
                        if reaches { Some(t) } else { Some(prev) }
                    }
                }
            };
        }

        let tail_pid = match final_tail {
            Some(p) => p,
            None => return Ok(None),
        };

        if let Some(raw) = raw_map.get(&tail_pid) {
            if raw.len() == 18 && raw[0] == 0xFF {
                let total_len = LittleEndian::read_u64(&raw[2..10]) as usize;
                let head_pid = LittleEndian::read_u64(&raw[10..18]);
                let v = self.read_overflow_chain_at_snapshot(pager, head_pid, ps, total_len)?;
                Ok(Some(v))
            } else {
                Ok(Some(raw.clone()))
            }
        } else {
            Ok(None)
        }
    }

    // ----------------- Phase 2: SnapStore fallback helpers -----------------

    fn read_from_snapstore_for_page(
        &self,
        page_id: u64,
        page_size: usize,
    ) -> Result<Option<Vec<u8>>> {
        // Build/load hash index lazily
        if self.hash_idx_cache.borrow().is_none() {
            let idx = build_hash_index(&self.freeze_dir)?;
            *self.hash_idx_cache.borrow_mut() = Some(idx);
        }
        let hash_idx_ref = self.hash_idx_cache.borrow();
        let hash_idx = hash_idx_ref.as_ref().unwrap();

        let hash = match hash_idx.get(&page_id) {
            Some(&h) => h,
            None => return Ok(None),
        };

        // Ensure/open SnapStore and read by hash
        match self.read_from_snapstore_by_hash(hash)? {
            Some(mut payload) => {
                if payload.len() != page_size {
                    // Unexpected size, ignore
                    return Ok(None);
                }
                Ok(Some(payload.drain(..).collect()))
            }
            None => Ok(None),
        }
    }

    fn read_from_snapstore_by_hash(&self, hash: u64) -> Result<Option<Vec<u8>>> {
        // Open store lazily
        {
            let mut opt = self.store.borrow_mut();
            if opt.is_none() {
                let ps = match read_meta(&self.root) {
                    Ok(m) => m.page_size,
                    Err(_) => return Ok(None),
                };
                if let Ok(ss) = SnapStore::open(&self.root, ps) {
                    *opt = Some(ss);
                } else {
                    return Ok(None);
                }
            }
        }
        // Now get()
        let mut opt = self.store.borrow_mut();
        if let Some(ss) = opt.as_mut() {
            match ss.get(hash) {
                Ok(v) => Ok(v),
                Err(_) => Ok(None), // best-effort
            }
        } else {
            Ok(None)
        }
    }
}

impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        if !self.ended && !self.keep_sidecar {
            let _ = fs::remove_dir_all(&self.freeze_dir);
        }
    }
}

/// Utility: root of sidecar directory for snapshots.
pub fn snapshots_root(root: &Path) -> PathBuf {
    root.join(".snapshots")
}