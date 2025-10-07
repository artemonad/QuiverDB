//! Content-addressed store for snapshot pages (Phase 2).
//!
//! Directory selection:
//! - If P1_SNAPSTORE_DIR is set:
//!   - Absolute path is used as-is;
//!   - Relative path is resolved against DB root (<root>/<P1_SNAPSTORE_DIR>).
//! - Otherwise: default <root>/.snapstore.
//!
//! Files:
//! - store.bin: frames [hash u64][len u32][crc32 u32] + payload(page_size)
//! - index.bin: full snapshot of the index [hash u64][offset u64][refcnt u32][pad u32]
//!
//! Hash: xxhash64(seed=0) over page bytes. CRC: crc32 over [hdr(without crc)] + payload.
//! Notes:
//! - refcnt == 0 is allowed in the map (frames remain in store.bin until GC/compact).
//! - index.bin is rewritten fully on changes (simple and robust).

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::hash::{hash64, HashKind};

/// Compact report.
pub struct CompactReport {
    pub before: u64,
    pub after: u64,
    pub kept: usize,
    pub dropped: usize,
}

pub struct SnapStore {
    root: PathBuf,
    dir: PathBuf,
    store_path: PathBuf,
    index_path: PathBuf,
    page_size: u32,
    map: HashMap<u64, Entry>, // hash -> (offset, refcnt)
}

#[derive(Debug, Clone, Copy)]
struct Entry {
    offset: u64,
    refcnt: u32,
}

impl SnapStore {
    /// Open snapstore (create directory if missing). Loads index.bin (if present).
    /// Directory is selected as described in the header.
    pub fn open(root: &Path, page_size: u32) -> Result<Self> {
        let dir = resolve_snapstore_dir(root);
        std::fs::create_dir_all(&dir)
            .with_context(|| format!("create snapstore dir {}", dir.display()))?;

        let store_path = dir.join("store.bin");
        let index_path = dir.join("index.bin");

        // Ensure store.bin exists.
        if !store_path.exists() {
            let f = OpenOptions::new()
                .create(true)
                .write(true)
                .read(true)
                .open(&store_path)
                .with_context(|| format!("create {}", store_path.display()))?;
            let _ = f.sync_all();
        }

        let mut ss = Self {
            root: root.to_path_buf(),
            dir,
            store_path,
            index_path,
            page_size,
            map: HashMap::new(),
        };

        // Load index (if exists)
        if ss.index_path.exists() {
            ss.load_index()?;
        }

        Ok(ss)
    }

    /// Page size this snapstore was opened for.
    pub fn page_size(&self) -> u32 {
        self.page_size
    }

    /// Expose actual snapstore directory (for tests/diagnostics).
    pub fn dir_path(&self) -> &Path {
        &self.dir
    }

    /// Check if a frame exists by hash.
    pub fn contains(&self, hash: u64) -> bool {
        self.map.contains_key(&hash)
    }

    /// Get frame by hash.
    pub fn get(&self, hash: u64) -> Result<Option<Vec<u8>>> {
        let entry = match self.map.get(&hash) {
            Some(e) => *e,
            None => return Ok(None),
        };

        let mut f = OpenOptions::new()
            .read(true)
            .open(&self.store_path)
            .with_context(|| format!("open {}", self.store_path.display()))?;

        // Header: [hash u64][len u32][crc32 u32]
        let mut hdr = [0u8; 8 + 4 + 4];
        f.seek(SeekFrom::Start(entry.offset))?;
        f.read_exact(&mut hdr)?;

        let h = LittleEndian::read_u64(&hdr[0..8]);
        if h != hash {
            return Err(anyhow!(
                "snapstore: header hash mismatch at off={}, expected={}, got={}",
                entry.offset,
                hash,
                h
            ));
        }
        let len = LittleEndian::read_u32(&hdr[8..12]) as usize;
        let crc_expected = LittleEndian::read_u32(&hdr[12..16]);

        if len as u32 != self.page_size {
            return Err(anyhow!(
                "snapstore: frame len {} != page_size {}",
                len,
                self.page_size
            ));
        }

        let mut payload = vec![0u8; len];
        f.read_exact(&mut payload)?;

        // CRC verify
        let mut hasher = Crc32::new();
        hasher.update(&hdr[0..12]);
        hasher.update(&payload);
        let crc_actual = hasher.finalize();
        if crc_actual != crc_expected {
            return Err(anyhow!(
                "snapstore: CRC mismatch for hash {} at off {}",
                hash,
                entry.offset
            ));
        }

        Ok(Some(payload))
    }

    /// Insert (or increment refcount) a frame for the page bytes.
    /// Returns (hash, offset).
    pub fn put(&mut self, page: &[u8]) -> Result<(u64, u64)> {
        if page.len() as u32 != self.page_size {
            return Err(anyhow!(
                "snapstore.put: page size {} != {}",
                page.len(),
                self.page_size
            ));
        }

        // Content hash (xxhash64(seed=0))
        let h = hash64(HashKind::Xx64Seed0, page);

        // Already present — bump refcnt and save index
        if self.map.contains_key(&h) {
            let off: u64 = {
                let e = self
                    .map
                    .get_mut(&h)
                    .expect("entry must exist because contains_key returned true");
                e.refcnt = e.refcnt.saturating_add(1);
                e.offset
            };
            self.save_index()?; // persist refcnt
            return Ok((h, off));
        }

        // Append new frame to store.bin
        let mut f = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&self.store_path)
            .with_context(|| format!("open {}", self.store_path.display()))?;

        let off = f.metadata()?.len();

        // Header: [hash u64][len u32][crc32 u32]
        let mut hdr = vec![0u8; 8 + 4 + 4];
        LittleEndian::write_u64(&mut hdr[0..8], h);
        LittleEndian::write_u32(&mut hdr[8..12], self.page_size);

        let mut hasher = Crc32::new();
        hasher.update(&hdr[0..12]);
        hasher.update(page);
        let crc = hasher.finalize();
        LittleEndian::write_u32(&mut hdr[12..16], crc);

        f.seek(SeekFrom::End(0))?;
        f.write_all(&hdr)?;
        f.write_all(page)?;
        let _ = f.sync_all();

        // Update in-memory index
        self.map.insert(h, Entry { offset: off, refcnt: 1 });

        // Rewrite index.bin
        self.save_index()?;

        Ok((h, off))
    }

    /// Increase refcount for an existing hash.
    pub fn add_ref(&mut self, hash: u64) -> Result<()> {
        {
            let e = self
                .map
                .get_mut(&hash)
                .ok_or_else(|| anyhow!("snapstore.add_ref: unknown hash {}", hash))?;
            e.refcnt = e.refcnt.saturating_add(1);
        }
        self.save_index()
    }

    /// Decrease refcount (down to 0). Data remains until future GC/compact.
    pub fn dec_ref(&mut self, hash: u64) -> Result<()> {
        {
            let e = self
                .map
                .get_mut(&hash)
                .ok_or_else(|| anyhow!("snapstore.dec_ref: unknown hash {}", hash))?;
            if e.refcnt > 0 {
                e.refcnt -= 1;
            }
        }
        self.save_index()
    }

    /// Compact store.bin: keep only frames with refcnt > 0, rebuild offsets and index.bin.
    pub fn compact(&mut self) -> Result<CompactReport> {
        // Old size
        let before = std::fs::metadata(&self.store_path)?.len();

        // Open source store.bin
        let mut fin = OpenOptions::new()
            .read(true)
            .open(&self.store_path)
            .with_context(|| format!("open {}", self.store_path.display()))?;

        // Temporary output file
        let tmp_path = self.dir.join("store.bin.compact.tmp");
        let mut fout = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp_path)
            .with_context(|| format!("open {}", tmp_path.display()))?;

        // Build new offsets for hashes with refcnt>0
        let mut new_offsets: HashMap<u64, u64> = HashMap::new();
        let mut kept = 0usize;
        let mut dropped = 0usize;

        for (h, e) in self.map.iter() {
            if e.refcnt == 0 {
                dropped += 1;
                continue;
            }

            // Read original frame
            let mut hdr = [0u8; 8 + 4 + 4];
            fin.seek(SeekFrom::Start(e.offset))?;
            fin.read_exact(&mut hdr)?;

            let hash_in = LittleEndian::read_u64(&hdr[0..8]);
            if hash_in != *h {
                return Err(anyhow!(
                    "snapstore.compact: header hash mismatch at off={}, expected={}, got={}",
                    e.offset,
                    h,
                    hash_in
                ));
            }
            let len = LittleEndian::read_u32(&hdr[8..12]) as usize;
            if len as u32 != self.page_size {
                return Err(anyhow!(
                    "snapstore.compact: frame len {} != page_size {}",
                    len,
                    self.page_size
                ));
            }
            let mut payload = vec![0u8; len];
            fin.read_exact(&mut payload)?;

            // Write new frame with recomputed CRC
            let new_off = fout.seek(SeekFrom::End(0))?;

            let mut out_hdr = [0u8; 8 + 4 + 4];
            LittleEndian::write_u64(&mut out_hdr[0..8], *h);
            LittleEndian::write_u32(&mut out_hdr[8..12], self.page_size);
            let mut hasher = Crc32::new();
            hasher.update(&out_hdr[0..12]);
            hasher.update(&payload);
            let crc = hasher.finalize();
            LittleEndian::write_u32(&mut out_hdr[12..16], crc);

            fout.write_all(&out_hdr)?;
            fout.write_all(&payload)?;

            new_offsets.insert(*h, new_off);
            kept += 1;
        }

        let _ = fout.sync_all();

        // Atomic replace store.bin
        let backup = self.dir.join("store.bin.old");
        let _ = std::fs::rename(&self.store_path, &backup);
        std::fs::rename(&tmp_path, &self.store_path)
            .with_context(|| "rename compacted store.bin")?;
        let _ = std::fs::remove_file(&backup);

        // Update map: new offsets, drop refcnt==0
        self.map.retain(|_h, e| e.refcnt > 0);
        for (h, e) in self.map.iter_mut() {
            if let Some(&off) = new_offsets.get(h) {
                e.offset = off;
            } else {
                e.refcnt = 0;
            }
        }

        // Rewrite index.bin
        self.save_index()?;

        // New size
        let after = std::fs::metadata(&self.store_path)?.len();

        Ok(CompactReport {
            before,
            after,
            kept,
            dropped,
        })
    }

    /// Rewrite index.bin from current map.
    fn save_index(&self) -> Result<()> {
        let mut f = OpenOptions::new()
            .create(true)
            .truncate(true)
            .write(true)
            .open(&self.index_path)
            .with_context(|| format!("open {}", self.index_path.display()))?;

        // Record: [hash u64][offset u64][refcnt u32][pad u32]
        let mut rec = [0u8; 8 + 8 + 4 + 4];
        for (h, e) in self.map.iter() {
            LittleEndian::write_u64(&mut rec[0..8], *h);
            LittleEndian::write_u64(&mut rec[8..16], e.offset);
            LittleEndian::write_u32(&mut rec[16..20], e.refcnt);
            LittleEndian::write_u32(&mut rec[20..24], 0);
            f.write_all(&rec)?;
        }
        let _ = f.sync_all();
        Ok(())
    }

    /// Load index.bin into memory.
    fn load_index(&mut self) -> Result<()> {
        let mut f = OpenOptions::new()
            .read(true)
            .open(&self.index_path)
            .with_context(|| format!("open {}", self.index_path.display()))?;

        let len = f.metadata()?.len();
        const REC: u64 = 8 + 8 + 4 + 4;

        if len % REC != 0 {
            return Err(anyhow!(
                "snapstore: index.bin length {} not aligned to {}",
                len,
                REC
            ));
        }

        let mut pos = 0u64;
        let mut buf = [0u8; REC as usize];

        self.map.clear();

        while pos + REC <= len {
            f.seek(SeekFrom::Start(pos))?;
            f.read_exact(&mut buf)?;

            let h = LittleEndian::read_u64(&buf[0..8]);
            let off = LittleEndian::read_u64(&buf[8..16]);
            let rc = LittleEndian::read_u32(&buf[16..20]);
            // pad ignored

            self.map.insert(h, Entry { offset: off, refcnt: rc });

            pos += REC;
        }
        Ok(())
    }
}

/// Resolve snapstore directory according to P1_SNAPSTORE_DIR.
/// - Not set or empty => <root>/.snapstore
/// - Absolute path => used as-is
/// - Relative path => <root>/<value>
fn resolve_snapstore_dir(root: &Path) -> PathBuf {
    match std::env::var("P1_SNAPSTORE_DIR") {
        Ok(s) => {
            let s = s.trim();
            if s.is_empty() {
                return root.join(".snapstore");
            }
            let p = PathBuf::from(s);
            if p.is_absolute() {
                p
            } else {
                root.join(p)
            }
        }
        Err(_) => root.join(".snapstore"),
    }
}