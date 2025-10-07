//! Freeze/Index I/O helpers for snapshots sidecar.
//!
//! Files layout in <root>/.snapshots/<snapshot_id>/:
//! - freeze.bin: frames [page_id u64][page_lsn u64][page_len u32][crc32 u32] + payload(page_size)
//! - index.bin:  entries [page_id u64][offset u64][page_lsn u64]
//! - hashindex.bin (Phase 2): entries [page_id u64][hash u64][page_lsn u64]
//!
//! All helpers are best-effort and return errors to the caller; the caller decides
//! whether to ignore/log them or fail.

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

fn freeze_path_of(dir: &Path) -> PathBuf {
    dir.join("freeze.bin")
}

fn index_path_of(dir: &Path) -> PathBuf {
    dir.join("index.bin")
}

fn hashindex_path_of(dir: &Path) -> PathBuf {
    dir.join("hashindex.bin")
}

/// Append a freeze frame into <freeze_dir>/freeze.bin and matching index entry into index.bin.
/// Returns the byte offset (in freeze.bin) at which the frame starts.
pub(crate) fn write_freeze_frame_to_dir(
    freeze_dir: &Path,
    page_id: u64,
    page_lsn: u64,
    page_bytes: &[u8],
) -> Result<u64> {
    // freeze.bin (append-open)
    let freeze_path = freeze_path_of(freeze_dir);
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(&freeze_path)
        .with_context(|| format!("open freeze {}", freeze_path.display()))?;
    let offset = f.metadata()?.len();

    // Header: [page_id u64][page_lsn u64][page_len u32][crc32 u32]
    let page_len = page_bytes.len() as u32;
    let mut hdr = vec![0u8; 8 + 8 + 4 + 4];
    LittleEndian::write_u64(&mut hdr[0..8], page_id);
    LittleEndian::write_u64(&mut hdr[8..16], page_lsn);
    LittleEndian::write_u32(&mut hdr[16..20], page_len);

    // CRC32 over (header without crc) + payload
    let mut hasher = Crc32::new();
    hasher.update(&hdr[0..20]);
    hasher.update(page_bytes);
    let crc = hasher.finalize();
    LittleEndian::write_u32(&mut hdr[20..24], crc);

    // Append header + payload
    f.seek(SeekFrom::End(0))?;
    f.write_all(&hdr)?;
    f.write_all(page_bytes)?;

    // index.bin (append-open)
    let index_path = index_path_of(freeze_dir);
    let mut idx = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(&index_path)
        .with_context(|| format!("open index {}", index_path.display()))?;
    idx.seek(SeekFrom::End(0))?;

    // Index entry: [page_id u64][offset u64][page_lsn u64]
    let mut entry = vec![0u8; 8 + 8 + 8];
    LittleEndian::write_u64(&mut entry[0..8], page_id);
    LittleEndian::write_u64(&mut entry[8..16], offset as u64);
    LittleEndian::write_u64(&mut entry[16..24], page_lsn);
    idx.write_all(&entry)?;

    Ok(offset as u64)
}

/// Build an in-memory index mapping page_id -> offset (freeze.bin) from <freeze_dir>/index.bin.
pub(crate) fn build_freeze_index(freeze_dir: &Path) -> Result<HashMap<u64, u64>> {
    let mut map = HashMap::new();
    let idx_path = index_path_of(freeze_dir);
    if !idx_path.exists() {
        return Ok(map);
    }
    let mut f = OpenOptions::new()
        .read(true)
        .open(&idx_path)
        .with_context(|| format!("open {}", idx_path.display()))?;
    let mut pos = 0u64;
    let len = f.metadata()?.len();

    // Each record: [page_id u64][offset u64][page_lsn u64] = 24 bytes
    const REC: u64 = 8 + 8 + 8;
    let mut buf = vec![0u8; REC as usize];
    while pos + REC <= len {
        f.seek(SeekFrom::Start(pos))?;
        f.read_exact(&mut buf)?;
        let page_id = LittleEndian::read_u64(&buf[0..8]);
        let off = LittleEndian::read_u64(&buf[8..16]);
        // let _lsn = LittleEndian::read_u64(&buf[16..24]);
        map.insert(page_id, off);
        pos += REC;
    }
    Ok(map)
}

/// Read a single frozen page by offset from <freeze_dir>/freeze.bin.
/// Returns Ok(Some(bytes)) on success, Ok(None) if freeze.bin is missing.
pub(crate) fn read_frozen_page_at_offset(
    freeze_dir: &Path,
    off: u64,
    page_id: u64,
    page_size: usize,
) -> Result<Option<Vec<u8>>> {
    let path = freeze_path_of(freeze_dir);
    if !path.exists() {
        return Ok(None);
    }
    let mut f = OpenOptions::new()
        .read(true)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;

    // Header: [page_id u64][page_lsn u64][page_len u32][crc32 u32]
    let mut hdr = [0u8; 8 + 8 + 4 + 4];
    f.seek(SeekFrom::Start(off))?;
    f.read_exact(&mut hdr)?;
    let pid = LittleEndian::read_u64(&hdr[0..8]);
    if pid != page_id {
        return Err(anyhow!(
            "freeze frame page_id mismatch at off={}, expected={}, got={}",
            off,
            page_id,
            pid
        ));
    }
    let page_len = LittleEndian::read_u32(&hdr[16..20]) as usize;
    if page_len != page_size {
        return Err(anyhow!(
            "freeze frame len={} != page_size {}",
            page_len,
            page_size
        ));
    }
    let crc_expected = LittleEndian::read_u32(&hdr[20..24]);

    // Payload
    let mut payload = vec![0u8; page_len];
    f.read_exact(&mut payload)?;

    // CRC verify
    let mut hasher = Crc32::new();
    hasher.update(&hdr[0..20]);
    hasher.update(&payload);
    let crc_actual = hasher.finalize();
    if crc_actual != crc_expected {
        return Err(anyhow!("freeze frame CRC mismatch for page {}", page_id));
    }

    Ok(Some(payload))
}

// ---------------- Phase 2: hash-index helpers ----------------

/// Append hash-index entry into <freeze_dir>/hashindex.bin.
/// Record layout: [page_id u64][hash u64][page_lsn u64]
pub(crate) fn append_hash_index_entry(
    freeze_dir: &Path,
    page_id: u64,
    page_hash: u64,
    page_lsn: u64,
) -> Result<()> {
    let path = hashindex_path_of(freeze_dir);
    let mut f = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;
    f.seek(SeekFrom::End(0))?;

    let mut rec = [0u8; 8 + 8 + 8];
    LittleEndian::write_u64(&mut rec[0..8], page_id);
    LittleEndian::write_u64(&mut rec[8..16], page_hash);
    LittleEndian::write_u64(&mut rec[16..24], page_lsn);
    f.write_all(&rec)?;
    Ok(())
}

/// Build a hash index (page_id -> hash) from <freeze_dir>/hashindex.bin.
pub(crate) fn build_hash_index(freeze_dir: &Path) -> Result<HashMap<u64, u64>> {
    let mut map = HashMap::new();
    let path = hashindex_path_of(freeze_dir);
    if !path.exists() {
        return Ok(map);
    }
    let mut f = OpenOptions::new()
        .read(true)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;

    let mut pos = 0u64;
    let len = f.metadata()?.len();
    const REC: u64 = 8 + 8 + 8; // [page_id][hash][page_lsn]

    let mut buf = vec![0u8; REC as usize];
    while pos + REC <= len {
        f.seek(SeekFrom::Start(pos))?;
        f.read_exact(&mut buf)?;
        let page_id = LittleEndian::read_u64(&buf[0..8]);
        let hash = LittleEndian::read_u64(&buf[8..16]);
        // let _lsn = LittleEndian::read_u64(&buf[16..24]); // currently unused on read path
        map.insert(page_id, hash);
        pos += REC;
    }

    Ok(map)
}