use anyhow::{anyhow, Result};
use std::path::PathBuf;

use crate::dir::Directory;
use crate::lock::acquire_exclusive_lock;
use crate::meta::read_meta;
use crate::pager::Pager;
use crate::util::hex_dump;
use crate::{init_db, wal_replay_if_any};
use crate::free::FreeList;

use super::DirtyGuard;

pub fn cmd_init(path: PathBuf, page_size: u32) -> Result<()> {
    init_db(&path, page_size)?;
    println!("Initialized DB at {}", path.display());
    Ok(())
}

pub fn cmd_status(path: PathBuf) -> Result<()> {
    let meta = read_meta(&path)?;
    println!("DB at {}", path.display());
    println!("  version      = {}", meta.version);
    println!("  page_size    = {} bytes", meta.page_size);
    println!("  hash_kind    = {}", meta.hash_kind);
    println!("  flags        = 0x{:08x}", meta.flags);
    println!("    tde        = {}", (meta.flags & 0x1) != 0);
    println!("  next_page_id = {}", meta.next_page_id);

    if let Ok(dir) = Directory::open(&path) {
        println!("  buckets      = {}", dir.bucket_count);
        let used = dir.count_used_buckets()?;
        println!("  used_buckets = {}", used);
    } else {
        println!("  directory    = (not initialized)");
    }

    // Free-list (optional)
    match FreeList::open(&path) {
        Ok(fl) => {
            let cnt = fl.count()?;
            println!("  free_pages   = {}", cnt);
        }
        Err(_) => {
            println!("  free_pages   = (free-list not found)");
        }
    }

    Ok(())
}

pub fn cmd_alloc(path: PathBuf, count: u32) -> Result<()> {
    let _lock = acquire_exclusive_lock(&path)?;
    let guard = DirtyGuard::begin(&path)?;
    wal_replay_if_any(&path)?;
    let mut pager = Pager::open(&path)?;
    let start = pager.allocate_pages(count as u64)?;
    println!(
        "Allocated {} page(s): [{}..{}]",
        count,
        start,
        start + count as u64 - 1
    );
    guard.finish()?;
    Ok(())
}

pub fn cmd_write(path: PathBuf, page_id: u64, fill: u8) -> Result<()> {
    let _lock = acquire_exclusive_lock(&path)?;
    let guard = DirtyGuard::begin(&path)?;
    wal_replay_if_any(&path)?;
    let mut pager = Pager::open(&path)?;
    pager.ensure_allocated(page_id)?;
    let ps = pager.meta.page_size as usize;
    let mut buf = vec![fill; ps];
    pager.commit_page(page_id, &mut buf)?;
    println!("Wrote page {} with fill=0x{:02x}", page_id, fill);
    guard.finish()?;
    Ok(())
}

pub fn cmd_read(path: PathBuf, page_id: u64, len: usize) -> Result<()> {
    wal_replay_if_any(&path)?;
    let pager = Pager::open(&path)?;
    if page_id >= pager.meta.next_page_id {
        return Err(anyhow!(
            "page {} not allocated yet (next_page_id = {})",
            page_id,
            pager.meta.next_page_id
        ));
    }
    let ps = pager.meta.page_size as usize;
    let mut buf = vec![0u8; ps];
    pager.read_page(page_id, &mut buf)?;
    let n = len.min(ps);
    println!("First {} bytes of page {}:", n, page_id);
    println!("{}", hex_dump(&buf[..n]));
    Ok(())
}

// ---------- Free-list tooling (v0.6) ----------

pub fn cmd_free_status(path: PathBuf) -> Result<()> {
    match FreeList::open(&path) {
        Ok(fl) => {
            let cnt = fl.count()?;
            println!(
                "Free-list at {}: {} page(s)",
                fl.path().display(),
                cnt
            );
        }
        Err(e) => {
            println!("Free-list not available at {}: {}", path.display(), e);
        }
    }
    Ok(())
}

pub fn cmd_free_push(path: PathBuf, page_id: u64) -> Result<()> {
    let _lock = acquire_exclusive_lock(&path)?;
    let fl = FreeList::open(&path)?;
    fl.push(page_id)?;
    println!("Pushed page {} into free-list", page_id);
    Ok(())
}

pub fn cmd_free_pop(path: PathBuf, count: u32) -> Result<()> {
    let _lock = acquire_exclusive_lock(&path)?;
    let fl = FreeList::open(&path)?;
    let mut popped: Vec<u64> = Vec::new();
    for _ in 0..count {
        match fl.pop()? {
            Some(pid) => popped.push(pid),
            None => break,
        }
    }
    if popped.is_empty() {
        println!("Free-list is empty");
    } else {
        println!("Popped {} page(s): {:?}", popped.len(), popped);
    }
    Ok(())
}