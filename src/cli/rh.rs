use anyhow::{anyhow, Result};
use std::path::PathBuf;

use crate::meta::read_meta;
use crate::pager::Pager;
use crate::page_rh::{
    rh_compact_inplace, rh_kv_delete_inplace, rh_kv_insert, rh_kv_list, rh_kv_lookup,
    rh_page_init, rh_page_is_kv,
};
use crate::util::{display_text, hex_dump};
use crate::{acquire_exclusive_lock, wal_replay_if_any};

use super::DirtyGuard;

pub fn cmd_pagefmt_rh(path: PathBuf, page_id: u64) -> Result<()> {
    let _lock = acquire_exclusive_lock(&path)?;
    let guard = DirtyGuard::begin(&path)?;
    wal_replay_if_any(&path)?;
    let mut pager = Pager::open(&path)?;
    pager.ensure_allocated(page_id)?;
    let ps = pager.meta.page_size as usize;
    let mut buf = vec![0u8; ps];
    rh_page_init(&mut buf, page_id)?;
    pager.commit_page(page_id, &mut buf)?;
    println!("Formatted page {} as KV-RH (v2)", page_id);
    guard.finish()?;
    Ok(())
}

pub fn cmd_rh_put(path: PathBuf, page_id: u64, key: String, value: String) -> Result<()> {
    let _lock = acquire_exclusive_lock(&path)?;
    let guard = DirtyGuard::begin(&path)?;
    wal_replay_if_any(&path)?;
    let meta = read_meta(&path)?;
    let mut pager = Pager::open(&path)?;
    pager.ensure_allocated(page_id)?;
    let ps = pager.meta.page_size as usize;
    let mut buf = vec![0u8; ps];
    pager.read_page(page_id, &mut buf)?;
    if !rh_page_is_kv(&buf) {
        return Err(anyhow!(
            "page {} is not KV-RH formatted (run 'pagefmt-rh' first)",
            page_id
        ));
    }
    let k = key.as_bytes();
    let v = value.as_bytes();
    match rh_kv_insert(&mut buf, meta.hash_kind, k, v)? {
        true => {
            pager.commit_page(page_id, &mut buf)?;
            println!(
                "Inserted key='{}' ({} B), value={} B into RH page {}",
                display_text(k),
                k.len(),
                v.len(),
                page_id
            );
        }
        false => {
            println!(
                "Not enough space on RH page {} for key={} B, value={} B",
                page_id,
                k.len(),
                v.len()
            );
        }
    }
    guard.finish()?;
    Ok(())
}

pub fn cmd_rh_get(path: PathBuf, page_id: u64, key: String) -> Result<()> {
    wal_replay_if_any(&path)?;
    let meta = read_meta(&path)?;
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
    if !rh_page_is_kv(&buf) {
        return Err(anyhow!(
            "page {} is not KV-RH formatted (run 'pagefmt-rh' first)",
            page_id
        ));
    }
    let k = key.as_bytes();
    match rh_kv_lookup(&buf, meta.hash_kind, k)? {
        Some(v) => {
            println!(
                "FOUND key='{}' ({} B) -> value ({} B): {}",
                display_text(k),
                k.len(),
                v.len(),
                display_text(&v)
            );
            println!("hex: {}", hex_dump(&v[..v.len().min(64)]));
        }
        None => println!("NOT FOUND key='{}'", display_text(k)),
    }
    Ok(())
}

pub fn cmd_rh_list(path: PathBuf, page_id: u64) -> Result<()> {
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
    if !rh_page_is_kv(&buf) {
        return Err(anyhow!(
            "page {} is not KV-RH formatted (run 'pagefmt-rh' first)",
            page_id
        ));
    }
    let items = rh_kv_list(&buf)?;
    println!("RH Page {}: {} record(s)", page_id, items.len());
    for (i, (k, v)) in items.iter().enumerate() {
        println!(
            "  {:04}: key='{}' ({} B), value='{}' ({} B)",
            i,
            display_text(k),
            k.len(),
            display_text(v),
            v.len()
        );
    }
    Ok(())
}

pub fn cmd_rh_del(path: PathBuf, page_id: u64, key: String) -> Result<()> {
    let _lock = acquire_exclusive_lock(&path)?;
    let guard = DirtyGuard::begin(&path)?;
    wal_replay_if_any(&path)?;
    let meta = read_meta(&path)?;
    let mut pager = Pager::open(&path)?;
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
    if !rh_page_is_kv(&buf) {
        return Err(anyhow!(
            "page {} is not KV-RH formatted (run 'pagefmt-rh' first)",
            page_id
        ));
    }
    let existed = rh_kv_delete_inplace(&mut buf, meta.hash_kind, key.as_bytes())?;
    if existed {
        pager.commit_page(page_id, &mut buf)?;
        println!("Deleted key='{}' from RH page {}", key, page_id);
    } else {
        println!("Key='{}' not found on RH page {}", key, page_id);
    }
    guard.finish()?;
    Ok(())
}

pub fn cmd_rh_compact(path: PathBuf, page_id: u64) -> Result<()> {
    let _lock = acquire_exclusive_lock(&path)?;
    let guard = DirtyGuard::begin(&path)?;
    wal_replay_if_any(&path)?;
    let meta = read_meta(&path)?;
    let mut pager = Pager::open(&path)?;
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
    if !rh_page_is_kv(&buf) {
        return Err(anyhow!(
            "page {} is not KV-RH formatted (run 'pagefmt-rh' first)",
            page_id
        ));
    }
    rh_compact_inplace(&mut buf, meta.hash_kind)?;
    pager.commit_page(page_id, &mut buf)?;
    println!("Compacted RH page {}", page_id);
    guard.finish()?;
    Ok(())
}