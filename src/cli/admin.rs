use anyhow::{anyhow, Result};
use std::path::PathBuf;

use crate::meta::read_meta;
use crate::pager::Pager;
use crate::dir::Directory;
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
    // JSON toggle via ENV (no CLI change in this step)
    let json = std::env::var("P1_STATUS_JSON")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false);

    let meta = read_meta(&path)?;
    // Directory (optional)
    let dir_info = Directory::open(&path);
    let (buckets_opt, used_opt) = match &dir_info {
        Ok(dir) => {
            let used = dir.count_used_buckets().unwrap_or(0);
            (Some(dir.bucket_count), Some(used))
        }
        Err(_) => (None, None),
    };

    // Free-list (optional)
    let free_pages_opt = match FreeList::open(&path) {
        Ok(fl) => fl.count().ok(),
        Err(_) => None,
    };

    if json {
        // JSON one-liner summary
        let path_s = crate::cli::admin::json_escape(&path.display().to_string());
        let hash_kind = meta.hash_kind.to_string(); // "xxhash64(seed=0)"
        let tde = (meta.flags & 0x1) != 0;

        let mut fields = Vec::new();
        fields.push(format!("\"path\":\"{}\"", path_s));
        fields.push(format!("\"version\":{}", meta.version));
        fields.push(format!("\"page_size\":{}", meta.page_size));
        fields.push(format!("\"hash_kind\":\"{}\"", crate::cli::admin::json_escape(&hash_kind)));
        fields.push(format!("\"flags\":{}", meta.flags));
        fields.push(format!("\"tde\":{}", tde));
        fields.push(format!("\"next_page_id\":{}", meta.next_page_id));
        fields.push(format!("\"last_lsn\":{}", meta.last_lsn));
        fields.push(format!("\"clean_shutdown\":{}", meta.clean_shutdown));
        if let Some(b) = buckets_opt {
            fields.push(format!("\"buckets\":{}", b));
        } else {
            fields.push("\"buckets\":null".to_string());
        }
        if let Some(u) = used_opt {
            fields.push(format!("\"used_buckets\":{}", u));
        } else {
            fields.push("\"used_buckets\":null".to_string());
        }
        if let Some(fp) = free_pages_opt {
            fields.push(format!("\"free_pages\":{}", fp));
        } else {
            fields.push("\"free_pages\":null".to_string());
        }
        println!("{{{}}}", fields.join(","));
        return Ok(());
    }

    // Human-friendly output (original behavior)
    println!("DB at {}", path.display());
    println!("  version      = {}", meta.version);
    println!("  page_size    = {} bytes", meta.page_size);
    println!("  hash_kind    = {}", meta.hash_kind);
    println!("  flags        = 0x{:08x}", meta.flags);
    println!("    tde        = {}", (meta.flags & 0x1) != 0);
    println!("  next_page_id = {}", meta.next_page_id);
    println!("  last_lsn     = {}", meta.last_lsn);
    println!("  clean_shutdown = {}", meta.clean_shutdown);

    if let Ok(dir) = Directory::open(&path) {
        println!("  buckets      = {}", dir.bucket_count);
        let used = dir.count_used_buckets()?;
        println!("  used_buckets = {}", used);
    } else {
        println!("  directory    = (not initialized)");
    }

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
    let _lock = crate::lock::acquire_exclusive_lock(&path)?;
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
    let _lock = crate::lock::acquire_exclusive_lock(&path)?;
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
    let _lock = crate::lock::acquire_exclusive_lock(&path)?;
    let fl = FreeList::open(&path)?;
    fl.push(page_id)?;
    println!("Pushed page {} into free-list", page_id);
    Ok(())
}

pub fn cmd_free_pop(path: PathBuf, count: u32) -> Result<()> {
    let _lock = crate::lock::acquire_exclusive_lock(&path)?;
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

// ---------- v0.8/0.9: DB check (CRC scan, overflow reachability, strict mode, JSON) ----------

use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::consts::{PAGE_MAGIC, NO_PAGE, FREE_FILE, FREE_HDR_SIZE, FREE_MAGIC};
use crate::page_rh::{rh_header_read, rh_kv_list};
use crate::page_ovf::ovf_header_read;

fn read_free_set(root: &Path) -> Result<HashSet<u64>> {
    let path = root.join(FREE_FILE);
    let mut set = HashSet::new();
    if !path.exists() {
        return Ok(set);
    }
    let mut f = OpenOptions::new().read(true).open(&path)?;
    let mut magic = [0u8; 8];
    f.read_exact(&mut magic)?;
    if &magic != FREE_MAGIC {
        return Ok(set);
    }
    let mut hdr = vec![0u8; FREE_HDR_SIZE - 8];
    f.read_exact(&mut hdr)?;
    let len = f.metadata()?.len();
    let mut pos = FREE_HDR_SIZE as u64;
    while pos + 8 <= len {
        let mut buf8 = [0u8; 8];
        f.seek(SeekFrom::Start(pos))?;
        f.read_exact(&mut buf8)?;
        let pid = LittleEndian::read_u64(&buf8);
        set.insert(pid);
        pos += 8;
    }
    Ok(set)
}

fn json_escape(s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    for ch in s.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => out.push_str(&format!("\\u{:04x}", c as u32)),
            c => out.push(c),
        }
    }
    out
}

/// JSON-capable check. If `json=true`, prints a single JSON object with summary.
/// In strict mode returns error when directory fails, crc/io errors exist, or overflow orphans > 0.
pub fn cmd_check_strict_json(path: PathBuf, strict: bool, json: bool) -> Result<()> {
    let mut dir_ok = true;
    let mut dir_err: Option<String> = None;

    // 1) Directory
    match Directory::open(&path) {
        Ok(dir) => {
            if !json {
                println!("Directory: OK (buckets={}, kind={})", dir.bucket_count, dir.hash_kind);
            }
        }
        Err(e) => {
            dir_ok = false;
            dir_err = Some(e.to_string());
            if !json {
                println!("Directory: ERROR: {}", dir_err.as_ref().unwrap());
            }
        }
    }

    // 2) Pager scan
    let pager = Pager::open(&path)?;
    let ps = pager.meta.page_size as usize;
    let pages = pager.meta.next_page_id;
    let free_set = read_free_set(&path)?;

    let mut v2_rh = 0u64;
    let mut v2_ovf = 0u64;
    let mut magic_other = 0u64;
    let mut no_magic = 0u64;
    let mut crc_fail = 0u64;
    let mut io_fail = 0u64;

    let mut ovf_all: Vec<u64> = Vec::new();
    let mut ovf_marked: HashSet<u64> = HashSet::new();

    let mut buf = vec![0u8; ps];

    for pid in 0..pages {
        match pager.read_page(pid, &mut buf) {
            Ok(()) => {
                if buf.len() >= 4 && &buf[..4] == PAGE_MAGIC {
                    let ver = LittleEndian::read_u16(&buf[4..6]);
                    if ver >= 2 {
                        if rh_header_read(&buf).is_ok() {
                            v2_rh += 1;
                            if let Ok(items) = rh_kv_list(&buf) {
                                for (_k, v) in items {
                                    if v.len() == 18 && v[0] == 0xFF {
                                        let head_pid = LittleEndian::read_u64(&v[10..18]);
                                        let mut cur = head_pid;
                                        while cur != NO_PAGE && !ovf_marked.contains(&cur) {
                                            let mut obuf = vec![0u8; ps];
                                            if pager.read_page(cur, &mut obuf).is_err() {
                                                break;
                                            }
                                            if let Ok(h) = ovf_header_read(&obuf) {
                                                ovf_marked.insert(cur);
                                                cur = h.next_page_id;
                                            } else {
                                                break;
                                            }
                                        }
                                    }
                                }
                            }
                        } else if ovf_header_read(&buf).is_ok() {
                            v2_ovf += 1;
                            ovf_all.push(pid);
                        } else {
                            magic_other += 1;
                        }
                    } else {
                        magic_other += 1;
                    }
                } else {
                    no_magic += 1;
                }
            }
            Err(e) => {
                let msg = e.to_string();
                if msg.to_ascii_lowercase().contains("crc") {
                    crc_fail += 1;
                } else {
                    io_fail += 1;
                }
            }
        }
    }

    let mut ovf_orphans: Vec<u64> = Vec::new();
    for pid in ovf_all {
        if !ovf_marked.contains(&pid) && !free_set.contains(&pid) {
            ovf_orphans.push(pid);
        }
    }

    if json {
        let path_s = json_escape(&path.display().to_string());
        let preview = format!("[{}]", ovf_orphans.iter().take(16).map(|n| n.to_string()).collect::<Vec<_>>().join(","));
        let mut fields = vec![
            format!("\"path\":\"{}\"", path_s),
            format!("\"dir_ok\":{}", dir_ok),
            format!("\"pages_total\":{}", pages),
            format!("\"v2_rh_pages\":{}", v2_rh),
            format!("\"v2_overflow\":{}", v2_ovf),
            format!("\"other_magic\":{}", magic_other),
            format!("\"no_magic\":{}", no_magic),
            format!("\"crc_fail\":{}", crc_fail),
            format!("\"io_fail\":{}", io_fail),
            format!("\"free_pages\":{}", free_set.len()),
            format!("\"overflow_reached\":{}", ovf_marked.len()),
            format!("\"overflow_orphans\":{}", ovf_orphans.len()),
            format!("\"orphans_preview\":{}", preview),
        ];
        if !dir_ok {
            if let Some(e) = &dir_err {
                fields.push(format!("\"dir_error\":\"{}\"", json_escape(e)));
            }
        }
        println!("{{{}}}", fields.join(","));
    } else {
        println!("Running check for {} (strict={})", path.display(), strict);
        if dir_ok {
            if let Ok(dir) = Directory::open(&path) {
                println!("Directory: OK (buckets={}, kind={})", dir.bucket_count, dir.hash_kind);
            }
        } else {
            println!("Directory: ERROR: {}", dir_err.unwrap_or_else(|| "unknown".to_string()));
        }

        println!("Check report:");
        println!("  pages_total      = {}", pages);
        println!("  v2_rh_pages      = {}", v2_rh);
        println!("  v2_overflow      = {}", v2_ovf);
        println!("  other_magic      = {}", magic_other);
        println!("  no_magic         = {}", no_magic);
        println!("  crc_fail         = {}", crc_fail);
        println!("  io_fail          = {}", io_fail);
        println!("  free_pages       = {}", free_set.len());
        println!("  overflow_reached = {}", ovf_marked.len());
        println!("  overflow_orphans = {}", ovf_orphans.len());
        if !ovf_orphans.is_empty() {
            let preview = ovf_orphans.iter().take(16).cloned().collect::<Vec<_>>();
            println!("  orphans_preview  = {:?}", preview);
        }
    }

    if strict {
        let has_issues = !dir_ok || crc_fail > 0 || io_fail > 0 || !ovf_orphans.is_empty();
        if has_issues {
            return Err(anyhow!(
                "check failed (strict): dir_ok={}, crc_fail={}, io_fail={}, overflow_orphans={}",
                dir_ok, crc_fail, io_fail, ovf_orphans.len()
            ));
        }
    }

    Ok(())
}

/// Backward-compatible wrappers
pub fn cmd_check_strict(path: PathBuf, strict: bool) -> Result<()> {
    cmd_check_strict_json(path, strict, false)
}
pub fn cmd_check(path: PathBuf) -> Result<()> {
    cmd_check_strict_json(path, false, false)
}

// ---------- v0.9: Repair (no change here) ----------

pub fn cmd_repair(path: PathBuf) -> Result<()> {
    let _lock = crate::lock::acquire_exclusive_lock(&path)?;
    let guard = DirtyGuard::begin(&path)?;
    wal_replay_if_any(&path)?;

    let dir = Directory::open(&path)
        .map_err(|e| anyhow!("repair requires valid directory: {e}"))?;

    let pager = Pager::open(&path)?;
    let ps = pager.meta.page_size as usize;
    let pages = pager.meta.next_page_id;
    let free_set = read_free_set(&path)?;

    let mut marked: HashSet<u64> = HashSet::new();
    for b in 0..dir.bucket_count {
        let mut pid = dir.head(b)?;
        while pid != NO_PAGE {
            let mut buf = vec![0u8; ps];
            if pager.read_page(pid, &mut buf).is_err() {
                break;
            }
            if !crate::page_rh::rh_page_is_kv(&buf) {
                break;
            }
            if let Ok(items) = crate::page_rh::rh_kv_list(&buf) {
                for (_k, v) in items {
                    if v.len() == 18 && v[0] == 0xFF {
                        let head_pid = LittleEndian::read_u64(&v[10..18]);
                        let mut cur = head_pid;
                        while cur != NO_PAGE && !marked.contains(&cur) {
                            let mut obuf = vec![0u8; ps];
                            if pager.read_page(cur, &mut obuf).is_err() {
                                break;
                            }
                            if let Ok(h) = crate::page_ovf::ovf_header_read(&obuf) {
                                marked.insert(cur);
                                cur = h.next_page_id;
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
            let h = crate::page_rh::rh_header_read(&buf)?;
            pid = h.next_page_id;
        }
    }

    let mut all_ovf: Vec<u64> = Vec::new();
    for pid in 0..pages {
        let mut buf = vec![0u8; ps];
        if pager.read_page(pid, &mut buf).is_ok() {
            if crate::page_ovf::ovf_header_read(&buf).is_ok() {
                all_ovf.push(pid);
            }
        }
    }

    let orphans: Vec<u64> = all_ovf
        .into_iter()
        .filter(|pid| !marked.contains(pid) && !free_set.contains(pid))
        .collect();

    let mut freed = 0usize;
    for pid in orphans {
        pager.free_page(pid)?;
        freed += 1;
    }

    println!("Repair: freed {} orphan overflow page(s)", freed);
    guard.finish()?;
    Ok(())
}

// ---------- v0.9: Metrics (snapshot/reset + JSON) ----------

pub fn cmd_metrics_json(json: bool) -> Result<()> {
    let m = crate::metrics::snapshot();

    if json {
        let avg = m.avg_wal_batch_pages();
        let hit_ratio = m.cache_hit_ratio() * 100.0;
        println!(
            "{{\
            \"wal_appends_total\":{},\
            \"wal_bytes_written\":{},\
            \"wal_fsync_calls\":{},\
            \"wal_fsync_batch_pages\":{},\
            \"wal_truncations\":{},\
            \"page_cache_hits\":{},\
            \"page_cache_misses\":{},\
            \"rh_page_compactions\":{},\
            \"overflow_chains_created\":{},\
            \"overflow_chains_freed\":{},\
            \"sweep_orphan_runs\":{},\
            \"avg_wal_batch_pages\":{:.2},\
            \"page_cache_hit_ratio\":{:.2},\
            \"snapshots_active\":{},\
            \"snapshot_freeze_frames\":{},\
            \"snapshot_freeze_bytes\":{},\
            \"backup_pages_emitted\":{},\
            \"backup_bytes_emitted\":{},\
            \"restore_pages_written\":{},\
            \"restore_bytes_written\":{},\
            \"snapshot_fallback_scans\":{}\
            }}",
            m.wal_appends_total,
            m.wal_bytes_written,
            m.wal_fsync_calls,
            m.wal_fsync_batch_pages,
            m.wal_truncations,
            m.page_cache_hits,
            m.page_cache_misses,
            m.rh_page_compactions,
            m.overflow_chains_created,
            m.overflow_chains_freed,
            m.sweep_orphan_runs,
            avg,
            hit_ratio,
            m.snapshots_active,
            m.snapshot_freeze_frames,
            m.snapshot_freeze_bytes,
            m.backup_pages_emitted,
            m.backup_bytes_emitted,
            m.restore_pages_written,
            m.restore_bytes_written,
            m.snapshot_fallback_scans
        );
        return Ok(());
    }

    let cache_total = m.page_cache_hits + m.page_cache_misses;
    let cache_hit_ratio = if cache_total > 0 {
        (m.page_cache_hits as f64) / (cache_total as f64)
    } else {
        0.0
    };

    println!("Metrics snapshot:");
    println!("  wal_appends_total        = {}", m.wal_appends_total);
    println!("  wal_bytes_written        = {}", m.wal_bytes_written);
    println!("  wal_fsync_calls          = {}", m.wal_fsync_calls);
    println!("  wal_avg_batch_pages      = {:.2}", m.avg_wal_batch_pages());
    println!("  wal_truncations          = {}", m.wal_truncations);
    println!("  page_cache_hits          = {}", m.page_cache_hits);
    println!("  page_cache_misses        = {}", m.page_cache_misses);
    println!("  page_cache_hit_ratio     = {:.2}%", cache_hit_ratio * 100.0);
    println!("  rh_page_compactions      = {}", m.rh_page_compactions);
    println!("  overflow_chains_created  = {}", m.overflow_chains_created);
    println!("  overflow_chains_freed    = {}", m.overflow_chains_freed);
    println!("  sweep_orphan_runs        = {}", m.sweep_orphan_runs);
    // NEW: снапшоты/бэкап/рестор + fallback
    println!("  snapshots_active         = {}", m.snapshots_active);
    println!("  snapshot_freeze_frames   = {}", m.snapshot_freeze_frames);
    println!("  snapshot_freeze_bytes    = {}", m.snapshot_freeze_bytes);
    println!("  backup_pages_emitted     = {}", m.backup_pages_emitted);
    println!("  backup_bytes_emitted     = {}", m.backup_bytes_emitted);
    println!("  restore_pages_written    = {}", m.restore_pages_written);
    println!("  restore_bytes_written    = {}", m.restore_bytes_written);
    println!("  snapshot_fallback_scans  = {}", m.snapshot_fallback_scans);

    Ok(())
}

pub fn cmd_metrics() -> Result<()> {
    cmd_metrics_json(false)
}

pub fn cmd_metrics_reset() -> Result<()> {
    crate::metrics::reset();
    println!("Metrics: reset to zero");
    Ok(())
}