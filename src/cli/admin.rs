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

// ---------- v0.8: DB check (CRC scan, overflow reachability) ----------

use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::Path;

use crate::consts::{PAGE_MAGIC, NO_PAGE, FREE_FILE, FREE_HDR_SIZE, FREE_MAGIC};
use crate::page_rh::{rh_header_read, rh_page_is_kv, rh_kv_list};
use crate::page_ovf::ovf_header_read;

/// Вспомогательно: прочитать множество page_id, лежащих в free-list.
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

/// Полный скан БД:
/// - Проверка каталога (Directory::open валидирует CRC/версию).
/// - Проход по всем страницам: учёт v2 RH/Overflow, unknown/magic, ошибки/CRC mismatch.
/// - Подсчёт живых Overflow и достижимых из плейсхолдеров (orphans).
pub fn cmd_check(path: PathBuf) -> Result<()> {
    println!("Running check for {}", path.display());

    // 1) Directory
    match Directory::open(&path) {
        Ok(dir) => {
            println!("Directory: OK (buckets={}, kind={})", dir.bucket_count, dir.hash_kind);
        }
        Err(e) => {
            println!("Directory: ERROR: {}", e);
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
                        // RH?
                        if rh_header_read(&buf).is_ok() {
                            v2_rh += 1;

                            // Соберём головы overflow из значений-«плейсхолдеров» и пометим всю цепочку
                            if let Ok(items) = rh_kv_list(&buf) {
                                for (_k, v) in items {
                                    // Плейсхолдер: tag=0xFF, len=18
                                    if v.len() == 18 && v[0] == 0xFF {
                                        let head_pid = LittleEndian::read_u64(&v[10..18]);
                                        // Обойдём overflow-цепочку и пометим
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
                            magic_other += 1; // наш magic, но не RH/OVF (неожиданный тип/версия)
                        }
                    } else {
                        magic_other += 1; // старый формат (поддержка не требуется, учитываем как «другой»)
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

    // Подсчёт сирот: overflow-страницы, которые не помечены достижимыми и не лежат в free-list.
    let mut ovf_orphans: Vec<u64> = Vec::new();
    for pid in ovf_all {
        if !ovf_marked.contains(&pid) && !free_set.contains(&pid) {
            ovf_orphans.push(pid);
        }
    }

    // 3) Report
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

    Ok(())
}

/// v0.9: Repair — освободить сиротские overflow-страницы (best-effort).
/// - Требует exclusive-lock.
/// - Делает wal_replay_if_any перед сканом.
/// - Строго маркирует достижимые overflow из каталога; всё остальное, что является overflow и не в free-list — освобождается.
pub fn cmd_repair(path: PathBuf) -> Result<()> {
    let _lock = acquire_exclusive_lock(&path)?;
    let guard = DirtyGuard::begin(&path)?;
    wal_replay_if_any(&path)?;

    // Каталог обязателен для консервативного ремонта.
    let dir = Directory::open(&path)
        .map_err(|e| anyhow!("repair requires valid directory: {e}"))?;

    let mut pager = Pager::open(&path)?;
    let ps = pager.meta.page_size as usize;
    let pages = pager.meta.next_page_id;
    let free_set = read_free_set(&path)?;

    // Соберём "достижимые" overflow через плейсхолдеры из всех цепочек каталога.
    let mut marked: HashSet<u64> = HashSet::new();

    for b in 0..dir.bucket_count {
        let mut pid = dir.head(b)?;
        while pid != NO_PAGE {
            let mut buf = vec![0u8; ps];
            if pager.read_page(pid, &mut buf).is_err() {
                break; // консервативно
            }
            if !rh_page_is_kv(&buf) {
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
                            if let Ok(h) = ovf_header_read(&obuf) {
                                marked.insert(cur);
                                cur = h.next_page_id;
                            } else {
                                break;
                            }
                        }
                    }
                }
            }
            let h = rh_header_read(&buf)?;
            pid = h.next_page_id;
        }
    }

    // Соберём все overflow-страницы
    let mut all_ovf: Vec<u64> = Vec::new();
    for pid in 0..pages {
        let mut buf = vec![0u8; ps];
        if pager.read_page(pid, &mut buf).is_ok() {
            if ovf_header_read(&buf).is_ok() {
                all_ovf.push(pid);
            }
        }
    }

    // Орфаны: overflow, которые не достижимы и не в free-list.
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

// ---------- v0.9: Metrics (snapshot/reset) ----------

pub fn cmd_metrics() -> Result<()> {
    let m = crate::metrics::snapshot();
    let cache_total = m.page_cache_hits + m.page_cache_misses;
    let cache_hit_ratio = if cache_total > 0 {
        (m.page_cache_hits as f64) / (cache_total as f64)
    } else {
        0.0
    };

    println!("Metrics snapshot:");
    println!("  wal_appends_total       = {}", m.wal_appends_total);
    println!("  wal_bytes_written       = {}", m.wal_bytes_written);
    println!("  wal_fsync_calls         = {}", m.wal_fsync_calls);
    println!("  wal_avg_batch_pages     = {:.2}", m.avg_wal_batch_pages());
    println!("  wal_truncations         = {}", m.wal_truncations);
    println!("  page_cache_hits         = {}", m.page_cache_hits);
    println!("  page_cache_misses       = {}", m.page_cache_misses);
    println!("  page_cache_hit_ratio    = {:.2}%", cache_hit_ratio * 100.0);
    println!("  rh_page_compactions     = {}", m.rh_page_compactions);
    println!("  overflow_chains_created = {}", m.overflow_chains_created);
    println!("  overflow_chains_freed   = {}", m.overflow_chains_freed);
    println!("  sweep_orphan_runs       = {}", m.sweep_orphan_runs);
    Ok(())
}

pub fn cmd_metrics_reset() -> Result<()> {
    crate::metrics::reset();
    println!("Metrics: reset to zero");
    Ok(())
}