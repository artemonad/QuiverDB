//! Pager: segments, page IO, WAL-integrated commits, and a small read cache.
//!
//! Env toggles (backward compatible):
//! - P1_PAGE_CACHE_PAGES=N  -> enable read cache with N pages (default 0 = disabled)
//! - P1_DATA_FSYNC=[0|1]   -> fsync data segments on every write (default 1)
//!   If set to 0, durability relies on WAL only and WAL will NOT be truncated
//!   in commit_page (to keep crash recovery intact). Use for performance/bench,
//!   not for strict durability of data files.

use crate::consts::{
    DATA_SEG_EXT, DATA_SEG_PREFIX, PAGE_MAGIC, SEGMENT_SIZE, PAGE_TYPE_KV_RH, PAGE_TYPE_OVERFLOW,
};
use crate::free::FreeList;
use crate::meta::{read_meta, write_meta_overwrite, MetaHeader};
use crate::metrics::{record_cache_hit, record_cache_miss};
use crate::page_ovf::{ovf_header_read, ovf_header_write};
use crate::page_rh::{rh_header_read, rh_header_write, rh_page_update_crc, rh_page_verify_crc};
use crate::util::{read_at, write_at};
use crate::wal::Wal;
use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::cell::RefCell;
use std::collections::{BTreeMap};
use std::fs::OpenOptions;
use std::path::{Path, PathBuf};
// NEW: снапшоты (freeze интеграция)
use std::sync::{Arc, Mutex};
use crate::snapshots::SnapshotManager;

/// New: centralized configuration
use crate::config::QuiverConfig;

// Подмодуль кэша страниц (выделен из этого файла)
mod cache;

pub struct Pager {
    pub root: PathBuf,
    pub meta: MetaHeader,
    // Небольшой LRU-подобный кэш страниц (включается переменной окружения или через конфиг).
    cache: RefCell<Option<cache::PageCache>>,
    // Гарантировать ли fsync на сегментах данных при каждой записи.
    // По умолчанию true. Если false — rely-on-WAL (WAL не будет truncate в commit_page).
    data_fsync: bool,
    // NEW: in-process SnapshotManager (shared с Db; используется для freeze перед перезаписью)
    pub(crate) snap_mgr: Option<Arc<Mutex<SnapshotManager>>>,
}

impl Pager {
    /// Backward-compatible open: reads env variables and constructs with defaults.
    pub fn open(root: &Path) -> Result<Self> {
        let cfg = QuiverConfig::from_env();
        Self::open_with_config(root, &cfg)
    }

    /// New: open with explicit configuration (preferred).
    pub fn open_with_config(root: &Path, cfg: &QuiverConfig) -> Result<Self> {
        let meta = read_meta(root)?;

        let cache = if cfg.page_cache_pages > 0 {
            Some(cache::PageCache::new(cfg.page_cache_pages, meta.page_size as usize))
        } else {
            None
        };

        Ok(Self {
            root: root.to_path_buf(),
            meta,
            cache: RefCell::new(cache),
            data_fsync: cfg.data_fsync,
            snap_mgr: None, // присвоим из Db при открытии writer'а
        })
    }

    /// NEW: установить менеджер снапшотов (вызывается Db::open_with_config для writer'а).
    pub fn set_snapshot_manager(&mut self, mgr: Option<Arc<Mutex<SnapshotManager>>>) {
        self.snap_mgr = mgr;
    }

    fn pages_per_seg(&self) -> u64 {
        let ps = self.meta.page_size as u64;
        (SEGMENT_SIZE / ps).max(1)
    }

    fn locate(&self, page_id: u64) -> (u64, u64) {
        let pps = self.pages_per_seg();
        let seg_no = (page_id / pps) + 1;
        let off_in_seg = (page_id % pps) * self.meta.page_size as u64;
        (seg_no, off_in_seg)
    }

    fn seg_path(&self, seg_no: u64) -> PathBuf {
        self.root
            .join(format!("{}{:06}.{}", DATA_SEG_PREFIX, seg_no, DATA_SEG_EXT))
    }

    fn open_seg_rw(&self, seg_no: u64, create: bool) -> Result<std::fs::File> {
        let path = self.seg_path(seg_no);
        let mut opts = OpenOptions::new();
        opts.read(true).write(true);
        if create {
            opts.create(true);
        }
        opts.open(&path)
            .with_context(|| format!("open segment {}", path.display()))
    }

    pub fn allocate_pages(&mut self, count: u64) -> Result<u64> {
        // Группируем расширение по сегментам, чтобы делать по одному set_len на сегмент.
        let start = self.meta.next_page_id;
        let end = start + count;

        let mut need_per_seg: BTreeMap<u64, u64> = BTreeMap::new();
        for pid in start..end {
            let (seg_no, off) = self.locate(pid);
            let need_len = off + self.meta.page_size as u64;
            need_per_seg
                .entry(seg_no)
                .and_modify(|mx| *mx = (*mx).max(need_len))
                .or_insert(need_len);
        }

        for (seg_no, need_len) in need_per_seg {
            let f = self.open_seg_rw(seg_no, true)?;
            let cur_len = f.metadata()?.len();
            if cur_len < need_len {
                f.set_len(need_len)?;
                if self.data_fsync {
                    f.sync_all()?;
                }
            }
        }

        self.meta.next_page_id = end;
        write_meta_overwrite(&self.root, &self.meta)?;
        Ok(start)
    }

    /// Попытаться получить одну страницу из free-list; если пуст — аллоцировать новую.
    /// При реюзе free-страницы meta.next_page_id не меняется.
    pub fn allocate_one_page(&mut self) -> Result<u64> {
        if let Ok(fl) = FreeList::open(&self.root) {
            if let Some(pid) = fl.pop()? {
                // Защитимся от испорченного free-файла.
                if pid >= self.meta.next_page_id {
                    return Err(anyhow!(
                        "free-list returned invalid page_id {} >= next_page_id {}",
                        pid,
                        self.meta.next_page_id
                    ));
                }
                // Убедимся, что физически сегмент/размер присутствуют (после возможного сбоя).
                self.ensure_allocated(pid)?;
                return Ok(pid);
            }
        }
        // Фолбэк — аллоцируем новый «хвостовой» page_id.
        self.allocate_pages(1)
    }

    /// Добавить страницу во free-list (best-effort).
    /// Никакой очистки/обнуления не делаем — потребитель обязан перезаписать страницу перед commit.
    pub fn free_page(&self, page_id: u64) -> Result<()> {
        if page_id >= self.meta.next_page_id {
            return Err(anyhow!(
                "cannot free page {} (>= next_page_id {})",
                page_id,
                self.meta.next_page_id
            ));
        }
        let fl = FreeList::open(&self.root)?;
        fl.push(page_id)
    }

    /// Ensure that given page is physically allocated on disk.
    /// - If page_id >= next_page_id, extends allocation (and meta.next_page_id).
    /// - Otherwise, still verifies/creates underlying segment file and grows it
    ///   to include the page (handles missing/short segment after crash).
    pub fn ensure_allocated(&mut self, page_id: u64) -> Result<()> {
        let (seg_no, off) = self.locate(page_id);

        if page_id >= self.meta.next_page_id {
            // Allocate up to (page_id+1)
            let to_alloc = page_id + 1 - self.meta.next_page_id;
            self.allocate_pages(to_alloc)?;
        } else {
            // Page is considered allocated by meta, but the segment file
            // could be missing or too short (e.g., crash + removed file).
            // Create/open the segment with create=true and ensure length.
            let f = self.open_seg_rw(seg_no, true)?;
            let need_len = off + self.meta.page_size as u64;
            let cur_len = f.metadata()?.len();
            if cur_len < need_len {
                f.set_len(need_len)?;
                if self.data_fsync {
                    f.sync_all()?;
                }
            }
        }
        Ok(())
    }

    // "Raw" запись без WAL (используется при replay)
    pub fn write_page_raw(&mut self, page_id: u64, buf: &[u8]) -> Result<()> {
        if buf.len() != self.meta.page_size as usize {
            return Err(anyhow!(
                "buffer size {} != page_size {}",
                buf.len(),
                self.meta.page_size
            ));
        }
        if page_id >= self.meta.next_page_id {
            return Err(anyhow!(
                "page {} not allocated (next_page_id={})",
                page_id,
                self.meta.next_page_id
            ));
        }
        let (seg_no, off) = self.locate(page_id);
        let mut f = self.open_seg_rw(seg_no, false)?;
        let need_len = off + self.meta.page_size as u64;
        let cur_len = f.metadata()?.len();
        if cur_len < need_len {
            return Err(anyhow!(
                "segment too short ({} < {}), page not allocated?",
                cur_len,
                need_len
            ));
        }
        write_at(&mut f, off, buf)?;
        if self.data_fsync {
            f.sync_all()?;
        }

        // Обновим кэш (если включён).
        if let Some(cache) = self.cache.borrow_mut().as_mut() {
            cache.put(page_id, buf);
        }
        Ok(())
    }

    /// Запись через WAL с LSN:
    /// Порядок:
    /// 1) Если есть активные снапшоты — заморозить текущую страницу (COW по LSN).
    /// 2) Присваиваем новый LSN (meta.last_lsn + 1).
    /// 3) Если это v2-страница — вписываем LSN в заголовок (RH/Overflow).
    /// 4) Обновляем CRC (v2).
    /// 5) append + fsync WAL (с коалессированием).
    /// 6) Пишем страницу в сегмент.
    /// 7) Обновляем meta.last_lsn и (опционально) ротация WAL.
    /// 8) Обновляем кэш.
    pub fn commit_page(&mut self, page_id: u64, buf: &mut [u8]) -> Result<()> {
        if buf.len() != self.meta.page_size as usize {
            return Err(anyhow!(
                "buffer size {} != page_size {}",
                buf.len(),
                self.meta.page_size
            ));
        }

        // [1] COW: если есть активные снапшоты, заморозим текущую страницу (её "старую" версию),
        // но только если страница уже существует и это v2-страница с корректным LSN.
        if let Some(mgr) = &self.snap_mgr {
            if page_id < self.meta.next_page_id {
                let mut cur = vec![0u8; self.meta.page_size as usize];
                if self.read_page(page_id, &mut cur).is_ok() {
                    if &cur[..4] == PAGE_MAGIC {
                        let ver = LittleEndian::read_u16(&cur[4..6]);
                        if ver >= 2 {
                            // Попробуем распарсить LSN из RH/OVF
                            let cur_lsn_opt = if rh_header_read(&cur).is_ok() {
                                rh_header_read(&cur).ok().map(|h| h.lsn)
                            } else if ovf_header_read(&cur).is_ok() {
                                ovf_header_read(&cur).ok().map(|h| h.lsn)
                            } else {
                                None
                            };
                            if let Some(cur_lsn) = cur_lsn_opt {
                                // freeze_if_needed для всех активных снапшотов (по их правилам)
                                let mut g = mgr.lock().unwrap();
                                let _ = g.freeze_if_needed(page_id, cur_lsn, &cur);
                            }
                        }
                    }
                }
            }
        }

        // [2] Новый LSN для записи
        let lsn = self.meta.last_lsn.wrapping_add(1);

        // Если это наша страница — для v2 впишем LSN в заголовок, затем посчитаем CRC.
        if buf.len() >= 8 && &buf[..4] == PAGE_MAGIC {
            let ver = LittleEndian::read_u16(&buf[4..6]);

            // v2: впишем lsn в заголовок, чтобы payload в WAL уже содержал актуальный lsn.
            if ver >= 2 {
                let ptype = LittleEndian::read_u16(&buf[6..8]);
                match ptype {
                    t if t == PAGE_TYPE_KV_RH => {
                        let mut h = rh_header_read(buf)?;
                        h.lsn = lsn;
                        rh_header_write(buf, &h)?;
                        rh_page_update_crc(buf)?;
                    }
                    t if t == PAGE_TYPE_OVERFLOW => {
                        let mut h = ovf_header_read(buf)?;
                        h.lsn = lsn;
                        ovf_header_write(buf, &h)?;
                        rh_page_update_crc(buf)?;
                    }
                    _ => {
                        // неизвестный тип v2 — CRC посчитаем на всякий
                        rh_page_update_crc(buf)?;
                    }
                }
            }
        }

        // [5] Append WAL (содержит lsn) + fsync
        let mut wal = Wal::open_for_append(&self.root)?;
        wal.append_page_image(lsn, page_id, buf)?;
        wal.fsync()?; // group-commit коалессирует fsync внутри

        // [6] Пишем страницу
        self.write_page_raw(page_id, buf)?;

        // [7] WAL-rotate (truncate) только если данные fsync'нуты.
        if self.data_fsync {
            wal.maybe_truncate()?; // если WAL разросся, очистим до заголовка
        }

        // [7b] Зафиксируем last_lsn в meta (best-effort).
        self.meta.last_lsn = lsn;
        write_meta_overwrite(&self.root, &self.meta)?;

        // [8] Обновим кэш
        if let Some(cache) = self.cache.borrow_mut().as_mut() {
            cache.put(page_id, buf);
        }
        Ok(())
    }

    pub fn read_page(&self, page_id: u64, buf: &mut [u8]) -> Result<()> {
        if buf.len() != self.meta.page_size as usize {
            return Err(anyhow!(
                "buffer size {} != page_size {}",
                buf.len(),
                self.meta.page_size
            ));
        }
        if page_id >= self.meta.next_page_id {
            return Err(anyhow!(
                "page {} not allocated (next_page_id={})",
                page_id,
                self.meta.next_page_id
            ));
        }

        // Попытка из кэша (обновляет last_access).
        {
            if let Some(cache) = self.cache.borrow_mut().as_mut() {
                if cache.get_mut(page_id, buf) {
                    record_cache_hit();
                    return Ok(());
                } else {
                    record_cache_miss();
                }
            }
        }

        let (seg_no, off) = self.locate(page_id);
        let mut f = self.open_seg_rw(seg_no, false)?;
        let need_len = off + self.meta.page_size as u64;
        let cur_len = f.metadata()?.len();
        if cur_len < need_len {
            return Err(anyhow!(
                "segment too short ({} < {}), page not allocated?",
                cur_len,
                need_len
            ));
        }
        read_at(&mut f, off, buf)?;

        // Проверим CRC только если страница имеет наш magic и это v2.
        if buf.len() >= 8 && &buf[..4] == PAGE_MAGIC {
            let ver = LittleEndian::read_u16(&buf[4..6]);
            if ver >= 2 {
                let ok = rh_page_verify_crc(buf)?;
                if !ok {
                    return Err(anyhow!(
                        "page {} CRC mismatch (possible corruption)",
                        page_id
                    ));
                }
            }
        }

        // Положим в кэш (если включён).
        if let Some(cache) = self.cache.borrow_mut().as_mut() {
            cache.put(page_id, buf);
        }
        Ok(())
    }
}