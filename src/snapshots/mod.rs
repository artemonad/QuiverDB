//! Snapshot Isolation (Phase 1).
//!
//! Что уже есть:
//! - SnapshotManager / SnapshotHandle (begin/end, sidecar-директория).
//! - Запись "заморозки" страниц в sidecar (freeze.bin + index.bin) через
//!   SnapshotManager::freeze_if_needed(page_id, page_lsn, page_bytes).
//! - Метрики: record_snapshot_begin/end, record_snapshot_freeze_frame.
//!
//! В этом файле реализовано чтение под снапшотом (Phase 1):
//! - SnapshotHandle::get/scan_all/scan_prefix читают консистентный срез по snapshot_lsn.
//! - Если live-страница новее (live_lsn > snapshot_lsn) — читаем frozen-копию из sidecar.
//! - Аналогично для overflow-цепочек: каждое звено берётся как-of snapshot_lsn (live или frozen).
//! - Индекс freeze (index.bin) lazily загружается в SnapshotHandle (in-memory cache) и
//!   при необходимости перечитывается один раз при промахе.
//!
//! Дополнительно (устойчивость при горячих мутациях цепочек):
//! - Если обычный обход цепочки от live head не находит ключ (например, head уже указывает
//!   на «новую» страницу, недостижимую от старой), используется fallback-скан по всем
//!   страницам as-of snapshot_lsn с вычислением tail-wins по snapshot next_page_id.
//!
//! RAW FALLBACK (новое):
//! - Если live-страница выглядит как v2 (MAGIC + ver>=2), но парсинг RH/OVF-хедера не удался,
//!   под снапшотом сначала пробуем frozen-копию (с одним refresh индекса), и если её нет —
//!   возвращаем live как есть (best-effort). Это согласовано с write-path fallback freeze
//!   (commit_page замораживает «сырую» v2-страницу с page_lsn=0 перед overwrite/free).

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use std::cell::RefCell;
use std::collections::{HashMap, HashSet};
use std::fs::{self, OpenOptions};
use std::io::{Read, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::consts::{NO_PAGE, PAGE_HDR_V2_SIZE, PAGE_MAGIC};
use crate::db::Db;
use crate::dir::Directory;
use crate::metrics::{record_snapshot_begin, record_snapshot_end, record_snapshot_freeze_frame};
use crate::pager::Pager;
use crate::page_ovf::ovf_header_read;
use crate::page_rh::{rh_header_read, rh_kv_list, rh_kv_lookup, rh_page_is_kv};

/// Идентификатор снапшота (Phase 1: строка вида "<pid>-<ts>-<ctr>")
pub type SnapshotId = String;

/// Внутреннее состояние одного снапшота (Phase 1).
#[derive(Debug)]
struct SnapshotState {
    pub id: SnapshotId,
    pub lsn: u64,
    pub freeze_dir: PathBuf,        // <root>/.snapshots/<id>/
    pub frozen_pages: HashSet<u64>, // страницы, которые уже «заморожены» для снапшота
    pub ended: bool,
    // In-memory индекс (page_id -> offset) для freeze.bin (дополняется по мере записи/чтения)
    pub index_offsets: HashMap<u64, u64>,
}

impl SnapshotState {
    fn freeze_path(&self) -> PathBuf {
        self.freeze_dir.join("freeze.bin")
    }
    fn index_path(&self) -> PathBuf {
        self.freeze_dir.join("index.bin")
    }
}

/// Менеджер снапшотов (in-process).
#[derive(Debug)]
pub struct SnapshotManager {
    root: PathBuf,
    next_ctr: u64,
    // приватно (наружу этот state не нужен)
    active: HashMap<SnapshotId, SnapshotState>,
    pub max_snapshot_lsn: u64, // максимум lsn по активным снапшотам
}

impl SnapshotManager {
    /// Создать менеджер для корня БД (не создаёт директорий).
    pub fn new(root: &Path) -> Self {
        Self {
            root: root.to_path_buf(),
            next_ctr: 1,
            active: HashMap::new(),
            max_snapshot_lsn: 0,
        }
    }

    /// Создать новый снапшот на текущем last_lsn. Возвращает SnapshotHandle.
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

        // Директория sidecar
        let freeze_dir = self.root.join(".snapshots").join(&id);
        fs::create_dir_all(&freeze_dir)
            .with_context(|| format!("create snapshot dir {}", freeze_dir.display()))?;

        // Регистрация
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

        record_snapshot_begin();

        Ok(SnapshotHandle {
            root: self.root.clone(),
            id,
            lsn,
            freeze_dir,
            ended: false,
            index_cache: RefCell::new(None),
        })
    }

    /// Завершить снапшот (удаляет sidecar).
    pub fn end(&mut self, id: &str) -> Result<()> {
        if let Some(st) = self.active.get_mut(id) {
            if !st.ended {
                st.ended = true;
                let _ = fs::remove_dir_all(&st.freeze_dir);
            }
            self.active.remove(id);
            self.recompute_max_lsn();

            record_snapshot_end();
            Ok(())
        } else {
            Err(anyhow!("snapshot '{}' is not active", id))
        }
    }

    /// Пересчитать max_snapshot_lsn.
    fn recompute_max_lsn(&mut self) {
        self.max_snapshot_lsn = self.active.values().map(|s| s.lsn).max().unwrap_or(0);
    }

    /// Отметить, что страница «заморожена» для данного снапшота.
    pub fn mark_frozen(&mut self, id: &str, page_id: u64) {
        if let Some(st) = self.active.get_mut(id) {
            st.frozen_pages.insert(page_id);
        }
    }

    /// Проверить, нужна ли страница снапшоту (заморожена ли уже).
    pub fn is_frozen(&self, id: &str, page_id: u64) -> bool {
        self.active
            .get(id)
            .map(|s| s.frozen_pages.contains(&page_id))
            .unwrap_or(false)
    }

    /// freeze_if_needed: если у какого-либо активного снапшота snapshot_lsn >= page_lsn
    /// и ещё нет копии — записать кадр в freeze.bin + index.bin.
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

        for id in to_freeze {
            if let Some(st) = self.active.get_mut(&id) {
                let offset = write_freeze_frame(st, page_id, page_lsn, page_bytes)
                    .with_context(|| format!("write freeze frame for snapshot {}", st.id))?;
                record_snapshot_freeze_frame(page_bytes.len());
                st.index_offsets.insert(page_id, offset);
                st.frozen_pages.insert(page_id);
            }
        }

        Ok(())
    }
}

/// RAII-хэндл снапшота (Phase 1).
#[derive(Debug)]
pub struct SnapshotHandle {
    root: PathBuf,
    pub id: SnapshotId,
    pub lsn: u64,
    pub(crate) freeze_dir: PathBuf,
    ended: bool,
    // Lazy-кэш индекса: page_id -> offset (freeze.bin). Заполняется лениво.
    index_cache: RefCell<Option<HashMap<u64, u64>>>,
}

impl SnapshotHandle {
    /// LSN снапшота.
    pub fn lsn(&self) -> u64 {
        self.lsn
    }

    /// Получить значение ключа в контексте снапшота (консистентный view по snapshot_lsn).
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

            // Устойчивость: если не KV — прерываем цепочку (не роняем чтение).
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

    /// Скан всей БД в контексте снапшота.
    pub fn scan_all(&self) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix_impl(None)
    }

    /// Скан по префиксу в контексте снапшота.
    pub fn scan_prefix(&self, prefix: &[u8]) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        self.scan_prefix_impl(Some(prefix))
    }

    /// Завершить снапшот (удаляет sidecar).
    pub fn end(&mut self) -> Result<()> {
        if self.ended {
            return Ok(());
        }
        let _ = fs::remove_dir_all(&self.freeze_dir);
        self.ended = true;
        Ok(())
    }

    // ----------------- внутренние помощники -----------------

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

    /// Выбрать байты страницы pid как-of snapshot_lsn:
    /// - RH: если live.lsn ≤ snapshot → live; иначе frozen (с одним refresh).
    /// - OVF: если live.lsn ≤ snapshot → live; иначе frozen (с одним refresh).
    /// - RAW FALLBACK: если live выглядит как v2, но хедер не читается → frozen (try), затем live.
    /// - non-v2: live.
    /// - ошибка чтения live: пытаемся frozen.
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
                let is_v2 = buf.len() >= 8 && &buf[..4] == PAGE_MAGIC && LittleEndian::read_u16(&buf[4..6]) >= 2;

                // RH (нормальный путь)
                if let Ok(h) = rh_header_read(&buf) {
                    if h.lsn <= self.lsn {
                        return Ok(Some(buf));
                    }
                    if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, false)? {
                        return Ok(Some(bytes));
                    }
                    if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, true)? {
                        return Ok(Some(bytes));
                    }
                    return Ok(None);
                }
                // OVF (нормальный путь)
                if let Ok(hovf) = ovf_header_read(&buf) {
                    if hovf.lsn <= self.lsn {
                        return Ok(Some(buf));
                    }
                    if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, false)? {
                        return Ok(Some(bytes));
                    }
                    if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, true)? {
                        return Ok(Some(bytes));
                    }
                    return Ok(None);
                }

                // RAW FALLBACK: выглядит как v2, но хедер не распознан — сначала frozen, потом live.
                if is_v2 {
                    if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, false)? {
                        return Ok(Some(bytes));
                    }
                    if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, true)? {
                        return Ok(Some(bytes));
                    }
                    // frozen не нашли — вернём live как есть (best-effort)
                    return Ok(Some(buf));
                }

                // Не v2 — используем live
                Ok(Some(buf))
            }
            Err(_) => {
                if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, false)? {
                    return Ok(Some(bytes));
                }
                if let Some(bytes) = self.read_frozen_page_cached(page_id, page_size, true)? {
                    return Ok(Some(bytes));
                }
                Ok(None)
            }
        }
    }

    /// Прочитать overflow-цепочку как-of snapshot_lsn.
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

    /// Прочитать frozen-страницу из кэша; при refresh_index=true пересобираем индекс перед попыткой.
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
        let idx = idx_ref.as_ref().unwrap(); // к этому моменту Some

        let off = match idx.get(&page_id) {
            Some(&o) => o,
            None => return Ok(None),
        };
        read_frozen_page_at_offset(&self.freeze_dir, off, page_id, page_size)
    }

    /// Fallback: найти ключ по полному скану страниц as-of snapshot_lsn (дорого, но безопасно).
    fn fallback_get_by_scan(&self, pager: &Pager, key: &[u8]) -> Result<Option<Vec<u8>>> {
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

        fn tail_from(
            start: u64,
            contains: &HashSet<u64>,
            next_map: &HashMap<u64, u64>,
        ) -> u64 {
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
                        // Предпочтём тот, до которого можно дойти из другого
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
}

impl Drop for SnapshotHandle {
    fn drop(&mut self) {
        if !self.ended {
            let _ = fs::remove_dir_all(&self.freeze_dir);
        }
    }
}

/// Утилита: корень sidecar-директории снапшотов.
pub fn snapshots_root(root: &Path) -> PathBuf {
    root.join(".snapshots")
}

// ====== Внутренние функции записи freeze/index ======

fn write_freeze_frame(
    st: &mut SnapshotState,
    page_id: u64,
    page_lsn: u64,
    page_bytes: &[u8],
) -> Result<u64> {
    // freeze.bin (append-open)
    let freeze_path = st.freeze_path();
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
    std::io::Seek::seek(&mut f, SeekFrom::End(0))?;
    f.write_all(&hdr)?;
    f.write_all(page_bytes)?;

    // index.bin (append-open)
    let index_path = st.index_path();
    let mut idx = OpenOptions::new()
        .create(true)
        .write(true)
        .read(true)
        .open(&index_path)
        .with_context(|| format!("open index {}", index_path.display()))?;
    std::io::Seek::seek(&mut idx, SeekFrom::End(0))?;

    // Index entry: [page_id u64][offset u64][page_lsn u64]
    let mut entry = vec![0u8; 8 + 8 + 8];
    LittleEndian::write_u64(&mut entry[0..8], page_id);
    LittleEndian::write_u64(&mut entry[8..16], offset as u64);
    LittleEndian::write_u64(&mut entry[16..24], page_lsn);
    idx.write_all(&entry)?;

    Ok(offset as u64)
}

// ====== Внутренние функции чтения freeze/index ======

fn build_freeze_index(freeze_dir: &Path) -> Result<HashMap<u64, u64>> {
    let mut map = HashMap::new();
    let idx_path = freeze_dir.join("index.bin");
    if !idx_path.exists() {
        return Ok(map);
    }
    let mut f = OpenOptions::new()
        .read(true)
        .open(&idx_path)
        .with_context(|| format!("open {}", idx_path.display()))?;
    let mut pos = 0u64;
    let len = f.metadata()?.len();

    // Каждая запись index: [page_id u64][offset u64][page_lsn u64] = 24 байта
    const REC: u64 = 8 + 8 + 8;
    let mut buf = vec![0u8; REC as usize];
    while pos + REC <= len {
        std::io::Seek::seek(&mut f, SeekFrom::Start(pos))?;
        f.read_exact(&mut buf)?;
        let page_id = LittleEndian::read_u64(&buf[0..8]);
        let off = LittleEndian::read_u64(&buf[8..16]);
        // let _lsn = LittleEndian::read_u64(&buf[16..24]);
        map.insert(page_id, off);
        pos += REC;
    }
    Ok(map)
}

fn read_frozen_page_at_offset(
    freeze_dir: &Path,
    off: u64,
    page_id: u64,
    page_size: usize,
) -> Result<Option<Vec<u8>>> {
    let path = freeze_dir.join("freeze.bin");
    if !path.exists() {
        return Ok(None);
    }
    let mut f = OpenOptions::new()
        .read(true)
        .open(&path)
        .with_context(|| format!("open {}", path.display()))?;

    // Header: [page_id u64][page_lsn u64][page_len u32][crc32 u32]
    let mut hdr = [0u8; 8 + 8 + 4 + 4];
    std::io::Seek::seek(&mut f, SeekFrom::Start(off))?;
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