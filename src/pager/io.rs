//! pager/io — низкоуровневые операции ввода/вывода страниц:
//! - ensure_allocated: гарантирует, что страница физически аллоцирована
//! - read_page: чтение + проверка трейлера (CRC32C по умолчанию; AES‑GCM при TDE)
//!   с процессным page cache
//! - write_page_raw: запись + (опциональный) fsync данных сегмента
//! - free_page: поместить page_id в free‑лист (минимальная реализация 2.0)
//! - prefetch_page — прогревает страницу в процессный page cache
//!
//! Совместимость TDE:
//! - Если TDE включён, для прочитанной страницы сначала проверяется AEAD‑тег.
//!   Если он не проходит, в STRICT‑режиме — ошибка.
//!   В non‑strict: CRC‑fallback теперь допускается ТОЛЬКО для страниц, чей page_lsn
//!   меньше начала текущей TDE‑эпохи (since_lsn) из KeyJournal.
//!   Для страниц с lsn >= since_lsn CRC‑fallback запрещён (epoch‑aware запрет).
//!
//! NEW:
//! - Глобальный page cache вынесен в модуль pager::cache. Этот файл использует его API:
//!   page_cache_get/put/invalidate/init и тонкие обёртки configure.
//! - «Strict» режим нулевого трейлера CRC: ENV P1_ZERO_CHECKSUM_STRICT=1|true|yes|on.
//! - «Strict» режим для TDE (без CRC‑fallback): ENV P1_TDE_STRICT=1|true|yes|on.
//! - По умолчанию не кэшируем OVERFLOW‑страниц; включить можно ENV P1_PAGE_CACHE_OVF=1.
//! - Метрика cache_miss инкрементируется только для страниц, которые потенциально кэшируются.
//! - NEW: Строгий запрет чтения “за хвост” логической аллокации: P1_READ_BEYOND_ALLOC_STRICT=1.

use anyhow::{anyhow, Result};
use std::io::{Read, Seek, SeekFrom, Write};

// Нужен для чтения lsn из заголовка (KV/Ovf)
use byteorder::{ByteOrder, LittleEndian};

use crate::crypto::KeyJournal;
use crate::free::FreeList;
use crate::metrics::{record_cache_hit, record_cache_miss};
use crate::page::{
    page_trailer_is_zero_crc32,
    page_verify_checksum,
    page_verify_trailer_aead_with,
    verify_page_crc_strict_kind, // Единая строгая CRC‑проверка из модуля page
    KV_OFF_LSN,
    OFF_TYPE,
    OVF_OFF_LSN,
    PAGE_MAGIC,
    PAGE_TYPE_KV_RH3,
    PAGE_TYPE_OVERFLOW3,
}; // NEW: Journal для epoch‑aware TDE fallback

use super::core::Pager;

// NEW: подключаем процессный кэш
use crate::pager::cache::{
    page_cache_configure as pc_configure_impl, page_cache_get as pc_get,
    page_cache_init as pc_init, page_cache_invalidate as pc_invalidate, page_cache_put as pc_put,
};

// --------------------------- Strict flags (dynamic) ---------------------------

#[inline]
fn zero_cksum_strict() -> bool {
    std::env::var("P1_ZERO_CHECKSUM_STRICT")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

#[inline]
fn tde_strict() -> bool {
    std::env::var("P1_TDE_STRICT")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

// NEW: флаг кэширования OVERFLOW‑страниц (по умолчанию — выключено)
#[inline]
fn cache_ovf_enabled() -> bool {
    std::env::var("P1_PAGE_CACHE_OVF")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

// NEW: строгий запрет чтения страниц за пределом логической аллокации (page_id >= next_page_id)
#[inline]
fn read_beyond_alloc_strict() -> bool {
    std::env::var("P1_READ_BEYOND_ALLOC_STRICT")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

// --------------------------- Обёртки для совместимости ---------------------------

/// Programmatic configuration of the global page cache (process-wide).
/// Safe to call multiple times; switching page_size resets the cache contents.
/// Setting cap_pages=0 disables the cache.
pub(crate) fn page_cache_configure(page_size: usize, cap_pages: usize) {
    pc_configure_impl(page_size, cap_pages);
}

// ------------------------------------------------------------------------

impl Pager {
    /// Гарантировать, что page_id физически аллоцирован на диске.
    ///
    /// Правила:
    /// - Если page_id >= meta.next_page_id — выделяем до неё включительно (allocate_pages).
    /// - Иначе убеждаемся, что соответствующий сегмент имеет достаточную длину.
    pub fn ensure_allocated(&mut self, page_id: u64) -> Result<()> {
        let (seg_no, off) = self.locate(page_id);

        if page_id >= self.meta.next_page_id {
            let to_alloc = page_id + 1 - self.meta.next_page_id;
            self.allocate_pages(to_alloc)?;
            return Ok(());
        }

        // Убедимся, что сегмент физически достаточно длинный.
        let f = self.open_seg_rw(seg_no, true)?;
        let need_len = off + (self.meta.page_size as u64);
        let cur_len = f.metadata()?.len();
        if cur_len < need_len {
            f.set_len(need_len)?;
            if self.data_fsync {
                let _ = f.sync_all();
            }
        }
        Ok(())
    }

    /// Прочитать страницу в буфер.
    /// - TDE выключен: проверка CRC32/CRC32C (STRICT может запретить нулевой трейлер).
    /// - TDE включён: сначала проверка AES‑GCM тега; если не прошла —
    ///   STRICT → ошибка; non‑strict → epoch‑aware fallback:
    ///     * CRC‑fallback разрешён только если page_lsn < since_lsn текущей TDE‑эпохи (KeyJournal).
    ///     * Если page_lsn ≥ since_lsn — CRC‑fallback запрещён, возвращаем ошибку.
    pub fn read_page(&self, page_id: u64, buf: &mut [u8]) -> Result<()> {
        let ps = self.meta.page_size as usize;

        if buf.len() != ps {
            return Err(anyhow!(
                "buffer size {} != page_size {}",
                buf.len(),
                self.meta.page_size
            ));
        }

        let (seg_no, off) = self.locate(page_id);

        // Guard: чтение за пределом логической аллокации
        if page_id >= self.meta.next_page_id {
            if read_beyond_alloc_strict() {
                return Err(anyhow!(
                    "page {} not allocated (next_page_id={})",
                    page_id,
                    self.meta.next_page_id
                ));
            }
            // В нестрогом режиме допустим “lenient read” при достаточно длинном сегменте:
            let seg_path = self.seg_path(seg_no);
            let seg_len = std::fs::metadata(&seg_path).map(|m| m.len()).unwrap_or(0);
            let need_len = off + (self.meta.page_size as u64);
            if seg_len < need_len {
                return Err(anyhow!(
                    "page {} not allocated (next_page_id={}), seg_len={}, need={}",
                    page_id,
                    self.meta.next_page_id,
                    seg_len,
                    need_len
                ));
            }
        }

        // TDE guard: читать можно; если ключ не загружен, читать нельзя — слой выше должен сделать ensure_tde_key().
        if self.tde_enabled && self.tde_key.is_none() {
            return Err(anyhow!(
                "TDE is enabled but key is not loaded; ensure pager.ensure_tde_key() on open"
            ));
        }

        // Попытка кэш-хита
        if let Some(src) = pc_get(self.db_id, page_id, ps) {
            debug_assert_eq!(src.len(), buf.len());
            (&mut *buf).copy_from_slice(&src);
            record_cache_hit();
            return Ok(());
        }

        // Miss: читаем с диска
        let mut f = self.open_seg_rw(seg_no, false)?;
        f.seek(SeekFrom::Start(off))?;
        f.read_exact(buf)?;

        // Верификация трейлера (TDE-aware)
        if self.tde_enabled {
            if &buf[0..4] != PAGE_MAGIC {
                return Err(anyhow!("bad page magic on TDE read"));
            }
            let ptype = LittleEndian::read_u16(&buf[OFF_TYPE..OFF_TYPE + 2]);
            let lsn = match ptype {
                t if t == PAGE_TYPE_KV_RH3 => {
                    LittleEndian::read_u64(&buf[KV_OFF_LSN..KV_OFF_LSN + 8])
                }
                t if t == PAGE_TYPE_OVERFLOW3 => {
                    LittleEndian::read_u64(&buf[OVF_OFF_LSN..OVF_OFF_LSN + 8])
                }
                _ => return Err(anyhow!("unsupported page type {} on TDE read", ptype)),
            };
            let key = self
                .tde_key
                .as_ref()
                .ok_or_else(|| anyhow!("TDE key missing"))?;
            let ok_aead = page_verify_trailer_aead_with(buf, key, page_id, lsn)?;
            if !ok_aead {
                // Жёсткий режим — сразу ошибка
                if tde_strict() {
                    return Err(anyhow!(
                        "page {} AEAD tag verify failed (TDE strict mode, lsn={})",
                        page_id,
                        lsn
                    ));
                }

                // Epoch-aware запрет CRC‑fallback: если страница “внутри” текущей TDE‑эпохи —
                // fallback запрещён.
                if let Some(epoch_since) = self.tde_current_epoch_since() {
                    if epoch_since > 0 && lsn >= epoch_since {
                        return Err(anyhow!(
                            "page {} AEAD verify failed; CRC fallback disabled for current TDE epoch (lsn={}, epoch_since={})",
                            page_id, lsn, epoch_since
                        ));
                    }
                }

                // Разрешённый fallback (для исторических страниц до эпохи) — строгая CRC‑проверка
                let ok_crc = verify_page_crc_strict_kind(buf, self.meta.checksum_kind)?;
                if !ok_crc {
                    return Err(anyhow!(
                        "page {} AEAD/CRC verify failed (lsn={}, fallback)",
                        page_id,
                        lsn
                    ));
                }
            }
        } else {
            // CRC (2.0)
            if zero_cksum_strict() {
                if page_trailer_is_zero_crc32(buf)? {
                    return Err(anyhow!(
                        "page {} zero checksum trailer (strict mode enabled)",
                        page_id
                    ));
                }
            }
            let ok = page_verify_checksum(buf, self.meta.checksum_kind)?;
            if !ok {
                return Err(anyhow!("page {} checksum mismatch", page_id));
            }
        }

        // Решим, стоит ли кэшировать страницу (по типу)
        let mut cache_ok = false;
        if buf.len() >= 12 && &buf[0..4] == PAGE_MAGIC {
            let ptype = LittleEndian::read_u16(&buf[OFF_TYPE..OFF_TYPE + 2]);
            cache_ok = match ptype {
                t if t == PAGE_TYPE_OVERFLOW3 => cache_ovf_enabled(),
                t if t == PAGE_TYPE_KV_RH3 => true,
                _ => false,
            };
        }

        if cache_ok {
            pc_put(self.db_id, page_id, buf, ps);
            record_cache_miss();
        }

        Ok(())
    }

    /// Низкоуровневая запись страницы «как есть» в сегмент.
    /// fsync данных выполняется только если self.data_fsync == true.
    pub fn write_page_raw(&mut self, page_id: u64, buf: &[u8]) -> Result<()> {
        self.write_page_raw_with_fsync(page_id, buf, true)
    }

    /// Вариант записи с управлением fsync данными (используется батч‑коммитом).
    /// Если do_fsync=false — только записывает; fsync данных должен сделать вызывающий код.
    pub(crate) fn write_page_raw_with_fsync(
        &mut self,
        page_id: u64,
        buf: &[u8],
        do_fsync: bool,
    ) -> Result<()> {
        let ps = self.meta.page_size as usize;

        if buf.len() != ps {
            return Err(anyhow!(
                "buffer size {} != page_size {}",
                buf.len(),
                self.meta.page_size
            ));
        }

        self.ensure_allocated(page_id)?;

        let (seg_no, off) = self.locate(page_id);
        let mut f = self.open_seg_rw(seg_no, false)?;
        f.seek(SeekFrom::Start(off))?;
        f.write_all(buf)?;
        if do_fsync && self.data_fsync {
            let _ = f.sync_all();
        }

        pc_invalidate(self.db_id, page_id, ps);

        Ok(())
    }

    /// Поместить страницу в free‑лист (минимальная реализация 2.0).
    /// Замечания:
    /// - Не зануляет данные страницы на диске; только добавляет page_id в `<root>/free`.
    /// - Вызовы должны происходить в writer‑контексте (внешняя синхронизация на уровне Db/lock).
    pub fn free_page(&self, page_id: u64) -> Result<()> {
        if page_id >= self.meta.next_page_id {
            return Ok(());
        }
        let fl = match FreeList::open(&self.root) {
            Ok(fl) => fl,
            Err(_) => FreeList::create(&self.root)?,
        };
        fl.push(page_id)
    }

    /// Префетч страницы в процессный cache (best-effort).
    /// Если страница не аллоцирована или TDE ключ отсутствует — возвращает Err,
    /// который можно игнорировать в вызывающем коде.
    pub fn prefetch_page(&self, page_id: u64) -> Result<()> {
        if self.tde_enabled && self.tde_key.is_none() {
            return Err(anyhow!(
                "TDE is enabled but key is not loaded; prefetch disabled until key is available"
            ));
        }
        if page_id >= self.meta.next_page_id {
            let (seg_no, off) = self.locate(page_id);
            let seg_path = self.seg_path(seg_no);
            let seg_len = std::fs::metadata(&seg_path).map(|m| m.len()).unwrap_or(0);
            let need_len = off + (self.meta.page_size as u64);
            if seg_len < need_len {
                return Ok(());
            }
        }
        pc_init(self.meta.page_size as usize);

        let mut tmp = vec![0u8; self.meta.page_size as usize];
        let _ = self.read_page(page_id, &mut tmp)?;
        Ok(())
    }

    // --------- NEW: helper для epoch‑aware TDE fallback ---------

    /// Возвращает since_lsn текущей (последней) TDE‑эпохи из KeyJournal, если журнал есть и не пустой.
    fn tde_current_epoch_since(&self) -> Option<u64> {
        match KeyJournal::open(&self.root) {
            Ok(j) => {
                if let Ok(eps) = j.epochs() {
                    eps.last().map(|(since, _kid)| *since)
                } else {
                    None
                }
            }
            Err(_) => None,
        }
    }
}
