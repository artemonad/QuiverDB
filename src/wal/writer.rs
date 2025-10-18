use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use std::io::{Seek, SeekFrom};
use std::path::Path;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::sync::OnceLock;

use crate::metrics::{
    record_wal_append,
    record_wal_flushed_lsn,
    record_wal_fsync,
    record_wal_pending_lsn,
    // NEW: threshold flush metric
    record_wal_threshold_flush,
    record_wal_truncation,
};

use super::{
    WAL_HDR_SIZE, WAL_REC_BEGIN, WAL_REC_COMMIT, WAL_REC_HDR_SIZE, WAL_REC_HEADS_UPDATE,
    WAL_REC_PAGE_IMAGE, WAL_REC_TRUNCATE, WAL_ROTATE_SIZE,
};

use super::encode;
use super::registry::{get_or_create_wal_inner, set_group_coalesce_ms, WalInner};

#[derive(Debug, Clone, Copy)]
pub struct WalGroupCfg {
    pub coalesce_ms: u64,
}

pub struct Wal {
    inner: Arc<WalInner>,
    // NEW: подавление пороговых fsync'ов во время батча
    in_batch: bool,
}

impl Wal {
    pub fn open_for_append(root: &Path) -> Result<Self> {
        let inner = get_or_create_wal_inner(root)?;
        Ok(Self {
            inner,
            in_batch: false,
        })
    }

    pub fn set_group_config(root: &Path, cfg: WalGroupCfg) -> Result<()> {
        set_group_coalesce_ms(root, cfg.coalesce_ms)
    }

    // NEW: сигнализировать писателю, что начался "ручной" батч (BEGIN..COMMIT)
    #[inline]
    pub fn start_batch(&mut self) {
        self.in_batch = true;
    }

    // NEW: конец батча — пороговые fsync можно снова применять
    #[inline]
    pub fn end_batch(&mut self) {
        self.in_batch = false;
    }

    #[inline]
    fn write_record(&mut self, rec_type: u8, lsn: u64, page_id: u64, payload: &[u8]) -> Result<()> {
        // MutexGuard<File> -> &mut File для impl Write + Seek
        let mut guard = self.inner.file.lock().unwrap();
        encode::write_record(&mut *guard, rec_type, lsn, page_id, payload)
    }

    pub fn append_page_image(&mut self, lsn: u64, page_id: u64, page: &[u8]) -> Result<()> {
        self.write_record(WAL_REC_PAGE_IMAGE, lsn, page_id, page)?;
        // учёт метрик/счётчиков
        record_wal_append(WAL_REC_HDR_SIZE + page.len());
        self.inner
            .pages_since_last_fsync
            .fetch_add(1, Ordering::Relaxed);
        self.inner
            .bytes_since_last_fsync
            .fetch_add((WAL_REC_HDR_SIZE + page.len()) as u64, Ordering::Relaxed);

        {
            let mut st = self.inner.flush.lock().unwrap();
            if lsn > st.pending_max_lsn {
                st.pending_max_lsn = lsn;
                record_wal_pending_lsn(lsn);
                self.inner.cv.notify_all();
            }
        }

        // Пороговые fsync вне батча
        self.maybe_flush_by_threshold()?;
        Ok(())
    }

    pub fn append_begin(&mut self, lsn: u64) -> Result<()> {
        self.write_record(WAL_REC_BEGIN, lsn, 0, &[])?;
        // учёт байтов
        self.inner
            .bytes_since_last_fsync
            .fetch_add(WAL_REC_HDR_SIZE as u64, Ordering::Relaxed);
        // вне батча — можно флашить по порогу
        self.maybe_flush_by_threshold()?;
        Ok(())
    }

    pub fn append_commit(&mut self, lsn: u64) -> Result<()> {
        self.write_record(WAL_REC_COMMIT, lsn, 0, &[])?;
        // учёт байтов
        self.inner
            .bytes_since_last_fsync
            .fetch_add(WAL_REC_HDR_SIZE as u64, Ordering::Relaxed);
        {
            let mut st = self.inner.flush.lock().unwrap();
            if lsn > st.pending_max_lsn {
                st.pending_max_lsn = lsn;
                self.inner.cv.notify_all();
            }
        }
        record_wal_pending_lsn(lsn);

        // вне батча — можно флашить по порогу
        self.maybe_flush_by_threshold()?;
        Ok(())
    }

    pub fn append_truncate_marker(&mut self) -> Result<()> {
        self.write_record(WAL_REC_TRUNCATE, 0, 0, &[])?;
        // учёт байтов
        self.inner
            .bytes_since_last_fsync
            .fetch_add(WAL_REC_HDR_SIZE as u64, Ordering::Relaxed);
        // вне батча — можно флашить по порогу
        self.maybe_flush_by_threshold()?;
        Ok(())
    }

    pub fn append_heads_update(&mut self, lsn: u64, updates: &[(u32, u64)]) -> Result<()> {
        if updates.is_empty() {
            return Ok(());
        }
        let mut payload = Vec::with_capacity(updates.len() * 12);
        let mut tmp4 = [0u8; 4];
        let mut tmp8 = [0u8; 8];
        for &(b, pid) in updates {
            LittleEndian::write_u32(&mut tmp4, b);
            payload.extend_from_slice(&tmp4);
            LittleEndian::write_u64(&mut tmp8, pid);
            payload.extend_from_slice(&tmp8);
        }
        self.write_record(WAL_REC_HEADS_UPDATE, lsn, 0, &payload)?;
        // учёт байтов
        self.inner
            .bytes_since_last_fsync
            .fetch_add((WAL_REC_HDR_SIZE + payload.len()) as u64, Ordering::Relaxed);

        // вне батча — можно флашить по порогу
        self.maybe_flush_by_threshold()?;
        Ok(())
    }

    pub fn fsync(&mut self) -> Result<()> {
        // Быстрый режим: отключить физический fsync WAL (для бенчей/разработки).
        if wal_disable_fsync() {
            let pages_this = self.inner.pages_since_last_fsync.swap(0, Ordering::Relaxed);
            let _ = self.inner.bytes_since_last_fsync.swap(0, Ordering::Relaxed);
            let mut st = self.inner.flush.lock().unwrap();
            if st.pending_max_lsn > st.flushed_lsn {
                st.flushed_lsn = st.pending_max_lsn;
                self.inner.cv.notify_all();
                record_wal_fsync(pages_this);
                record_wal_flushed_lsn(st.flushed_lsn);
            }
            return Ok(());
        }

        // Обычный путь: один поток делает fsync, остальные ждут на cv.
        let mut st = self.inner.flush.lock().unwrap();
        if st.pending_max_lsn <= st.flushed_lsn {
            return Ok(());
        }
        let my_target = st.pending_max_lsn;
        if st.flushing {
            while st.flushed_lsn < my_target {
                st = self.inner.cv.wait(st).unwrap();
            }
            return Ok(());
        }
        st.flushing = true;
        drop(st);

        // Коалессация по времени (если задана)
        let ms = self.inner.coalesce_ms.load(Ordering::Relaxed);
        if ms > 0 {
            let guard = self.inner.flush.lock().unwrap();
            let _ = self
                .inner
                .cv
                .wait_timeout(guard, std::time::Duration::from_millis(ms))
                .unwrap();
        }

        let target = {
            let st2 = self.inner.flush.lock().unwrap();
            st2.pending_max_lsn
        };

        {
            let f = self.inner.file.lock().unwrap();
            f.sync_all()?;
        }

        let pages_this_fsync = self.inner.pages_since_last_fsync.swap(0, Ordering::Relaxed);
        let _ = self.inner.bytes_since_last_fsync.swap(0, Ordering::Relaxed);

        let mut st3 = self.inner.flush.lock().unwrap();
        let prev_flushed = st3.flushed_lsn;
        st3.flushed_lsn = st3.flushed_lsn.max(target);
        st3.flushing = false;
        self.inner.cv.notify_all();

        let _delta_lsn = st3.flushed_lsn.saturating_sub(prev_flushed);
        record_wal_fsync(pages_this_fsync);
        record_wal_flushed_lsn(st3.flushed_lsn);

        Ok(())
    }

    pub fn maybe_truncate(&mut self) -> Result<()> {
        let mut f = self.inner.file.lock().unwrap();
        let len = f.metadata()?.len();
        if len > WAL_ROTATE_SIZE {
            f.set_len(WAL_HDR_SIZE as u64)?;
            f.seek(SeekFrom::End(0))?;
            f.sync_all()?;
            record_wal_truncation();
        }
        Ok(())
    }

    pub fn truncate_to_header(&mut self) -> Result<()> {
        let mut f = self.inner.file.lock().unwrap();
        f.set_len(WAL_HDR_SIZE as u64)?;
        f.seek(SeekFrom::End(0))?;
        f.sync_all()?;
        record_wal_truncation();
        Ok(())
    }

    // NEW: пороговые fsync'и вне батча
    #[inline]
    fn maybe_flush_by_threshold(&mut self) -> Result<()> {
        if self.in_batch {
            return Ok(()); // батч сам сделает fsync в конце
        }
        let pg_thr = wal_flush_every_pages();
        let bt_thr = wal_flush_bytes();

        let mut do_flush = false;
        if pg_thr > 0 {
            let pages = self.inner.pages_since_last_fsync.load(Ordering::Relaxed);
            if pages >= pg_thr {
                do_flush = true;
            }
        }
        if !do_flush && bt_thr > 0 {
            let bytes = self.inner.bytes_since_last_fsync.load(Ordering::Relaxed);
            if bytes >= bt_thr {
                do_flush = true;
            }
        }

        if do_flush {
            // Зафиксируем текущие значения, из-за которых сработал порог,
            // до того как fsync() обнулит счётчики.
            let pages_now = self.inner.pages_since_last_fsync.load(Ordering::Relaxed);
            let bytes_now = self.inner.bytes_since_last_fsync.load(Ordering::Relaxed);

            // Выполняем fsync
            self.fsync()?;

            // И запишем метрику «threshold flush»
            record_wal_threshold_flush(pages_now, bytes_now);
        }
        Ok(())
    }
}

// ----------- helpers -----------

fn wal_disable_fsync() -> bool {
    static DISABLED: OnceLock<bool> = OnceLock::new();
    *DISABLED.get_or_init(|| {
        std::env::var("P1_WAL_DISABLE_FSYNC")
            .ok()
            .map(|s| s.trim().to_ascii_lowercase())
            .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
            .unwrap_or(false)
    })
}

// NEW: порог страниц до fsync (PAGE_IMAGE)
fn wal_flush_every_pages() -> u64 {
    static PAGES: OnceLock<u64> = OnceLock::new();
    *PAGES.get_or_init(|| {
        std::env::var("P1_WAL_FLUSH_EVERY")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0)
    })
}

// NEW: порог байт до fsync (заголовки + payload всех записей)
fn wal_flush_bytes() -> u64 {
    static BYTES: OnceLock<u64> = OnceLock::new();
    *BYTES.get_or_init(|| {
        std::env::var("P1_WAL_FLUSH_BYTES")
            .ok()
            .and_then(|s| s.trim().parse::<u64>().ok())
            .unwrap_or(0)
    })
}
