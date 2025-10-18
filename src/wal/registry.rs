//! wal/registry — реестр/шаринг WAL-состояний между аппендерами.
//!
//! Назначение:
//! - Один процесс может создавать несколько Wal‑хэндлов, которые разделяют один и тот же файл и
//!   коалесцируют fsync (group-commit).
//! - Реестр мапит путь WAL-файла -> Arc<WalInner> c общим состоянием (flush/condvar).
//!
//! Новое:
//! - stream_id (u64) — уникальный идентификатор потока WAL, записывается в header файла WAL (reserved).
//!   Нужен для предотвращения смешивания разных источников на стороне apply.
//!   Поведение:
//!   * Если файл пуст/мал — генерируем stream_id и пишем header (P2WAL001 + stream_id).
//!   * Если header есть и stream_id==0 (старый формат) — генерируем stream_id и переписываем header.
//!   * Если header валидный и stream_id!=0 — используем его.
//!
//! Публичный API (для использования из writer.rs):
//! - get_or_create_wal_inner(root) -> Arc<WalInner>
//! - set_group_coalesce_ms(root, ms) — установить окно коалессации fsync.

use anyhow::{Context, Result};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

use super::{
    generate_stream_id, wal_header_read_stream_id, wal_path, write_wal_file_header_with_stream_id,
    WAL_HDR_SIZE, WAL_MAGIC,
};

/// Состояние «в полёте» для коалессации fsync.
///
/// pending_max_lsn — максимальный LSN среди PAGE_IMAGE, записанных, но ещё не fsync’нутых.
/// flushed_lsn     — последний LSN, гарантированно fsync’нутый.
/// flushing        — признак, что один поток делает fsync, остальные ждут по cv.
pub struct FlushState {
    pub pending_max_lsn: u64,
    pub flushed_lsn: u64,
    pub flushing: bool,
}

/// Внутренняя структура WAL, разделяемая всеми Wal‑хэндлами одного файла.
pub struct WalInner {
    pub file: Mutex<std::fs::File>,
    pub flush: Mutex<FlushState>,
    pub cv: Condvar,

    // Окно коалессации fsync (мс): обновляется через set_group_coalesce_ms(...)
    pub coalesce_ms: AtomicU64,

    // Счётчик PAGE_IMAGE с момента последнего fsync (группа).
    pub pages_since_last_fsync: AtomicU64,

    // NEW: Сумма байт, записанных в WAL с момента последнего fsync (заголовки + payload).
    pub bytes_since_last_fsync: AtomicU64,

    // NEW: Уникальный идентификатор WAL‑потока (записан в header).
    pub stream_id: u64,
}

impl WalInner {
    fn new(mut file: std::fs::File) -> Result<Self> {
        // Убедимся, что валидный заголовок присутствует и получить/установить stream_id.
        let len = file.metadata()?.len();

        let stream_id = if len < WAL_HDR_SIZE as u64 {
            // Новый/пустой файл → генерируем stream_id и пишем header
            let sid = generate_stream_id();
            write_wal_file_header_with_stream_id(&mut file, sid)?;
            file.sync_all().ok();
            sid
        } else {
            // Проверим magic и прочитаем stream_id
            let mut magic = [0u8; 8];
            file.seek(SeekFrom::Start(0))?;
            Read::read_exact(&mut file, &mut magic)?;
            if &magic != WAL_MAGIC {
                // Повреждённая магия — перепишем файл и создадим новый stream_id
                file.set_len(0)?;
                let sid = generate_stream_id();
                write_wal_file_header_with_stream_id(&mut file, sid)?;
                file.sync_all().ok();
                sid
            } else {
                // Валидный header — прочитаем stream_id
                let sid = wal_header_read_stream_id(&mut file)?;
                if sid == 0 {
                    // Старый header без stream_id — сгенерировать и переписать
                    let new_sid = generate_stream_id();
                    write_wal_file_header_with_stream_id(&mut file, new_sid)?;
                    file.sync_all().ok();
                    new_sid
                } else {
                    sid
                }
            }
        };

        // Позиционируемся в конец файла для дальнейшей записи
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            file: Mutex::new(file),
            flush: Mutex::new(FlushState {
                pending_max_lsn: 0,
                flushed_lsn: 0,
                flushing: false,
            }),
            cv: Condvar::new(),
            coalesce_ms: AtomicU64::new(0),
            pages_since_last_fsync: AtomicU64::new(0),
            bytes_since_last_fsync: AtomicU64::new(0),
            stream_id,
        })
    }

    /// Установить окно коалессации fsync в миллисекундах (0 — выключить коалессацию).
    pub fn set_coalesce_ms(&self, ms: u64) {
        self.coalesce_ms.store(ms, Ordering::Relaxed);
    }

    /// Получить stream_id WAL.
    pub fn get_stream_id(&self) -> u64 {
        self.stream_id
    }
}

struct WalRegistry {
    map: std::collections::HashMap<PathBuf, Arc<WalInner>>,
}

impl WalRegistry {
    fn new() -> Self {
        Self {
            map: std::collections::HashMap::new(),
        }
    }

    fn get_or_create(&mut self, path: PathBuf) -> Result<Arc<WalInner>> {
        if let Some(inner) = self.map.get(&path) {
            return Ok(inner.clone());
        }

        let f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("open wal {}", path.display()))?;
        // Внутри WalInner::new проверим/запишем заголовок, установим stream_id и позиционируемся в конец
        let inner = Arc::new(WalInner::new(f)?);
        self.map.insert(path, inner.clone());
        Ok(inner)
    }
}

static REGISTRY: OnceLock<Mutex<WalRegistry>> = OnceLock::new();

fn registry_lock() -> &'static Mutex<WalRegistry> {
    REGISTRY.get_or_init(|| Mutex::new(WalRegistry::new()))
}

/// Получить (или создать) общий WalInner для WAL файла в корне root.
pub fn get_or_create_wal_inner(root: &Path) -> Result<Arc<WalInner>> {
    let path = wal_path(root);
    let mut reg = registry_lock().lock().unwrap();
    reg.get_or_create(path)
}

/// Установить окно коалессации fsync (глобально для файла в root).
pub fn set_group_coalesce_ms(root: &Path, ms: u64) -> Result<()> {
    let path = wal_path(root);
    let mut reg = registry_lock().lock().unwrap();
    let inner = reg.get_or_create(path)?;
    inner.set_coalesce_ms(ms);
    Ok(())
}
