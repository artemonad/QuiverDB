use crate::consts::{
    WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN,
    WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE, WAL_ROTATE_SIZE,
};
use crate::metrics::{record_wal_append, record_wal_fsync, record_wal_truncation};
use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian, WriteBytesExt};
use crc32fast::Hasher as Crc32;
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};

/// Глобальный реестр WAL по пути файла.
/// Нужен для коалесса fsync между разными Wal экземплярами (один процесс).
static REGISTRY: OnceLock<Mutex<WalRegistry>> = OnceLock::new();

struct WalRegistry {
    map: HashMap<PathBuf, Arc<WalInner>>,
}

impl WalRegistry {
    fn new() -> Self {
        Self { map: HashMap::new() }
    }

    fn get_or_create(&mut self, path: PathBuf) -> Result<Arc<WalInner>> {
        if let Some(inner) = self.map.get(&path) {
            return Ok(inner.clone());
        }
        // Инициализируем файл (magic/заголовок), если пуст.
        let mut f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path)
            .with_context(|| format!("open wal {}", path.display()))?;

        if f.metadata()?.len() == 0 {
            write_wal_file_header(&mut f)?;
            f.sync_all()?;
        } else {
            let mut magic = [0u8; 8];
            f.seek(SeekFrom::Start(0))?;
            std::io::Read::read_exact(&mut f, &mut magic)?;
            if &magic != WAL_MAGIC {
                return Err(anyhow!("bad WAL magic in {}", path.display()));
            }
        }
        // Сместим курсор на конец для будущих append.
        f.seek(SeekFrom::End(0))?;

        let inner = Arc::new(WalInner {
            path: path.clone(),
            file: Mutex::new(f),
            flush: Mutex::new(FlushState {
                pending_max_lsn: 0,
                flushed_lsn: 0,
                flushing: false,
            }),
            cv: Condvar::new(),
            coalesce_ms: AtomicU64::new(0), // по умолчанию без коалесса
        });
        self.map.insert(path, inner.clone());
        Ok(inner)
    }
}

struct FlushState {
    /// Максимальный LSN среди добавленных, но ещё не сброшенных на диск записей.
    pending_max_lsn: u64,
    /// Последний LSN, гарантированно сброшенный на диск (после успешного fsync).
    flushed_lsn: u64,
    /// Признак, что сейчас один из потоков выполняет fsync (остальные ждут).
    flushing: bool,
}

struct WalInner {
    #[allow(dead_code)]
    path: PathBuf,
    file: Mutex<std::fs::File>,
    flush: Mutex<FlushState>,
    cv: Condvar,
    coalesce_ms: AtomicU64,
}

/// Конфигурация group-commit:
/// - coalesce_ms: сколько миллисекунд подождать перед fsync, чтобы собрать несколько записей.
///   0 = отключено (semantics как раньше: fsync сразу).
#[derive(Debug, Clone, Copy)]
pub struct WalGroupCfg {
    pub coalesce_ms: u64,
}

pub struct Wal {
    inner: Arc<WalInner>,
}

impl Wal {
    pub fn open_for_append(root: &Path) -> Result<Self> {
        let path = root.join(WAL_FILE);
        // Убедимся, что заголовок есть/валиден и получим общий inner.
        let inner = REGISTRY
            .get_or_init(|| Mutex::new(WalRegistry::new()))
            .lock()
            .unwrap()
            .get_or_create(path)?;
        Ok(Wal { inner })
    }

    /// Настроить коалессирование fsync для текущего WAL-файла.
    /// Можно вызвать один раз при старте (например, из CLI/Db::open).
    pub fn set_group_config(root: &Path, cfg: WalGroupCfg) -> Result<()> {
        let path = root.join(WAL_FILE);
        let reg = REGISTRY.get_or_init(|| Mutex::new(WalRegistry::new()));
        let mut reg = reg.lock().unwrap();
        let inner = reg.get_or_create(path)?;
        inner.coalesce_ms.store(cfg.coalesce_ms, Ordering::Relaxed);
        Ok(())
    }

    /// Добавить запись «полное изображение страницы» с LSN.
    pub fn append_page_image(&mut self, lsn: u64, page_id: u64, page: &[u8]) -> Result<()> {
        let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
        hdr[0] = WAL_REC_PAGE_IMAGE; // type
        hdr[1] = 0u8; // flags
        LittleEndian::write_u16(&mut hdr[2..4], 0); // reserved
        LittleEndian::write_u64(&mut hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8], lsn);
        LittleEndian::write_u64(
            &mut hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8],
            page_id,
        );
        LittleEndian::write_u32(
            &mut hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4],
            page.len() as u32,
        );

        // CRC over header (except crc field) + payload
        let mut hasher = Crc32::new();
        hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
        hasher.update(page);
        let crc = hasher.finalize();
        LittleEndian::write_u32(&mut hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4], crc);

        // Append в конец файла
        {
            let mut f = self.inner.file.lock().unwrap();
            f.write_all(&hdr)?;
            f.write_all(page)?;
        }

        // Метрики: учтём записанные байты (hdr + payload) и инкремент append-счётчика
        record_wal_append(WAL_REC_HDR_SIZE + page.len());

        // Зафиксируем pending_max_lsn (под мьютексом flush)
        {
            let mut st = self.inner.flush.lock().unwrap();
            if lsn > st.pending_max_lsn {
                st.pending_max_lsn = lsn;
                // Разбудим возможного «коалесса-ожидателя»
                self.inner.cv.notify_all();
            }
        }
        Ok(())
    }

    /// Групповой fsync:
    /// - Если уже идёт fsync — ждём его завершения (и что flushed_lsn >= наш pending_max_lsn).
    /// - Если никого нет — становимся «флашером», ждём coalesce_ms (если задан),
    ///   затем делаем sync_all и помечаем flushed_lsn = pending_max_lsn.
    pub fn fsync(&mut self) -> Result<()> {
        let mut st = self.inner.flush.lock().unwrap();

        // Быстрый путь: нечего синхронизировать
        if st.pending_max_lsn <= st.flushed_lsn {
            return Ok(());
        }

        // Если уже кто-то синхронизирует — ждём, пока наш target не окажется на диске.
        let my_target = st.pending_max_lsn;
        if st.flushing {
            while st.flushed_lsn < my_target {
                st = self.inner.cv.wait(st).unwrap();
            }
            return Ok(());
        }

        // Мы — флашер.
        st.flushing = true;
        drop(st);

        // Опциональная коалесса-пауза, чтобы собрать больше записей.
        let ms = self.inner.coalesce_ms.load(Ordering::Relaxed);
        if ms > 0 {
            let guard = self.inner.flush.lock().unwrap();
            let _ = self
                .inner
                .cv
                .wait_timeout(guard, std::time::Duration::from_millis(ms))
                .unwrap();
            // guard опускается здесь
        }

        // Снимем финальную цель (что именно хотим сбросить) и проведём fsync вне мьютекса flush.
        let target = {
            let st2 = self.inner.flush.lock().unwrap();
            st2.pending_max_lsn
        };

        {
            let f = self.inner.file.lock().unwrap();
            f.sync_all()?;
        }

        // Обновим состояние и разбудим всех ожидающих. Учтём метрики батча.
        let mut st3 = self.inner.flush.lock().unwrap();
        let prev_flushed = st3.flushed_lsn;
        st3.flushed_lsn = st3.flushed_lsn.max(target);
        st3.flushing = false;
        self.inner.cv.notify_all();

        let batch_pages = st3.flushed_lsn.saturating_sub(prev_flushed);
        record_wal_fsync(batch_pages);

        Ok(())
    }

    /// Если WAL превышает порог, обнуляем до заголовка (простой чекпоинт).
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
}

/// Заголовок WAL-файла.
pub fn write_wal_file_header(f: &mut std::fs::File) -> Result<()> {
    use std::io::Seek;
    f.seek(SeekFrom::Start(0))?;
    std::io::Write::write_all(f, WAL_MAGIC)?;
    f.write_u32::<LittleEndian>(0)?; // reserved
    f.write_u32::<LittleEndian>(0)?; // reserved
    Ok(())
}