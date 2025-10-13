//! crypto/key_journal — журнал KID-эпох (TDE key rotation).
//!
//! Формат файла <root>/key_journal.bin (LE):
//! - Header (16 B):
//!   [magic8="P2KJN01\0"][version u32=1][reserved u32=0]
//! - Body: последовательность записей
//!   [since_lsn u64][kid_len u16][kid bytes (UTF-8, kid_len)]
//!
//! Правила:
//! - since_lsn монотонно растёт (проверяется на append).
//! - last_kid — KID последней записи.
//! - При rotate новый KID применяется для всех страниц с LSN >= since_lsn.
//!
//! Примечания:
//! - Это простой append-only журнал. Для диагностики/статуса достаточно epochs()/last_kid().

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"P2KJN01\0";
const VERSION: u32 = 1;
const HDR_SIZE: u64 = 16;

pub struct KeyJournal {
    path: PathBuf,
}

impl KeyJournal {
    /// Открыть существующий журнал (проверка заголовка).
    pub fn open(root: &Path) -> Result<Self> {
        let path = root.join("key_journal.bin");
        let mut f = OpenOptions::new()
            .read(true)
            .open(&path)
            .with_context(|| format!("open key journal {}", path.display()))?;
        // validate header
        let mut hdr = [0u8; HDR_SIZE as usize];
        f.read_exact(&mut hdr)?;
        if &hdr[0..8] != MAGIC {
            return Err(anyhow!("bad key journal magic at {}", path.display()));
        }
        let ver = LittleEndian::read_u32(&hdr[8..12]);
        if ver != VERSION {
            return Err(anyhow!("unsupported key journal version {}", ver));
        }
        Ok(Self { path })
    }

    /// Открыть или создать новый журнал (с валидным заголовком).
    pub fn open_or_create(root: &Path) -> Result<Self> {
        let path = root.join("key_journal.bin");
        if !path.exists() {
            let mut f = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)
                .with_context(|| format!("create key journal {}", path.display()))?;
            // header
            f.write_all(MAGIC)?;
            let mut u4 = [0u8; 4];
            LittleEndian::write_u32(&mut u4, VERSION);
            f.write_all(&u4)?;
            LittleEndian::write_u32(&mut u4, 0);
            f.write_all(&u4)?;
            let _ = f.sync_all();
        }
        Self::open(root)
    }

    /// Прочитать все эпохи (в порядке в файле).
    pub fn epochs(&self) -> Result<Vec<(u64, String)>> {
        let mut out = Vec::new();
        let mut f = OpenOptions::new().read(true).open(&self.path)?;
        let len = f.metadata()?.len();
        let mut pos = HDR_SIZE;
        while pos + 10 <= len {
            f.seek(SeekFrom::Start(pos))?;
            let mut buf10 = [0u8; 10];
            if f.read_exact(&mut buf10).is_err() {
                break;
            }
            let since = LittleEndian::read_u64(&buf10[0..8]);
            let kid_len = LittleEndian::read_u16(&buf10[8..10]) as usize;
            let next = pos + 10 + kid_len as u64;
            if next > len {
                break;
            }
            let mut kid = vec![0u8; kid_len];
            if f.read_exact(&mut kid).is_err() {
                break;
            }
            let kid_str = String::from_utf8(kid).map_err(|e| anyhow!("kid utf8: {}", e))?;
            out.push((since, kid_str));
            pos = next;
        }
        Ok(out)
    }

    /// Последний KID (если журнал пуст — None).
    pub fn last_kid(&self) -> Result<Option<String>> {
        let epochs = self.epochs()?;
        Ok(epochs.last().map(|e| e.1.clone()))
    }

    /// Добавить новую эпоху. Требование: since_lsn > последнего в журнале.
    pub fn add_epoch(&self, since_lsn: u64, kid: &str) -> Result<()> {
        let mut last_since = 0u64;
        if let Ok(eps) = self.epochs() {
            if let Some((s, _)) = eps.last() {
                last_since = *s;
            }
        }
        if since_lsn <= last_since {
            return Err(anyhow!(
                "since_lsn {} must be greater than last epoch since_lsn {}",
                since_lsn,
                last_since
            ));
        }

        let mut f = OpenOptions::new().append(true).open(&self.path)?;
        // frame: [since u64][kid_len u16][kid]
        let mut buf10 = [0u8; 10];
        LittleEndian::write_u64(&mut buf10[0..8], since_lsn);
        let kid_bytes = kid.as_bytes();
        if kid_bytes.len() > u16::MAX as usize {
            return Err(anyhow!("kid too long"));
        }
        LittleEndian::write_u16(&mut buf10[8..10], kid_bytes.len() as u16);

        f.write_all(&buf10)?;
        f.write_all(kid_bytes)?;
        let _ = f.sync_all();
        Ok(())
    }
}