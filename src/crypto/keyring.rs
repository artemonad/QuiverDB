//! crypto/keyring — безопасный стор для обёрнутых DEK (KMS envelope) по KID.
//!
//! Формат файла <root>/keyring.bin (LE):
//! - Header (16 B):
//!   [magic8="P2KEYR01"][version u32=1][reserved u32=0]
//! - Body: последовательность записей
//!   [kid_len u16][kid bytes (UTF‑8, kid_len)]
//!   [blob_len u32][blob bytes (обёрнутый DEK из KMS)]
//!
//! Политика:
//! - put(kid, blob) — атомарная перезапись файла (tmp+rename) под файловой блокировкой.
//! - get(kid)       — читает все записи и берёт последнюю запись для KID.
//! - list()         — диагностический список (kid, blob_len).
//!
//! Замечание: формат простой и предназначен для небольшого числа KID‑эпох.

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use fs2::FileExt;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"P2KEYR01";
const VERSION: u32 = 1;
const HDR_SIZE: u64 = 16;

fn keyring_path(root: &Path) -> PathBuf {
    root.join("keyring.bin")
}

fn keyring_lock_path(root: &Path) -> PathBuf {
    root.join("keyring.bin.lock")
}

fn lock_keyring(root: &Path) -> Result<File> {
    let lp = keyring_lock_path(root);
    let f = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(&lp)
        .with_context(|| format!("open lock {}", lp.display()))?;
    f.lock_exclusive()
        .with_context(|| format!("lock_exclusive {}", lp.display()))?;
    Ok(f)
}

#[cfg(unix)]
fn fsync_parent_dir(path: &Path) -> std::io::Result<()> {
    use std::fs::File;
    if let Some(parent) = path.parent() {
        if !parent.as_os_str().is_empty() {
            let dir = File::open(parent)?;
            dir.sync_all()?;
        }
    }
    Ok(())
}
#[cfg(not(unix))]
fn fsync_parent_dir(_path: &Path) -> std::io::Result<()> {
    Ok(())
}

pub struct KeyRing {
    root: PathBuf,
    path: PathBuf,
}

impl KeyRing {
    /// Открыть существующий keyring и проверить заголовок.
    pub fn open(root: &Path) -> Result<Self> {
        let path = keyring_path(root);
        let mut f = OpenOptions::new()
            .read(true)
            .open(&path)
            .with_context(|| format!("open keyring {}", path.display()))?;
        // header
        let mut hdr = [0u8; HDR_SIZE as usize];
        f.read_exact(&mut hdr)?;
        if &hdr[0..8] != MAGIC {
            return Err(anyhow!("bad keyring magic at {}", path.display()));
        }
        let ver = LittleEndian::read_u32(&hdr[8..12]);
        if ver != VERSION {
            return Err(anyhow!("unsupported keyring version {}", ver));
        }
        Ok(Self { root: root.to_path_buf(), path })
    }

    /// Открыть или создать keyring с валидным заголовком.
    pub fn open_or_create(root: &Path) -> Result<Self> {
        let path = keyring_path(root);
        if !path.exists() {
            let _lk = lock_keyring(root)?;
            let mut f = OpenOptions::new()
                .create_new(true)
                .read(true)
                .write(true)
                .open(&path)
                .with_context(|| format!("create keyring {}", path.display()))?;
            // header
            f.write_all(MAGIC)?;
            let mut u4 = [0u8; 4];
            LittleEndian::write_u32(&mut u4, VERSION);
            f.write_all(&u4)?; // version
            LittleEndian::write_u32(&mut u4, 0);
            f.write_all(&u4)?; // reserved
            let _ = f.sync_all();
        }
        Self::open(root)
    }

    /// Вернуть обёрнутый DEK для KID (если есть).
    pub fn get(&self, kid: &str) -> Result<Option<Vec<u8>>> {
        let mut f = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .with_context(|| format!("open keyring {}", self.path.display()))?;
        let len = f.metadata()?.len();
        if len < HDR_SIZE {
            return Err(anyhow!("keyring too small (< header)"));
        }
        let mut pos = HDR_SIZE;
        let mut found: Option<Vec<u8>> = None;

        while pos + 2 <= len {
            f.seek(SeekFrom::Start(pos))?;
            // kid_len
            let mut buf2 = [0u8; 2];
            if f.read_exact(&mut buf2).is_err() {
                break;
            }
            let kid_len = LittleEndian::read_u16(&buf2) as usize;
            pos += 2;
            if pos + kid_len as u64 + 4 > len {
                break;
            }
            // kid
            let mut kid_bytes = vec![0u8; kid_len];
            if f.read_exact(&mut kid_bytes).is_err() {
                break;
            }
            pos += kid_len as u64;
            let kid_str = String::from_utf8(kid_bytes).unwrap_or_default();

            // blob_len
            let mut buf4 = [0u8; 4];
            if f.read_exact(&mut buf4).is_err() {
                break;
            }
            let blob_len = LittleEndian::read_u32(&buf4) as usize;
            pos += 4;
            if pos + blob_len as u64 > len {
                break;
            }
            // blob
            let mut blob = vec![0u8; blob_len];
            if f.read_exact(&mut blob).is_err() {
                break;
            }
            pos += blob_len as u64;

            if kid_str == kid {
                found = Some(blob);
                // не прерываем — если дальше есть дубликат, возьмём последний; но при put мы переписываем файл.
            }
        }

        Ok(found)
    }

    /// Записать/обновить запись KID->blob. Атомарно (tmp+rename) под lock.
    pub fn put(&self, kid: &str, blob: &[u8]) -> Result<()> {
        if kid.len() > u16::MAX as usize {
            return Err(anyhow!("kid too long"));
        }

        let _lk = lock_keyring(&self.root)?;

        // Прочитаем все записи в map
        let mut map: HashMap<String, Vec<u8>> = HashMap::new();
        if let Ok(list) = self.list_internal() {
            for (k, b) in list {
                map.insert(k, b);
            }
        }
        // Обновим/вставим запись
        map.insert(kid.to_string(), blob.to_vec());

        // Сериализуем в tmp
        let tmp = self.path.with_file_name(format!(
            "{}.tmp",
            self.path.file_name().unwrap().to_string_lossy()
        ));
        let _ = std::fs::remove_file(&tmp);
        let mut tf = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .with_context(|| format!("open tmp {}", tmp.display()))?;

        // header
        tf.write_all(MAGIC)?;
        let mut buf4 = [0u8; 4];
        LittleEndian::write_u32(&mut buf4, VERSION);
        tf.write_all(&buf4)?;
        LittleEndian::write_u32(&mut buf4, 0);
        tf.write_all(&buf4)?;

        // body (детерминированный порядок по KID)
        let mut items: Vec<(String, Vec<u8>)> = map.into_iter().collect();
        items.sort_by(|a, b| a.0.cmp(&b.0));
        for (k, v) in items {
            let kbytes = k.as_bytes();
            let mut buf2 = [0u8; 2];
            LittleEndian::write_u16(&mut buf2, kbytes.len() as u16);
            tf.write_all(&buf2)?;
            tf.write_all(kbytes)?;
            LittleEndian::write_u32(&mut buf4, v.len() as u32);
            tf.write_all(&buf4)?;
            tf.write_all(&v)?;
        }
        let _ = tf.sync_all();

        // rename + fsync parent
        std::fs::rename(&tmp, &self.path)?;
        let _ = fsync_parent_dir(&self.path);

        Ok(())
    }

    /// Диагностический список всех записей (kid, blob_len).
    pub fn list(&self) -> Result<Vec<(String, usize)>> {
        let entries = self.list_internal()?;
        Ok(entries.into_iter().map(|(k, v)| (k, v.len())).collect())
    }

    fn list_internal(&self) -> Result<Vec<(String, Vec<u8>)>> {
        let mut out: Vec<(String, Vec<u8>)> = Vec::new();

        if !self.path.exists() {
            return Ok(out);
        }
        let mut f = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .with_context(|| format!("open keyring {}", self.path.display()))?;
        let len = f.metadata()?.len();
        if len < HDR_SIZE {
            return Err(anyhow!("keyring too small (< header)"));
        }
        let mut hdr = [0u8; HDR_SIZE as usize];
        f.read_exact(&mut hdr)?;
        if &hdr[0..8] != MAGIC {
            return Err(anyhow!("bad keyring magic at {}", self.path.display()));
        }
        let ver = LittleEndian::read_u32(&hdr[8..12]);
        if ver != VERSION {
            return Err(anyhow!("unsupported keyring version {}", ver));
        }

        let mut pos = HDR_SIZE;
        while pos + 2 <= len {
            f.seek(SeekFrom::Start(pos))?;
            let mut buf2 = [0u8; 2];
            if f.read_exact(&mut buf2).is_err() {
                break;
            }
            let kid_len = LittleEndian::read_u16(&buf2) as usize;
            pos += 2;
            if pos + kid_len as u64 + 4 > len {
                break;
            }
            let mut kid_bytes = vec![0u8; kid_len];
            if f.read_exact(&mut kid_bytes).is_err() {
                break;
            }
            pos += kid_len as u64;
            let kid = String::from_utf8(kid_bytes).unwrap_or_default();

            let mut buf4 = [0u8; 4];
            if f.read_exact(&mut buf4).is_err() {
                break;
            }
            let blob_len = LittleEndian::read_u32(&buf4) as usize;
            pos += 4;
            if pos + blob_len as u64 > len {
                break;
            }
            let mut blob = vec![0u8; blob_len];
            if f.read_exact(&mut blob).is_err() {
                break;
            }
            pos += blob_len as u64;

            out.push((kid, blob));
        }

        Ok(out)
    }
}