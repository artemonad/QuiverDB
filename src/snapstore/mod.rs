//! snapstore — контент‑адресное хранилище (objects + refs) и интеграция с манифестом v2/снапшотами.
//!
//! Состав:
//! - SnapStore: простое хранилище объектов по SHA‑256 (hex) в <snapstore_dir>/{objects,refs},
//!   с refcount в <snapstore_dir>/refs/<hash>.ref (LE u64).
//! - manifest: форматы SnapshotManifestV2 и утилиты записи/чтения.
//! - snapshot: SnapshotManager для создания persisted‑снапшота из открытой БД.
//! - restore: восстановление БД из SnapStore+manifest v2 (полная БД в новый корень).
//!
//! NEW (2.2):
//! - P1_SNAPSTORE_DIR — переопределение пути SnapStore.
//!   * Если переменная не задана или пустая — используется <db_root>/.snapstore (старое поведение).
//!   * Если путь абсолютный — используется как есть.
//!   * Если путь относительный — трактуется относительно корня БД.
//!
//! Пример:
//!   P1_SNAPSTORE_DIR=/mnt/snapstore     → /mnt/snapstore/{objects,refs}
//!   P1_SNAPSTORE_DIR=.cache/snapstore   → <db_root>/.cache/snapstore/{objects,refs}

use anyhow::{anyhow, Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use fs2::FileExt;
use sha2::{Digest, Sha256};
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

// ---------------------- SnapStore (objects + refs) ----------------------

pub struct SnapStore {
    /// Каталог SnapStore (либо <root>/.snapstore, либо из P1_SNAPSTORE_DIR).
    dir: PathBuf,
    /// Внутренние подкаталоги.
    objects: PathBuf,
    refs: PathBuf,
    lock_path: PathBuf,
}

impl SnapStore {
    /// Открыть или создать SnapStore.
    ///
    /// Путь определяется так:
    /// - если P1_SNAPSTORE_DIR не задан или пуст — <root>/.snapstore;
    /// - если P1_SNAPSTORE_DIR абсолютный — используется как есть;
    /// - если P1_SNAPSTORE_DIR относительный — <root>/<P1_SNAPSTORE_DIR>.
    pub fn open_or_create(root: &Path) -> Result<Self> {
        let dir = resolve_snapstore_dir(root);
        let objects = dir.join("objects");
        let refs = dir.join("refs");
        let lock_path = dir.join("snapstore.lock");

        if !dir.exists() {
            fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        }
        if !objects.exists() {
            fs::create_dir_all(&objects)
                .with_context(|| format!("create {}", objects.display()))?;
        }
        if !refs.exists() {
            fs::create_dir_all(&refs).with_context(|| format!("create {}", refs.display()))?;
        }

        Ok(Self {
            dir,
            objects,
            refs,
            lock_path,
        })
    }

    /// Корневой путь SnapStore (учитывает P1_SNAPSTORE_DIR).
    pub fn dir_path(&self) -> &Path {
        &self.dir
    }

    /// Положить объект по содержимому bytes.
    /// Возвращает (hash_hex, existed_before, new_refcnt).
    pub fn put(&self, bytes: &[u8]) -> Result<(String, bool, u64)> {
        let hash_hex = sha256_hex(bytes);
        let _lk = self.lock_exclusive()?;

        let obj_path = self.object_path(&hash_hex);
        let existed = obj_path.exists();

        if !existed {
            // сохранить через tmp+rename
            let tmp = obj_path.with_extension("tmp");
            if let Some(parent) = tmp.parent() {
                fs::create_dir_all(parent)?;
            }
            {
                let mut f = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .truncate(true)
                    .open(&tmp)
                    .with_context(|| format!("open tmp {}", tmp.display()))?;
                f.write_all(bytes)?;
                let _ = f.sync_all();
            }
            fs::rename(&tmp, &obj_path)
                .with_context(|| format!("rename {} -> {}", tmp.display(), obj_path.display()))?;
        }

        let new_ref = self.add_ref_inner(&hash_hex)?;
        Ok((hash_hex, existed, new_ref))
    }

    /// Прочитать объект по hex‑хэшу.
    pub fn get(&self, hash_hex: &str) -> Result<Option<Vec<u8>>> {
        let p = self.object_path(hash_hex);
        if !p.exists() {
            return Ok(None);
        }
        let mut f = OpenOptions::new().read(true).open(&p)?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        Ok(Some(buf))
    }

    /// Существует ли объект.
    pub fn has(&self, hash_hex: &str) -> bool {
        self.object_path(hash_hex).exists()
    }

    /// Размер объекта, если существует.
    pub fn size(&self, hash_hex: &str) -> Result<Option<u64>> {
        let p = self.object_path(hash_hex);
        if !p.exists() {
            return Ok(None);
        }
        Ok(Some(p.metadata()?.len()))
    }

    /// Увеличить refcount для объекта.
    pub fn add_ref(&self, hash_hex: &str) -> Result<u64> {
        let _lk = self.lock_exclusive()?;
        self.add_ref_inner(hash_hex)
    }

    /// Уменьшить refcount; при 0 удаляет объект и ref‑файл. Возвращает новое значение.
    pub fn dec_ref(&self, hash_hex: &str) -> Result<u64> {
        let _lk = self.lock_exclusive()?;
        self.dec_ref_inner(hash_hex)
    }

    // ----------------- внутренняя логика -----------------

    fn lock_exclusive(&self) -> Result<File> {
        let f = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&self.lock_path)
            .with_context(|| format!("open lock {}", self.lock_path.display()))?;
        f.lock_exclusive()
            .with_context(|| format!("lock_exclusive {}", self.lock_path.display()))?;
        Ok(f)
    }

    fn object_path(&self, hash_hex: &str) -> PathBuf {
        // objects/<hh>/<rest>
        let (hh, rest) = split_hash_hex(hash_hex);
        self.objects.join(hh).join(rest)
    }

    fn ref_path(&self, hash_hex: &str) -> PathBuf {
        self.refs.join(format!("{}.ref", hash_hex))
    }

    fn add_ref_inner(&self, hash_hex: &str) -> Result<u64> {
        if hash_hex.len() != 64 {
            return Err(anyhow!("bad sha256 hex length"));
        }
        // ensure object subdir exists (на случай внешнего создания)
        let obj = self.object_path(hash_hex);
        if let Some(parent) = obj.parent() {
            if !parent.exists() {
                fs::create_dir_all(parent)?;
            }
        }

        let rp = self.ref_path(hash_hex);
        let cur = if rp.exists() {
            read_refcount(&rp).unwrap_or(0)
        } else {
            0
        };
        let new = cur.saturating_add(1);
        write_refcount(&rp, new)?;
        Ok(new)
    }

    fn dec_ref_inner(&self, hash_hex: &str) -> Result<u64> {
        let rp = self.ref_path(hash_hex);
        if !rp.exists() {
            // нечего уменьшать
            return Ok(0);
        }
        let cur = read_refcount(&rp).unwrap_or(0);
        if cur == 0 {
            return Ok(0);
        }
        let new = cur - 1;
        if new == 0 {
            // удалить объект и ref‑файл
            let obj = self.object_path(hash_hex);
            let _ = fs::remove_file(&obj);
            let _ = fs::remove_file(&rp);
            // попытаться удалить пустой подкаталог objects/<hh> (best‑effort)
            if let Some(dir_hh) = obj.parent() {
                let _ = fs::remove_dir(dir_hh);
            }
        } else {
            write_refcount(&rp, new)?;
        }
        Ok(new)
    }
}

// ----------------- утилиты SnapStore -----------------

fn sha256_hex(bytes: &[u8]) -> String {
    let mut hasher = Sha256::new();
    hasher.update(bytes);
    let digest = hasher.finalize();
    hex_encode(&digest)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
    out
}

fn split_hash_hex(h: &str) -> (&str, &str) {
    let hh = &h[0..2];
    let rest = &h[2..];
    (hh, rest)
}

fn read_refcount(p: &Path) -> Result<u64> {
    let mut f = OpenOptions::new().read(true).open(p)?;
    let mut buf = [0u8; 8];
    f.read_exact(&mut buf)?;
    Ok(LittleEndian::read_u64(&buf))
}

fn write_refcount(p: &Path, v: u64) -> Result<()> {
    let tmp = p.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        let mut buf = [0u8; 8];
        LittleEndian::write_u64(&mut buf, v);
        f.write_all(&buf)?;
        let _ = f.sync_all();
    }
    fs::rename(&tmp, p)?;
    Ok(())
}

/// Разрешение пути SnapStore с учётом ENV P1_SNAPSTORE_DIR.
///
/// Правила:
/// - пустой/неуказанный → <db_root>/.snapstore
/// - абсолютный         → используем как есть
/// - относительный      → <db_root>/<значение>
pub(super) fn resolve_snapstore_dir(db_root: &Path) -> PathBuf {
    match std::env::var("P1_SNAPSTORE_DIR") {
        Ok(val) => {
            let s = val.trim();
            if s.is_empty() {
                return db_root.join(".snapstore");
            }
            let p = Path::new(s);
            if p.is_absolute() {
                p.to_path_buf()
            } else {
                db_root.join(p)
            }
        }
        Err(_) => db_root.join(".snapstore"),
    }
}

// ---------------------- manifest v2 (подключение) ----------------------

pub mod manifest;

pub use manifest::{
    generate_snapshot_id, list_manifests, manifest_path, manifests_dir, read_manifest,
    write_manifest, ManifestObject, SnapshotManifestV2, SnapshotMetaV2,
    SNAPSHOT_MANIFEST_VERSION_V2,
};

// ---------------------- snapshot manager (подключение) ----------------------

mod restore;
pub mod snapshot;

pub use snapshot::SnapshotManager;
// NEW: реэкспорт функций восстановления
pub use restore::{restore_from_id, restore_from_manifest};
