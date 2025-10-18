//! snapstore/manifest — persisted snapshots (v2) manifest.
//!
//! Файл манифеста (JSON) хранится в <snapstore_dir>/manifests/<id>.json,
//! где <snapstore_dir> определяется так:
//!   - если P1_SNAPSTORE_DIR не задан или пуст — <root>/.snapstore;
//!   - если P1_SNAPSTORE_DIR абсолютный — используется как есть;
//!   - если относительный — трактуется относительно <root>.
//!
//! Формат стабильный на уровне API 2.2, сериализация через serde_json (pretty).
//!
//! Структура (v2):
//! - SnapshotManifestV2
//!   - meta: SnapshotMetaV2 {
//!         version=2,
//!         id, parent, created_unix_ms, message, labels,
//!         lsn, page_size, next_page_id, buckets,
//!         hash_kind, codec_default
//!     }
//!   - heads: массив (bucket u32, head_pid u64)
//!   - objects: массив ManifestObject { page_id u64, hash_hex String, bytes u64 }
//!
//! Примечание по совместимости:
//! - Поля hash_kind/codec_default добавлены в 2.2. Для чтения старых манифестов
//!   заданы serde default (xxhash64(seed=0), codec=none), так что read_manifest() не рушится.

use anyhow::{anyhow, Context, Result};
use serde::{Deserialize, Serialize};
use std::fs::{self, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

// Для значений по умолчанию новых полей meta
use crate::meta::{CODEC_NONE, HASH_KIND_XX64_SEED0};

// Используем общий резолвер каталога SnapStore из модуля snapstore
use super::resolve_snapstore_dir;

pub const SNAPSHOT_MANIFEST_VERSION_V2: u32 = 2;

fn default_hash_kind() -> u32 {
    HASH_KIND_XX64_SEED0
}
fn default_codec_default() -> u16 {
    CODEC_NONE
}

/// Метаданные снапшота (включая "рамку" БД на момент снимка).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotMetaV2 {
    pub version: u32, // == 2
    pub id: String,   // идентификатор снапшота (hex)
    pub parent: Option<String>,
    pub created_unix_ms: u64, // метка времени (ms)
    pub message: Option<String>,
    pub labels: Vec<String>,

    // Срез БД
    pub lsn: u64,
    pub page_size: u32,
    pub next_page_id: u64,
    pub buckets: u32,

    // Новые поля 2.2 (serde default для обратной совместимости)
    #[serde(default = "default_hash_kind")]
    pub hash_kind: u32, // 1 = xxhash64(seed=0)
    #[serde(default = "default_codec_default")]
    pub codec_default: u16, // 0=none, 1=zstd, 2=lz4 (резерв)
}

/// Привязка bucket -> head_pid для каталога в момент снапшота.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotHead {
    pub bucket: u32,
    pub head_pid: u64,
}

/// Объект контент-адресного хранилища (SnapStore), с которым связана страница.
/// hash_hex ссылается на <snapstore_dir>/objects/<hh>/<rest>.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ManifestObject {
    pub page_id: u64,
    pub hash_hex: String,
    pub bytes: u64,
}

/// Полный манифест снапшота (v2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapshotManifestV2 {
    pub meta: SnapshotMetaV2,
    pub heads: Vec<SnapshotHead>,
    pub objects: Vec<ManifestObject>,
}

impl SnapshotManifestV2 {
    /// Создать новый пустой манифест (builder-подобно) — далее дополняем heads/objects.
    pub fn new(
        id: String,
        parent: Option<String>,
        message: Option<String>,
        labels: Vec<String>,
        lsn: u64,
        page_size: u32,
        next_page_id: u64,
        buckets: u32,
        hash_kind: u32,
        codec_default: u16,
    ) -> Self {
        Self {
            meta: SnapshotMetaV2 {
                version: SNAPSHOT_MANIFEST_VERSION_V2,
                id,
                parent,
                created_unix_ms: now_unix_ms(),
                message,
                labels,
                lsn,
                page_size,
                next_page_id,
                buckets,
                hash_kind,
                codec_default,
            },
            heads: Vec::new(),
            objects: Vec::new(),
        }
    }

    /// Добавить пару (bucket, head_pid).
    pub fn add_head(&mut self, bucket: u32, head_pid: u64) {
        self.heads.push(SnapshotHead { bucket, head_pid });
    }

    /// Добавить информацию о размещённом объекте (page_id -> hash_hex, bytes).
    pub fn add_object(&mut self, page_id: u64, hash_hex: String, bytes: u64) {
        self.objects.push(ManifestObject {
            page_id,
            hash_hex,
            bytes,
        });
    }
}

// --------- Paths/IO ----------

/// Путь к каталогу manifests с учётом P1_SNAPSTORE_DIR.
///
/// По умолчанию: <root>/.snapstore/manifests.
/// С P1_SNAPSTORE_DIR:
///   - абсолютный путь: <P1_SNAPSTORE_DIR>/manifests
///   - относительный:   <root>/<P1_SNAPSTORE_DIR>/manifests
pub fn manifests_dir(root: &Path) -> PathBuf {
    resolve_snapstore_dir(root).join("manifests")
}

/// Путь к самому манифесту (<snapstore_dir>/manifests/<id>.json).
pub fn manifest_path(root: &Path, id: &str) -> PathBuf {
    manifests_dir(root).join(format!("{id}.json"))
}

/// Записать манифест в файл (pretty JSON).
pub fn write_manifest(root: &Path, m: &SnapshotManifestV2) -> Result<PathBuf> {
    let dir = manifests_dir(root);
    if !dir.exists() {
        fs::create_dir_all(&dir)
            .with_context(|| format!("create manifests dir {}", dir.display()))?;
    }
    let path = manifest_path(root, &m.meta.id);
    let tmp = path.with_extension("tmp");

    let json = serde_json::to_string_pretty(&m).context("serialize snapshot manifest")?;
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)
            .with_context(|| format!("open tmp manifest {}", tmp.display()))?;
        f.write_all(json.as_bytes())?;
        f.flush()?;
    }
    fs::rename(&tmp, &path)
        .with_context(|| format!("rename {} -> {}", tmp.display(), path.display()))?;
    Ok(path)
}

/// Прочитать манифест (учитывает P1_SNAPSTORE_DIR).
pub fn read_manifest(root: &Path, id: &str) -> Result<SnapshotManifestV2> {
    let path = manifest_path(root, id);
    let mut f = OpenOptions::new()
        .read(true)
        .open(&path)
        .with_context(|| format!("open manifest {}", path.display()))?;
    let mut buf = String::new();
    f.read_to_string(&mut buf)?;
    let m: SnapshotManifestV2 =
        serde_json::from_str(&buf).context("parse snapshot manifest json")?;
    if m.meta.version != SNAPSHOT_MANIFEST_VERSION_V2 {
        return Err(anyhow!(
            "unsupported manifest version {} (expected {})",
            m.meta.version,
            SNAPSHOT_MANIFEST_VERSION_V2
        ));
    }
    Ok(m)
}

/// Получить список всех доступных snapshot id (по файлам в manifests/).
pub fn list_manifests(root: &Path) -> Result<Vec<String>> {
    let dir = manifests_dir(root);
    if !dir.exists() {
        return Ok(Vec::new());
    }
    let mut out = Vec::new();
    for e in fs::read_dir(&dir).with_context(|| format!("read_dir {}", dir.display()))? {
        let p = e?.path();
        if p.extension().map(|ext| ext == "json").unwrap_or(false) {
            if let Some(stem) = p.file_stem().and_then(|s| s.to_str()) {
                out.push(stem.to_string());
            }
        }
    }
    out.sort();
    Ok(out)
}

// --------- Helpers ----------

/// Генерация случайного id снапшота (hex, 32 символа).
pub fn generate_snapshot_id() -> String {
    use rand::RngCore;
    let mut buf = [0u8; 16];
    rand::rngs::OsRng.fill_bytes(&mut buf);
    hex_encode(&buf)
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
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
