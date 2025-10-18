//! snapstore/restore — восстановление БД из persisted‑снапшота (SnapStore + manifest v2).
//!
//! Высокоуровневый сценарий:
//! - Имеем источник снапшота (src_root), где лежит .snapstore/{objects,manifests} и manifest v2.
//! - Хотим восстановить полноценную БД в dst_root (meta v4 + dir v2 + data сегменты).
//!
//! Публичные API:
//! - restore_from_id(src_root, dst_root, id, verify)
//! - restore_from_manifest(src_root, dst_root, &manifest, verify)
//!
//! Поведение:
//! - Создаёт (или валидирует) meta v4 и directory v2 в dst_root согласно полям manifest.meta.
//! - Переносит все страницы (objects) в сегменты dst_root по их page_id (raw‑write).
//! - Устанавливает directory heads и актуализирует meta.last_lsn/next_page_id (clean_shutdown=true).
//! - Усечёт WAL до заголовка.

use anyhow::{anyhow, Context, Result};
use std::path::Path;

use crate::dir::Directory;
use crate::meta::{
    init_meta_v4,
    read_meta,
    write_meta_overwrite, // для дефолтов при валидации
    CKSUM_CRC32C,
};
use crate::pager::Pager;
use crate::wal::Wal;

use super::manifest::{read_manifest, SnapshotManifestV2};
use super::SnapStore;

/// Восстановить БД в dst_root по id снапшота из src_root/.snapstore/manifests/<id>.json.
/// verify=true включает базовую проверку размеров страниц (равны page_size).
pub fn restore_from_id(src_root: &Path, dst_root: &Path, id: &str, verify: bool) -> Result<()> {
    let manifest = read_manifest(src_root, id)
        .with_context(|| format!("read manifest '{}' at {}", id, src_root.display()))?;
    restore_from_manifest(src_root, dst_root, &manifest, verify)
}

/// Восстановить БД в dst_root по заранее загруженному манифесту.
/// src_root — место, где живёт SnapStore (objects + manifests).
pub fn restore_from_manifest(
    src_root: &Path,
    dst_root: &Path,
    manifest: &SnapshotManifestV2,
    verify: bool,
) -> Result<()> {
    // 1) SnapStore (источник объектов страниц)
    let ss =
        SnapStore::open_or_create(src_root).context("open_or_create SnapStore at source root")?;

    // 2) Подготовка dst_root: meta v4 + directory v2
    ensure_meta_and_dir(dst_root, manifest)?;

    // 3) Запись всех страниц в dst_root
    let ps = manifest.meta.page_size as usize;

    // Откроем pager в dst_root для raw‑записи страниц.
    let mut pager =
        Pager::open(dst_root).with_context(|| format!("open pager at {}", dst_root.display()))?;

    // Восстановим все страницы (objects)
    for obj in &manifest.objects {
        let data = ss
            .get(&obj.hash_hex)
            .with_context(|| format!("snapstore get object {}", obj.hash_hex))?
            .ok_or_else(|| anyhow!("snapstore object {} not found", obj.hash_hex))?;

        if verify && data.len() != ps {
            return Err(anyhow!(
                "object {} length mismatch: got {}, expected {} (page_size)",
                obj.hash_hex,
                data.len(),
                ps
            ));
        }

        pager.ensure_allocated(obj.page_id)?;
        pager.write_page_raw(obj.page_id, &data)?;
    }

    // 4) Установим directory heads
    {
        let dir = Directory::open(dst_root)?;
        // set_heads_bulk — writer‑only API (pub(crate)), виден внутри crate'а
        let mut updates: Vec<(u32, u64)> = Vec::with_capacity(manifest.heads.len());
        for h in &manifest.heads {
            updates.push((h.bucket, h.head_pid));
        }
        if !updates.is_empty() {
            dir.set_heads_bulk(&updates)?;
        }
    }

    // 5) Установим meta.last_lsn/next_page_id/clean_shutdown и усечём WAL
    {
        let mut m = read_meta(dst_root)?;
        m.last_lsn = manifest.meta.lsn;
        m.next_page_id = manifest.meta.next_page_id;
        m.clean_shutdown = true;
        write_meta_overwrite(dst_root, &m)?;
    }
    {
        let mut wal = Wal::open_for_append(dst_root)?;
        wal.truncate_to_header()?;
    }

    Ok(())
}

// -------------------------- helpers --------------------------

fn ensure_meta_and_dir(dst_root: &Path, manifest: &SnapshotManifestV2) -> Result<()> {
    // Создадим каталог dst_root при необходимости
    if !dst_root.exists() {
        std::fs::create_dir_all(dst_root)
            .with_context(|| format!("create dst root {}", dst_root.display()))?;
    }

    let meta_path = dst_root.join("meta");
    if !meta_path.exists() {
        // fresh init согласно манифесту
        init_meta_v4(
            dst_root,
            manifest.meta.page_size,
            manifest.meta.hash_kind,
            manifest.meta.codec_default,
            CKSUM_CRC32C,
        )
        .with_context(|| "init_meta_v4 for restore")?;

        Directory::create(dst_root, manifest.meta.buckets)
            .with_context(|| "create directory v2 for restore")?;
        return Ok(());
    }

    // Существующий meta: провалидируем базовые поля и при необходимости оставим как есть.
    let m_cur = read_meta(dst_root)?;
    if m_cur.page_size != manifest.meta.page_size {
        return Err(anyhow!(
            "dst meta.page_size={} mismatches snapshot.page_size={}",
            m_cur.page_size,
            manifest.meta.page_size
        ));
    }
    // buckets в каталоге — валидируем
    let dir = Directory::open(dst_root)?;
    if dir.bucket_count != manifest.meta.buckets {
        return Err(anyhow!(
            "dst directory buckets={} mismatches snapshot.buckets={}",
            dir.bucket_count,
            manifest.meta.buckets
        ));
    }
    // hash_kind/codec_default — не критично, но предупредим, если расходится
    if m_cur.hash_kind != manifest.meta.hash_kind {
        eprintln!(
            "[WARN] restore: dst hash_kind={} differs from snapshot hash_kind={}; using dst meta",
            m_cur.hash_kind, manifest.meta.hash_kind
        );
    }
    if m_cur.codec_default != manifest.meta.codec_default {
        eprintln!(
            "[WARN] restore: dst codec_default={} differs from snapshot codec_default={}; using dst meta",
            m_cur.codec_default, manifest.meta.codec_default
        );
    }
    Ok(())
}
