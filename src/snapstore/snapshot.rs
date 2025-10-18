//! snapstore/snapshot — создание и удаление persisted‑снапшотов (v2)
//!
//! Реализовано:
//! - SnapshotManager::create_persisted — создать снапшот в SnapStore (+manifest v2).
//! - SnapshotManager::create_persisted_from_root — быстрый хелпер от корня.
//! - NEW: SnapshotManager::delete_persisted — удалить снапшот:
//!   * Читает manifest;
//!   * Для каждого объекта вызывает dec_ref (объект удаляется при rc==0);
//!   * Удаляет файл манифеста.
//!
//! Примечание:
//! - Путь SnapStore учитывает ENV P1_SNAPSTORE_DIR (см. snapstore::open_or_create).
//! - Удаление манифеста выполняется после успешного dec_ref всех объектов.

use anyhow::{Context, Result};
use std::fs;
use std::path::Path;

use super::SnapStore;
use crate::db::Db;
use crate::page::PAGE_MAGIC;
use crate::snapstore::manifest::{
    generate_snapshot_id, manifest_path, read_manifest, write_manifest, SnapshotManifestV2,
};

pub struct SnapshotManager;

impl SnapshotManager {
    /// Создать persisted‑снапшот для открытой БД.
    ///
    /// Параметры:
    /// - db: открытая БД (желательно reader: Db::open_ro).
    /// - message: произвольный комментарий.
    /// - labels: теги для поиска/фильтрации.
    /// - parent: id родительского снапшота (если делаем цепочку).
    ///
    /// Возвращает: id созданного снапшота (hex).
    pub fn create_persisted(
        db: &Db,
        message: Option<&str>,
        labels: &[&str],
        parent: Option<&str>,
    ) -> Result<String> {
        let root = &db.root;
        let meta = &db.pager.meta;
        let lsn = meta.last_lsn;
        let ps = meta.page_size as usize;
        let next_page_id = meta.next_page_id;
        let buckets = db.dir.bucket_count;

        // SnapStore: objects/refs
        let ss = SnapStore::open_or_create(root).context("open_or_create SnapStore")?;

        // Новый id снапшота
        let id = generate_snapshot_id();

        // Инициализируем манифест (ВАЖНО: передаём hash_kind и codec_default из meta)
        let mut manifest = SnapshotManifestV2::new(
            id.clone(),
            parent.map(|s| s.to_string()),
            message.map(|s| s.to_string()),
            labels.iter().map(|s| s.to_string()).collect(),
            lsn,
            meta.page_size,
            next_page_id,
            buckets,
            meta.hash_kind,
            meta.codec_default,
        );

        // Heads каталога (bucket -> head_pid)
        for b in 0..buckets {
            let head = db.dir.head(b).with_context(|| format!("dir.head({})", b))?;
            manifest.add_head(b, head);
        }

        // Обход всех страниц
        let mut page_buf = vec![0u8; ps];

        for pid in 0..next_page_id {
            // read_page проверит CRC/AEAD. Если страница не аллоцирована/битая — пропускаем.
            match db.pager.read_page(pid, &mut page_buf) {
                Ok(()) => {
                    // Пропустим буферы без корректной MAGIC
                    if &page_buf[0..4] != PAGE_MAGIC {
                        continue;
                    }

                    // Сохраним в SnapStore (dedup по содержимому)
                    let (hash_hex, _existed, _rc) = ss
                        .put(&page_buf)
                        .with_context(|| format!("snapstore put page {}", pid))?;

                    manifest.add_object(pid, hash_hex, ps as u64);
                }
                Err(_) => {
                    // Страница не читаема/невалидна — пропустим (persisted снапшот частичный по живым страницам)
                    continue;
                }
            }
        }

        // Сохраним манифест на диск
        let _path = write_manifest(root, &manifest).with_context(|| "write snapshot manifest")?;

        Ok(id)
    }

    /// Быстрый помощник: сделать снапшот по корню БД без заранее открытого Db.
    /// Откроет БД read-only и вызовет create_persisted.
    pub fn create_persisted_from_root<P: AsRef<Path>>(
        root: P,
        message: Option<&str>,
        labels: &[&str],
        parent: Option<&str>,
    ) -> Result<String> {
        let db = crate::db::Db::open_ro(root.as_ref())?;
        Self::create_persisted(&db, message, labels, parent)
    }

    /// NEW: Удалить persisted‑снапшот:
    /// - загружает манифест;
    /// - уменьшает refcount для всех объектов (объект удаляется при rc==0);
    /// - удаляет файл манифеста.
    pub fn delete_persisted<P: AsRef<Path>>(root: P, id: &str) -> Result<()> {
        let root = root.as_ref();

        // 1) Прочитаем манифест
        let m = read_manifest(root, id)
            .with_context(|| format!("read manifest '{}' at {}", id, root.display()))?;

        // 2) Откроем SnapStore (учитывает P1_SNAPSTORE_DIR)
        let ss = SnapStore::open_or_create(root).context("open_or_create SnapStore")?;

        // 3) Декремент refcount по всем объектам
        for obj in &m.objects {
            // dec_ref безопасен: при достижении 0 объект и ref‑файл удаляются
            let _ = ss
                .dec_ref(&obj.hash_hex)
                .with_context(|| format!("dec_ref for object {}", obj.hash_hex))?;
        }

        // 4) Удалим сам манифест
        let mpath = manifest_path(root, id);
        if mpath.exists() {
            fs::remove_file(&mpath)
                .with_context(|| format!("remove manifest {}", mpath.display()))?;
        }

        Ok(())
    }
}
