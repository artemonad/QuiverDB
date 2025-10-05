// src/lib.rs
#![allow(non_snake_case)]

use anyhow::Result;
use std::fs;
use std::path::Path;

// Публичные модули
pub mod consts;
pub mod config;
pub mod db;
pub mod db_kv;            // вынесенная KV-логика (put_in_chain)
pub mod db_maintenance;   // вынесенные maintenance-функции (stats/sweep)
pub mod dir;
pub mod free;
pub mod hash;
pub mod lock;      // file locking
pub mod meta;
pub mod page_rh;   // v2: Robin Hood in-page index
pub mod page_ovf;  // v2: overflow pages
pub mod pager;
pub mod util;
pub mod db_scan;
pub mod subs;
pub mod wal;
pub mod backup;
pub mod cli;
pub mod metrics;
pub mod snapshots; // NEW: snapshots scaffold (Phase 1)

// FFI (C ABI) — подключается только при сборке с фичей "ffi"
#[cfg(feature = "ffi")]
pub mod ffi;

// Переэкспорты часто используемых сущностей
pub use db::Db;
pub use dir::Directory;
pub use free::FreeList;
pub use meta::{
    read_meta, set_clean_shutdown, set_last_lsn, write_meta_new, write_meta_overwrite, MetaHeader,
};
pub use util::{display_text, hex_dump, parse_u8_byte};
pub use wal::wal_replay_if_any;

// Re-export locking helpers (optional but handy)
pub use lock::{
    acquire_exclusive_lock, acquire_shared_lock, try_acquire_exclusive_lock,
    try_acquire_shared_lock, LockGuard, LockMode,
};

/// Инициализация новой БД: meta, первый сегмент, пустой WAL, пустой free-list.
pub fn init_db(root: &Path, page_size: u32) -> Result<()> {
    use crate::consts::{DATA_SEG_EXT, DATA_SEG_PREFIX, WAL_FILE, FREE_FILE};
    use crate::hash::HASH_KIND_DEFAULT;
    use crate::meta::{validate_page_size, write_meta_new, MetaHeader};
    use crate::util::create_empty_file;
    use crate::wal::write_wal_file_header;
    use anyhow::Context;

    validate_page_size(page_size)?;

    if root.exists() {
        let meta_path = root.join("meta");
        if meta_path.exists() {
            anyhow::bail!("DB already initialized at {}", root.display());
        }
    } else {
        fs::create_dir_all(root).with_context(|| format!("create dir {}", root.display()))?;
    }

    // Первый сегмент данных
    let seg1 = root.join(format!("{}{:06}.{}", DATA_SEG_PREFIX, 1, DATA_SEG_EXT));
    create_empty_file(&seg1)?;

    // Пустой WAL
    let wal_path = root.join(WAL_FILE);
    if !wal_path.exists() {
        let mut f = std::fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&wal_path)
            .with_context(|| format!("create wal {}", wal_path.display()))?;
        write_wal_file_header(&mut f)?;
        f.sync_all()?;
    }

    // Пустой free-list
    let free_path = root.join(FREE_FILE);
    if !free_path.exists() {
        // создаст файл с корректным заголовком и count=0
        free::FreeList::create(root)?;
    }

    // meta (v3): стабильный хеш + журналирование
    let meta = MetaHeader {
        version: 3,
        page_size,
        flags: 0,
        next_page_id: 0,
        hash_kind: HASH_KIND_DEFAULT,
        last_lsn: 0,
        clean_shutdown: true,
    };
    write_meta_new(root, &meta)?;
    Ok(())
}