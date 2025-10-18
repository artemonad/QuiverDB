//! db/open — открытие Db (writer/read-only) с конфигом и блокировками.

use anyhow::{Context, Result};
use fs2::FileExt;
use std::path::Path;
use std::sync::Arc;

use crate::config::QuiverConfig;
use crate::dir::{Directory, NO_PAGE};
use crate::meta::set_clean_shutdown;
use crate::pager::Pager;
use crate::wal::{Wal, WalGroupCfg};

// программная конфигурация процессного page cache
use crate::pager::io::page_cache_configure;

use super::core::{open_lock_file, Db, MemKeyLoc, LOCK_FILE};

use crate::page::{kv_header_read_v3, PAGE_MAGIC, PAGE_TYPE_KV_RH3};
use byteorder::{ByteOrder, LittleEndian};
// packed-aware helpers (с оффсетами)
use crate::page::kv::kv_for_each_record_with_off;
// Bloom sidecar
use crate::bloom::BloomSidecar;
use crate::util::now_secs;

impl Db {
    pub fn open_with_config(root: &Path, cfg: QuiverConfig) -> Result<Self> {
        let lock = open_lock_file(root)?;
        lock.lock_exclusive()
            .with_context(|| format!("lock_exclusive {}", root.join(LOCK_FILE).display()))?;

        Pager::wal_replay_with_pager(root)?;
        set_clean_shutdown(root, false)?;

        let mut pager = Pager::open(root)?;
        pager.set_data_fsync(cfg.data_fsync);
        pager.set_tde_config(cfg.tde_enabled, cfg.tde_kid.clone());
        pager.set_ovf_threshold_bytes(cfg.ovf_threshold_bytes);
        if pager.tde_enabled {
            pager.ensure_tde_key()?;
        }

        if cfg.page_cache_pages > 0 {
            page_cache_configure(pager.meta.page_size as usize, cfg.page_cache_pages);
        }

        let _ = Wal::set_group_config(
            root,
            WalGroupCfg {
                coalesce_ms: cfg.wal_coalesce_ms,
            },
        );

        let dir = Directory::open(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            pager,
            dir,
            _lock: lock,
            readonly: false,
            mem_keydir: None,
            bloom_ro: None,
        })
    }

    pub fn open_ro_with_config(root: &Path, cfg: QuiverConfig) -> Result<Self> {
        let lock = open_lock_file(root)?;
        lock.lock_shared()
            .with_context(|| format!("lock_shared {}", root.join(LOCK_FILE).display()))?;

        let mut pager = Pager::open(root)?;
        pager.set_data_fsync(cfg.data_fsync);
        pager.set_tde_config(cfg.tde_enabled, cfg.tde_kid.clone());
        pager.set_ovf_threshold_bytes(cfg.ovf_threshold_bytes);
        if pager.tde_enabled {
            pager.ensure_tde_key()?;
        }

        if cfg.page_cache_pages > 0 {
            page_cache_configure(pager.meta.page_size as usize, cfg.page_cache_pages);
        }

        let dir = Directory::open(root)?;
        let mut db = Self {
            root: root.to_path_buf(),
            pager,
            dir,
            _lock: lock,
            readonly: true,
            mem_keydir: None,
            bloom_ro: None,
        };

        db.rebuild_mem_keydir_if_enabled()?;

        let bloom_path = db.root.join("bloom.bin");
        if bloom_path.exists() {
            if let Ok(sc) = BloomSidecar::open_ro(&db.root) {
                db.bloom_ro = Some(Arc::new(sc));
            }
        }

        Ok(db)
    }

    pub fn open(root: &Path) -> Result<Self> {
        let cfg = QuiverConfig::from_env();
        Self::open_with_config(root, cfg)
    }

    pub fn open_ro(root: &Path) -> Result<Self> {
        let cfg = QuiverConfig::from_env();
        Self::open_ro_with_config(root, cfg)
    }
}

// -------------------- keydir builder (RO) --------------------

impl Db {
    /// Построить in‑memory keydir, если включено (по умолчанию включено).
    /// Отключается ENV P1_MEM_KEYDIR=0|false|off|no.
    fn rebuild_mem_keydir_if_enabled(&mut self) -> Result<()> {
        let on = std::env::var("P1_MEM_KEYDIR")
            .ok()
            .map(|s| s.to_ascii_lowercase())
            .map(|s| !(s == "0" || s == "false" || s == "off" || s == "no"))
            .unwrap_or(true);
        if !on {
            return Ok(());
        }
        self.rebuild_mem_keydir()
    }

    /// Перестроить keydir в один проход по цепочке (head→tail).
    /// Логика:
    /// - Обход каждой страницы в порядке “новые→старые” (kv_for_each_record_with_off).
    /// - Если по ключу уже принято решение — дальше его игнорируем.
    /// - Tombstone → записать (pid=NO_PAGE, off=0) в keydir (ускоритель отрицательных результатов).
    /// - Валидная запись (TTL ок) → записать (pid текущей страницы, off смещение записи) в keydir.
    fn rebuild_mem_keydir(&mut self) -> Result<()> {
        use std::collections::HashSet;

        self.ensure_mem_keydir();
        self.mem_keydir_clear();

        let ps = self.pager.meta.page_size as usize;
        let now = now_secs();

        let mut page = vec![0u8; ps];

        for b in 0..self.dir.bucket_count {
            let mut pid = self.dir.head(b)?;
            if pid == NO_PAGE {
                continue;
            }

            // Набор ключей, по которым уже принято решение (present или tombstone).
            let mut decided: HashSet<Vec<u8>> = HashSet::new();

            while pid != NO_PAGE {
                self.pager.read_page(pid, &mut page)?;
                if &page[0..4] != PAGE_MAGIC {
                    break;
                }
                let ptype = LittleEndian::read_u16(&page[6..8]);
                if ptype != PAGE_TYPE_KV_RH3 {
                    break;
                }
                let hdr = kv_header_read_v3(&page)?;

                // Обход “новые→старые” внутри страницы, с оффсетами
                let bucket = b;
                let cur_pid = pid;
                kv_for_each_record_with_off(&page, |off_usize, k, _v, expires_at_sec, vflags| {
                    if decided.contains(k) {
                        return;
                    }
                    let is_tomb = (vflags & 0x1) == 1;
                    if is_tomb {
                        // Tombstone: фиксируем отрицательный ускоритель и отмечаем ключ решённым
                        self.mem_keydir_insert_loc(
                            bucket,
                            k,
                            MemKeyLoc {
                                pid: NO_PAGE,
                                off: 0,
                            },
                        );
                        decided.insert(k.to_vec());
                        return;
                    }
                    // TTL: 0 — бессрочно; истёкшие записи пропускаем (ищем глубже)
                    let ttl_ok = expires_at_sec == 0 || now < expires_at_sec;
                    if ttl_ok {
                        // Present: записываем (pid, off) страницы с валидной записью
                        let off_u32 = (off_usize as u64).min(u32::MAX as u64) as u32;
                        self.mem_keydir_insert_loc(
                            bucket,
                            k,
                            MemKeyLoc {
                                pid: cur_pid,
                                off: off_u32,
                            },
                        );
                        decided.insert(k.to_vec());
                    }
                });

                pid = hdr.next_page_id;
            }
        }

        Ok(())
    }
}
