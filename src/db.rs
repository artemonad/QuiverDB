use crate::consts::{NO_PAGE, PAGE_HDR_V2_SIZE, FREE_FILE, FREE_HDR_SIZE, FREE_MAGIC};
use crate::dir::Directory;
use crate::lock::{acquire_exclusive_lock, acquire_shared_lock, LockGuard};
use crate::meta::set_clean_shutdown;
use crate::page_rh::{
    rh_header_read, rh_header_write, rh_kv_delete_inplace, rh_kv_insert,
    rh_kv_lookup, rh_page_init, rh_page_is_kv, RH_SLOT_SIZE,
};
use crate::pager::Pager;
use crate::page_ovf::{
    ovf_free_chain, ovf_make_placeholder, ovf_parse_placeholder, ovf_read_chain, ovf_write_chain,
    ovf_header_read,
};
use crate::free::FreeList;
use crate::wal::{wal_replay_if_any, Wal, WalGroupCfg};
use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashSet;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

// New: centralized configuration/builder
use crate::config::{DbBuilder, QuiverConfig};

// New: subscriptions
use crate::subs::{SubRegistry, SubscriptionHandle, Event, callback};

// New: maintenance module (delegation)
use crate::db_maintenance;

// New: KV put-in-chain delegation
use crate::db_kv;

pub struct Db {
    pub root: std::path::PathBuf,
    pub pager: Pager,
    pub dir: Directory,
    _lock: LockGuard,
    readonly: bool,
    /// New: runtime configuration (built from env or explicit builder)
    pub cfg: QuiverConfig,
    /// New: in-process subscriptions registry
    pub subs: std::sync::Arc<SubRegistry>,
}

impl Db {
    /// New: builder entry point
    pub fn builder() -> DbBuilder {
        DbBuilder::new()
    }

    /// New: subscribe for live events by key prefix.
    /// The callback is called synchronously by writer after successful commit_page.
    pub fn subscribe_prefix<F>(&self, prefix: Vec<u8>, cb: F) -> SubscriptionHandle
    where
        F: Fn(&Event) + Send + Sync + 'static,
    {
        self.subs.subscribe(prefix, callback(cb))
    }

    /// New: open writer with explicit config (exclusive lock)
    pub fn open_with_config(root: &std::path::Path, cfg: QuiverConfig) -> Result<Self> {
        let lock = acquire_exclusive_lock(root)?;
        // group-commit settings from config
        Wal::set_group_config(root, WalGroupCfg { coalesce_ms: cfg.wal_coalesce_ms })?;
        wal_replay_if_any(root)?;
        let pager = Pager::open_with_config(root, &cfg)?;
        let dir = Directory::open(root)?;
        set_clean_shutdown(root, false)?;
        Ok(Self {
            root: root.to_path_buf(),
            pager,
            dir,
            _lock: lock,
            readonly: false,
            cfg,
            subs: SubRegistry::new(),
        })
    }

    /// New: open reader (shared lock) with explicit config
    pub fn open_ro_with_config(root: &std::path::Path, cfg: QuiverConfig) -> Result<Self> {
        let lock = acquire_shared_lock(root)?;
        let pager = Pager::open_with_config(root, &cfg)?;
        let dir = Directory::open(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            pager,
            dir,
            _lock: lock,
            readonly: true,
            cfg,
            subs: SubRegistry::new(),
        })
    }

    /// Открыть БД для записи: exclusive lock, wal_replay, clean_shutdown=false.
    /// Backward-compatible: loads config from env.
    pub fn open(root: &std::path::Path) -> Result<Self> {
        let cfg = QuiverConfig::from_env();
        Self::open_with_config(root, cfg)
    }

    /// Открыть БД «только чтение»: shared lock, без replay, без изменения clean_shutdown.
    /// Backward-compatible: loads config from env.
    pub fn open_ro(root: &std::path::Path) -> Result<Self> {
        let cfg = QuiverConfig::from_env();
        Self::open_ro_with_config(root, cfg)
    }

    /// Helper: publish put event (writer only)
    fn publish_put_event(&self, key: &[u8], value: &[u8]) {
        if self.readonly {
            return;
        }
        let ev = Event {
            key: key.to_vec(),
            value: Some(value.to_vec()),
            lsn: self.pager.meta.last_lsn,
        };
        self.subs.publish(&ev);
    }

    /// Helper: publish delete event (writer only)
    fn publish_del_event(&self, key: &[u8]) {
        if self.readonly {
            return;
        }
        let ev = Event {
            key: key.to_vec(),
            value: None,
            lsn: self.pager.meta.last_lsn,
        };
        self.subs.publish(&ev);
    }

    pub fn put(&mut self, key: &[u8], val: &[u8]) -> Result<()> {
        let ps = self.pager.meta.page_size as usize;

        // New: overflow threshold from config if provided; otherwise default = ps/4
        let ovf_threshold = self
            .cfg
            .ovf_threshold_bytes
            .unwrap_or(ps / 4);

        if key.len() > u16::MAX as usize {
            return Err(anyhow!("key too large for u16 length: {} bytes", key.len()));
        }

        let rec_sz_inline = 4 + key.len() + val.len();
        let rec_fits_inline = rec_sz_inline + RH_SLOT_SIZE + PAGE_HDR_V2_SIZE <= ps;
        let need_overflow = val.len() > ovf_threshold || !rec_fits_inline;

        let bucket = self.dir.bucket_of_key(key);
        let head = self.dir.head(bucket)?;

        if head == NO_PAGE {
            let new_pid = self.pager.allocate_one_page()?;
            let mut buf = vec![0u8; ps];
            rh_page_init(&mut buf, new_pid)?;

            if need_overflow {
                let ovf_head = ovf_write_chain(&mut self.pager, val)?;
                let placeholder = ovf_make_placeholder(val.len() as u64, ovf_head);
                let ok = rh_kv_insert(&mut buf, self.dir.hash_kind, key, &placeholder)?;
                if !ok {
                    ovf_free_chain(&self.pager, ovf_head)?;
                    return Err(anyhow!("empty page cannot fit overflow placeholder"));
                }
            } else {
                let ok = rh_kv_insert(&mut buf, self.dir.hash_kind, key, val)?;
                if !ok {
                    return Err(anyhow!("empty page cannot fit inline record"));
                }
            }

            self.pager.commit_page(new_pid, &mut buf)?;
            self.dir.set_head(bucket, new_pid)?;
            // Publish event once after successful commit + head update
            self.publish_put_event(key, val);
            return Ok(());
        }

        // Delegation to db_kv module
        db_kv::put_in_chain(self, bucket, head, key, val, need_overflow)
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let bucket = self.dir.bucket_of_key(key);
        let mut pid = self.dir.head(bucket)?;
        if pid == NO_PAGE {
            return Ok(None);
        }
        let ps = self.pager.meta.page_size as usize;

        // Возвращаем последнюю встреченную версию по порядку цепочки (tail wins).
        let mut best: Option<Vec<u8>> = None;

        while pid != NO_PAGE {
            let mut buf = vec![0u8; ps];
            self.pager.read_page(pid, &mut buf)?;
            if !rh_page_is_kv(&buf) {
                return Err(anyhow!("page {} is not KV-RH (v2) page", pid));
            }
            let h = rh_header_read(&buf)?;
            if let Some(v) = rh_kv_lookup(&buf, self.dir.hash_kind, key)? {
                let val = if let Some((total_len, head_pid)) = ovf_parse_placeholder(&v) {
                    ovf_read_chain(&self.pager, head_pid, Some(total_len as usize))?
                } else {
                    v
                };
                best = Some(val);
            }
            pid = h.next_page_id;
        }

        Ok(best)
    }

    pub fn del(&mut self, key: &[u8]) -> Result<bool> {
        if self.readonly {
            return Err(anyhow!("Db is read-only (opened with open_ro)"));
        }

        let bucket = self.dir.bucket_of_key(key);
        let head = self.dir.head(bucket)?;
        if head == NO_PAGE {
            return Ok(false);
        }
        let ps = self.pager.meta.page_size as usize;
        let mut existed_any = false;

        let mut prev: u64 = NO_PAGE;
        let mut pid: u64 = head;

        while pid != NO_PAGE {
            let mut buf = vec![0u8; ps];
            self.pager.read_page(pid, &mut buf)?;
            if !rh_page_is_kv(&buf) {
                return Err(anyhow!("page {} is not KV-RH (v2) page", pid));
            }

            let ovf_to_free = rh_kv_lookup(&buf, self.dir.hash_kind, key)?
                .and_then(|v| ovf_parse_placeholder(&v).map(|(_, h)| h));

            let deleted = rh_kv_delete_inplace(&mut buf, self.dir.hash_kind, key)?;
            if deleted {
                existed_any = true;
                self.pager.commit_page(pid, &mut buf)?;
                if let Some(ovf_head) = ovf_to_free {
                    ovf_free_chain(&self.pager, ovf_head)?;
                }
            }

            let h = rh_header_read(&buf)?;
            let next = h.next_page_id;

            if h.used_slots == 0 {
                if prev == NO_PAGE {
                    self.dir.set_head(bucket, next)?;
                } else {
                    let mut pbuf = vec![0u8; ps];
                    self.pager.read_page(prev, &mut pbuf)?;
                    let mut ph = rh_header_read(&pbuf)?;
                    ph.next_page_id = next;
                    rh_header_write(&mut pbuf, &ph)?;
                    self.pager.commit_page(prev, &mut pbuf)?;
                }
                self.pager.free_page(pid)?;
            } else {
                prev = pid;
            }

            pid = next;
        }

        self.sweep_orphan_overflow()?;

        // Publish single delete event if the key existed
        if existed_any {
            self.publish_del_event(key);
        }

        Ok(existed_any)
    }

    fn sweep_orphan_overflow(&self) -> Result<()> {
        if self.readonly {
            // В RO режиме не трогаем пространство.
            return Ok(());
        }
        db_maintenance::sweep_orphan_overflow_writer(self)
    }

    fn read_free_set(&self) -> Result<HashSet<u64>> {
        db_maintenance::read_free_set(self)
    }

    pub fn print_stats(&self) -> Result<()> {
        db_maintenance::print_stats(self)
    }
}

impl Drop for Db {
    fn drop(&mut self) {
        if !self.readonly {
            let _ = self.sweep_orphan_overflow();
            let _ = set_clean_shutdown(&self.root, true);
        }
    }
}