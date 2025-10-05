use crate::consts::{NO_PAGE, PAGE_HDR_V2_SIZE, FREE_FILE, FREE_HDR_SIZE, FREE_MAGIC};
use crate::dir::Directory;
use crate::lock::{acquire_exclusive_lock, acquire_shared_lock, LockGuard};
use crate::meta::set_clean_shutdown;
use crate::page_rh::{
    rh_compact_inplace, rh_header_read, rh_header_write, rh_kv_delete_inplace, rh_kv_insert,
    rh_kv_lookup, rh_page_init, rh_page_is_kv, rh_should_compact, RH_SLOT_SIZE,
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

/// New: centralized configuration/builder
use crate::config::{DbBuilder, QuiverConfig};

/// New: subscriptions
use crate::subs::{SubRegistry, SubscriptionHandle, Event, callback};

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

        self.put_in_chain(bucket, head, key, val, need_overflow)
    }

    fn put_in_chain(
        &mut self,
        bucket: u32,
        head: u64,
        key: &[u8],
        val: &[u8],
        need_overflow: bool,
    ) -> Result<()> {
        let ps = self.pager.meta.page_size as usize;

        // Создаём overflow один раз перед обходом и переиспользуем.
        let (new_value_bytes, new_ovf_head_opt) = if need_overflow {
            let ovf_head = ovf_write_chain(&mut self.pager, val)?;
            (
                ovf_make_placeholder(val.len() as u64, ovf_head).to_vec(),
                Some(ovf_head),
            )
        } else {
            (val.to_vec(), None)
        };

        let mut prev: u64 = NO_PAGE;
        let mut pid: u64 = head;

        macro_rules! cleanup_on_error {
            () => {
                if let Some(h) = new_ovf_head_opt {
                    ovf_free_chain(&self.pager, h)?;
                }
            };
        }

        loop {
            let mut buf = vec![0u8; ps];
            if let Err(e) = self.pager.read_page(pid, &mut buf) {
                cleanup_on_error!();
                return Err(e);
            }
            if !rh_page_is_kv(&buf) {
                cleanup_on_error!();
                return Err(anyhow!("page {} is not KV-RH (v2) page", pid));
            }

            let existing_val = rh_kv_lookup(&buf, self.dir.hash_kind, key)?;
            let old_ovf_head = existing_val.and_then(|v| ovf_parse_placeholder(&v).map(|(_, h)| h));

            // Попытка in-place вставки/обновления
            if rh_kv_insert(&mut buf, self.dir.hash_kind, key, &new_value_bytes)? {
                self.pager.commit_page(pid, &mut buf)?;

                // Освободим старую overflow-цепочку (если была и отличается от новой)
                if let Some(old_h) = old_ovf_head {
                    if let Some(new_h) = new_ovf_head_opt {
                        if new_h != old_h {
                            ovf_free_chain(&self.pager, old_h)?;
                        }
                    } else {
                        ovf_free_chain(&self.pager, old_h)?;
                    }
                }

                // Дополнительно: зачистим дубликаты ключа на последующих страницах цепочки
                // и вырежем пустые страницы из цепочки.
                let h_after = rh_header_read(&buf)?; // свежий next_page_id
                let mut prev2 = pid;
                let mut cur = h_after.next_page_id;
                while cur != NO_PAGE {
                    let mut buf2 = vec![0u8; ps];
                    self.pager.read_page(cur, &mut buf2)?;
                    if !rh_page_is_kv(&buf2) {
                        return Err(anyhow!("page {} is not KV-RH (v2) page", cur));
                    }

                    if let Some(vdup) = rh_kv_lookup(&buf2, self.dir.hash_kind, key)? {
                        if let Some((_, head_pid)) = ovf_parse_placeholder(&vdup) {
                            ovf_free_chain(&self.pager, head_pid)?;
                        }
                        let existed = rh_kv_delete_inplace(&mut buf2, self.dir.hash_kind, key)?;
                        if existed {
                            self.pager.commit_page(cur, &mut buf2)?;
                        }
                    }

                    let h2 = rh_header_read(&buf2)?;
                    let next2 = h2.next_page_id;
                    if h2.used_slots == 0 {
                        // Вырезаем пустую страницу из цепочки
                        let mut pbuf = vec![0u8; ps];
                        self.pager.read_page(prev2, &mut pbuf)?;
                        let mut ph = rh_header_read(&pbuf)?;
                        ph.next_page_id = next2;
                        rh_header_write(&mut pbuf, &ph)?;
                        self.pager.commit_page(prev2, &mut pbuf)?;
                        self.pager.free_page(cur)?;
                        cur = next2;
                        continue;
                    } else {
                        prev2 = cur;
                        cur = next2;
                    }
                }

                // Publish put event once per operation
                self.publish_put_event(key, val);
                return Ok(());
            }

            // Компактификация и повтор
            if rh_should_compact(&buf)? {
                rh_compact_inplace(&mut buf, self.dir.hash_kind)?;
                if rh_kv_insert(&mut buf, self.dir.hash_kind, key, &new_value_bytes)? {
                    self.pager.commit_page(pid, &mut buf)?;
                    if let Some(old_h) = old_ovf_head {
                        if let Some(new_h) = new_ovf_head_opt {
                            if new_h != old_h {
                                ovf_free_chain(&self.pager, old_h)?;
                            }
                        } else {
                            ovf_free_chain(&self.pager, old_h)?;
                        }
                    }

                    // Зачистка дубликатов на последующих страницах (как после in-place)
                    let h_after = rh_header_read(&buf)?; // next_page_id
                    let mut prev2 = pid;
                    let mut cur = h_after.next_page_id;
                    while cur != NO_PAGE {
                        let mut buf2 = vec![0u8; ps];
                        self.pager.read_page(cur, &mut buf2)?;
                        if !rh_page_is_kv(&buf2) {
                            return Err(anyhow!("page {} is not KV-RH (v2) page", cur));
                        }

                        if let Some(vdup) = rh_kv_lookup(&buf2, self.dir.hash_kind, key)? {
                            if let Some((_, head_pid)) = ovf_parse_placeholder(&vdup) {
                                ovf_free_chain(&self.pager, head_pid)?;
                            }
                            let existed =
                                rh_kv_delete_inplace(&mut buf2, self.dir.hash_kind, key)?;
                            if existed {
                                self.pager.commit_page(cur, &mut buf2)?;
                            }
                        }

                        let h2 = rh_header_read(&buf2)?;
                        let next2 = h2.next_page_id;
                        if h2.used_slots == 0 {
                            let mut pbuf = vec![0u8; ps];
                            self.pager.read_page(prev2, &mut pbuf)?;
                            let mut ph = rh_header_read(&pbuf)?;
                            ph.next_page_id = next2;
                            rh_header_write(&mut pbuf, &ph)?;
                            self.pager.commit_page(prev2, &mut pbuf)?;
                            self.pager.free_page(cur)?;
                            cur = next2;
                            continue;
                        } else {
                            prev2 = cur;
                            cur = next2;
                        }
                    }

                    // Publish after successful commit
                    self.publish_put_event(key, val);
                    return Ok(());
                }
            }

            // Удаление пустых страниц из цепочки
            let h = rh_header_read(&buf)?;
            if h.used_slots == 0 {
                let next = h.next_page_id;
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
                if next == NO_PAGE {
                    break;
                }
                pid = next;
                continue;
            }

            // Корректно запомнить хвост
            if h.next_page_id == NO_PAGE {
                prev = pid; // pid — реальный хвост
                break;
            }
            prev = pid;
            pid = h.next_page_id;
        }

        // Не нашли места — создаём новую страницу и вставляем запись
        let new_pid = self.pager.allocate_one_page()?;
        let mut newb = vec![0u8; ps];
        rh_page_init(&mut newb, new_pid)?;

        if !rh_kv_insert(&mut newb, self.dir.hash_kind, key, &new_value_bytes)? {
            // Откат новой цепочки (если была)
            if let Some((_, ovf_head)) = ovf_parse_placeholder(&new_value_bytes) {
                ovf_free_chain(&self.pager, ovf_head)?;
            }
            return Err(anyhow!("empty v2 page cannot fit record"));
        }
        self.pager.commit_page(new_pid, &mut newb)?;

        // Пришивка к цепочке
        if prev == NO_PAGE {
            self.dir.set_head(bucket, new_pid)?;
        } else {
            let mut tailb = vec![0u8; ps];
            self.pager.read_page(prev, &mut tailb)?;
            let mut th = rh_header_read(&tailb)?;
            th.next_page_id = new_pid;
            rh_header_write(&mut tailb, &th)?;
            self.pager.commit_page(prev, &mut tailb)?;
        }

        // После вставки новой версии в хвост — удалим старые копии ключа на предыдущих страницах,
        // вырезая пустые страницы, чтобы не копить мусор.
        {
            let mut prev2 = NO_PAGE;
            let mut cur = self.dir.head(bucket)?;
            while cur != NO_PAGE && cur != new_pid {
                let mut buf2 = vec![0u8; ps];
                self.pager.read_page(cur, &mut buf2)?;
                if !rh_page_is_kv(&buf2) {
                    return Err(anyhow!("page {} is not KV-RH (v2) page", cur));
                }

                if let Some(vdup) = rh_kv_lookup(&buf2, self.dir.hash_kind, key)? {
                    if let Some((_, head_pid)) = ovf_parse_placeholder(&vdup) {
                        ovf_free_chain(&self.pager, head_pid)?;
                    }
                    let existed = rh_kv_delete_inplace(&mut buf2, self.dir.hash_kind, key)?;
                    if existed {
                        self.pager.commit_page(cur, &mut buf2)?;
                    }
                }

                let h2 = rh_header_read(&buf2)?;
                let next2 = h2.next_page_id;
                if h2.used_slots == 0 {
                    // Вырезаем пустую страницу из цепочки
                    if prev2 == NO_PAGE {
                        self.dir.set_head(bucket, next2)?;
                    } else {
                        let mut pbuf = vec![0u8; ps];
                        self.pager.read_page(prev2, &mut pbuf)?;
                        let mut ph = rh_header_read(&pbuf)?;
                        ph.next_page_id = next2;
                        rh_header_write(&mut pbuf, &ph)?;
                        self.pager.commit_page(prev2, &mut pbuf)?;
                    }
                    self.pager.free_page(cur)?;
                    cur = next2;
                    continue;
                } else {
                    prev2 = cur;
                    cur = next2;
                }
            }
        }

        // Publish put event for appended case
        self.publish_put_event(key, val);

        Ok(())
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

        let ps = self.pager.meta.page_size as usize;
        let pages_alloc = self.pager.meta.next_page_id;
        let free_set = self.read_free_set()?;

        // метрика: запуск sweep
        crate::metrics::record_sweep_orphan_run();

        // Сбор голов overflow-цепей из реальных цепочек каталога.
        let mut heads: Vec<u64> = Vec::new();
        for b in 0..self.dir.bucket_count {
            let mut pid = self.dir.head(b)?;
            while pid != NO_PAGE {
                let mut buf = vec![0u8; ps];
                if self.pager.read_page(pid, &mut buf).is_err() {
                    break;
                }
                if !rh_page_is_kv(&buf) {
                    break;
                }
                if let Ok(items) = crate::page_rh::rh_kv_list(&buf) {
                    for (_k, v) in items {
                        if let Some((_, head_pid)) = ovf_parse_placeholder(&v) {
                            heads.push(head_pid);
                        }
                    }
                }
                let h = rh_header_read(&buf)?;
                pid = h.next_page_id;
            }
        }

        if heads.is_empty() {
            // Консервативно: ничего не удаляем.
            return Ok(());
        }

        // Mark
        let mut marked: HashSet<u64> = HashSet::new();
        for head_pid in heads {
            let mut cur = head_pid;
            while cur != NO_PAGE {
                if marked.contains(&cur) {
                    break;
                }
                let mut buf = vec![0u8; ps];
                if self.pager.read_page(cur, &mut buf).is_err() {
                    break;
                }
                if let Ok(h) = ovf_header_read(&buf) {
                    marked.insert(cur);
                    cur = h.next_page_id;
                } else {
                    break;
                }
            }
        }

        // Sweep
        for pid in 0..pages_alloc {
            if free_set.contains(&pid) {
                continue;
            }
            let mut buf = vec![0u8; ps];
            if self.pager.read_page(pid, &mut buf).is_ok() {
                if ovf_header_read(&buf).is_ok() && !marked.contains(&pid) {
                    self.pager.free_page(pid)?;
                }
            }
        }
        Ok(())
    }

    fn read_free_set(&self) -> Result<HashSet<u64>> {
        let mut set = HashSet::new();
        let path = self.root.join(FREE_FILE);
        if !path.exists() {
            return Ok(set);
        }
        let mut f = OpenOptions::new().read(true).open(&path)?;
        let mut magic = [0u8; 8];
        f.read_exact(&mut magic)?;
        if &magic != FREE_MAGIC {
            return Ok(set);
        }
        let mut hdr = vec![0u8; FREE_HDR_SIZE - 8];
        f.read_exact(&mut hdr)?;
        let len = f.metadata()?.len();
        let mut pos = FREE_HDR_SIZE as u64;
        while pos + 8 <= len {
            let mut buf8 = [0u8; 8];
            f.seek(SeekFrom::Start(pos))?;
            f.read_exact(&mut buf8)?;
            let pid = LittleEndian::read_u64(&buf8);
            set.insert(pid);
            pos += 8;
        }
        Ok(set)
    }

    pub fn print_stats(&self) -> Result<()> {
        let ps = self.pager.meta.page_size;
        let pages = self.pager.meta.next_page_id;
        let buckets = self.dir.bucket_count;
        let used = self.dir.count_used_buckets()?;

        let mut min_chain = u64::MAX;
        let mut max_chain = 0u64;
        let mut sum_chain = 0u64;

        for b in 0..buckets {
            let mut len = 0u64;
            let mut pid = self.dir.head(b)?;
            while pid != NO_PAGE {
                len += 1;
                let mut buf = vec![0u8; ps as usize];
                self.pager.read_page(pid, &mut buf)?;
                if !rh_page_is_kv(&buf) {
                    return Err(anyhow!("page {} is not KV-RH (v2) page", pid));
                }
                let h = rh_header_read(&buf)?;
                pid = h.next_page_id;
            }
            if len > 0 {
                min_chain = min_chain.min(len);
                max_chain = max_chain.max(len);
                sum_chain += len;
            }
        }
        let non_empty = used as u64;
        let avg_chain = if non_empty > 0 {
            sum_chain as f64 / (non_empty as f64)
        } else {
            0.0
        };

        let free_set = self.read_free_set()?;
        let mut ovf_pages = 0u64;
        let mut ovf_bytes = 0u64;
        for pid in 0..pages {
            if free_set.contains(&pid) {
                continue;
            }
            let mut buf = vec![0u8; ps as usize];
            if self.pager.read_page(pid, &mut buf).is_ok() {
                if let Ok(h) = ovf_header_read(&buf) {
                    ovf_pages += 1;
                    ovf_bytes += h.chunk_len as u64;
                }
            }
        }
        let free_pages = match FreeList::open(&self.root) {
            Ok(fl) => fl.count().unwrap_or(0),
            Err(_) => 0,
        };

        println!("DB stats:");
        println!("  page_size        = {}", ps);
        println!("  pages_allocated  = {}", pages);
        println!("  buckets          = {}", buckets);
        println!("  used_buckets     = {}", used);
        if non_empty > 0 {
            println!(
                "  chain_len min/avg/max = {}/{:.2}/{}",
                min_chain, avg_chain, max_chain
            );
        } else {
            println!("  chain_len min/avg/max = n/a");
        }
        println!("  overflow_pages    = {}", ovf_pages);
        println!("  overflow_bytes    = {}", ovf_bytes);
        println!("  free_pages        = {}", free_pages);

        // ----- Metrics snapshot -----
        let m = crate::metrics::snapshot();
        let cache_total = m.page_cache_hits + m.page_cache_misses;
        let cache_hit_ratio = if cache_total > 0 {
            (m.page_cache_hits as f64) / (cache_total as f64)
        } else {
            0.0
        };

        println!("Metrics:");
        println!("  wal_appends_total       = {}", m.wal_appends_total);
        println!("  wal_bytes_written       = {}", m.wal_bytes_written);
        println!("  wal_fsync_calls         = {}", m.wal_fsync_calls);
        println!("  wal_avg_batch_pages     = {:.2}", m.avg_wal_batch_pages());
        println!("  wal_truncations         = {}", m.wal_truncations);
        println!("  page_cache_hits         = {}", m.page_cache_hits);
        println!("  page_cache_misses       = {}", m.page_cache_misses);
        println!("  page_cache_hit_ratio    = {:.2}%", cache_hit_ratio * 100.0);
        println!("  rh_page_compactions     = {}", m.rh_page_compactions);
        println!("  overflow_chains_created = {}", m.overflow_chains_created);
        println!("  overflow_chains_freed   = {}", m.overflow_chains_freed);
        println!("  sweep_orphan_runs       = {}", m.sweep_orphan_runs);

        Ok(())
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