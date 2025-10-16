//! bloom/sidecar/ops — операции над BloomSidecar:
//! - rebuild_all / rebuild_bucket
//! - test
//! - update_bucket_bits
//! - set_last_lsn
//! - is_fresh_for_db
//!
//! Этот модуль подключён из mod.rs. Общие типы/константы/хелперы доступны через super::*.

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom, Write};

use crate::db::core::Db;
use crate::dir::NO_PAGE;
use crate::metrics::record_bloom_update; // NEW: метрика delta-update
use crate::page::{kv_header_read_v3, PAGE_MAGIC, PAGE_TYPE_KV_RH3, KV_HDR_MIN, TRAILER_LEN};
use crate::page::kv::kv_for_each_record;

use super::{
    BloomSidecar,
    bloom_cache_get, bloom_cache_put,
    lock_bloom_file,
};

impl BloomSidecar {
    /// Полная перестройка Bloom-файла по всей БД (синхронно).
    pub fn rebuild_all(&mut self, db: &Db) -> Result<()> {
        for b in 0..db.dir.bucket_count {
            self.rebuild_bucket(db, b)?;
        }
        self.set_last_lsn(db.pager.meta.last_lsn)?;
        self.reload_views()?;
        Ok(())
    }

    /// Перестроить Bloom-биты для одного бакета (safe-путь для single-record страниц).
    pub fn rebuild_bucket(&mut self, db: &Db, bucket: u32) -> Result<()> {
        if bucket >= self.meta.buckets {
            return Err(anyhow!(
                "bucket {} out of range 0..{}",
                bucket,
                self.meta.buckets - 1
            ));
        }

        let ps = db.pager.meta.page_size as usize;
        let now = crate::util::now_secs();

        enum State { Selected, Deleted }
        let mut state: HashMap<Vec<u8>, State> = HashMap::new();
        let mut page_buf = vec![0u8; ps];

        let mut pid = db.dir.head(bucket)?;
        while pid != NO_PAGE {
            db.pager.read_page(pid, &mut page_buf)?;
            if &page_buf[0..4] != PAGE_MAGIC {
                break;
            }
            let ptype = LittleEndian::read_u16(&page_buf[6..8]);
            if ptype != PAGE_TYPE_KV_RH3 {
                break;
            }
            let h = kv_header_read_v3(&page_buf)?;

            let mut touched = false;
            kv_for_each_record(&page_buf, |k, _v, expires_at_sec, vflags| {
                touched = true;
                if state.contains_key(k) { return; }
                let is_tomb = (vflags & 0x1) == 1;
                let ttl_ok = expires_at_sec == 0 || now < expires_at_sec;
                if is_tomb { state.insert(k.to_vec(), State::Deleted); }
                else if ttl_ok { state.insert(k.to_vec(), State::Selected); }
            });

            // Безопасный fallback: только если слотов нет и запись целиком в data-area.
            if !touched && h.table_slots == 0 {
                let data_end = ps.saturating_sub(TRAILER_LEN);
                let off = KV_HDR_MIN;
                // [klen u16][vlen u32][expires u32][vflags у8]
                if off + 11 <= data_end {
                    let klen = LittleEndian::read_u16(&page_buf[off..off + 2]) as usize;
                    let vlen = LittleEndian::read_u32(&page_buf[off + 2..off + 6]) as usize;
                    let expires_at_sec = LittleEndian::read_u32(&page_buf[off + 6..off + 10]);
                    let vflags = page_buf[off + 10];
                    let base = off + 11;
                    let end = base.saturating_add(klen).saturating_add(vlen);
                    if end <= data_end {
                        let key_slice = &page_buf[base..base + klen];
                        if !state.contains_key(key_slice) {
                            let is_tomb = (vflags & 0x1) == 1;
                            let ttl_ok = expires_at_sec == 0 || now < expires_at_sec;
                            if is_tomb { state.insert(key_slice.to_vec(), State::Deleted); }
                            else if ttl_ok { state.insert(key_slice.to_vec(), State::Selected); }
                        }
                    }
                }
            }

            pid = h.next_page_id;
        }

        // Сформируем биты и запишем
        let mut bits = vec![0u8; self.meta.bytes_per_bucket as usize];
        for (k, st) in state.into_iter() {
            if let State::Selected = st {
                self.add_key_to_bits(&k, &mut bits);
            }
        }

        let _lk = lock_bloom_file(&self.root)?;
        let mut f = OpenOptions::new().read(true).write(true).open(&self.path)?;
        let off = self.hdr_size_u64 + (bucket as u64) * (self.meta.bytes_per_bucket as u64);
        f.seek(SeekFrom::Start(off))?;
        f.write_all(&bits)?;
        let _ = f.sync_all();

        Ok(())
    }

    /// Тест по битам (из RAM/mmap/LRU/file).
    pub fn test(&self, bucket: u32, key: &[u8]) -> Result<bool> {
        if bucket >= self.meta.buckets {
            return Err(anyhow!(
                "bucket {} out of range 0..{}",
                bucket,
                self.meta.buckets - 1
            ));
        }

        // RAM
        if let Some(ref body) = self.body {
            let bpb = self.meta.bytes_per_bucket as usize;
            let start = (bucket as usize) * bpb;
            let end = start + bpb;
            if end <= body.len() {
                return Ok(self.test_in_bits(key, &body[start..end]));
            }
        }

        // MMAP (robust: поддержка отображения только body и “всего файла”)
        if let Some(ref mm) = self.mmap {
            let bpb = self.meta.bytes_per_bucket as usize;
            let total_body_len = (self.meta.buckets as usize)
                .saturating_mul(bpb);

            // Если mmap длиной ровно равен телу — это “старый” режим (map только body).
            // Если mmap длиннее (>= hdr + body) — это “новый” режим (map от offset=0).
            let base = if mm.len() >= self.hdr_size_usize + total_body_len {
                self.hdr_size_usize
            } else {
                0
            };

            let start = base + (bucket as usize) * bpb;
            let end = start + bpb;
            if end <= mm.len() {
                return Ok(self.test_in_bits(key, &mm[start..end]));
            }
        }

        // LRU
        if let Some(bits) = bloom_cache_get(&self.path, bucket, self.meta.last_lsn) {
            return Ok(self.test_in_bits(key, &bits));
        }

        // File fallback
        let mut bits = vec![0u8; self.meta.bytes_per_bucket as usize];
        let off = self.hdr_size_u64 + (bucket as u64) * (self.meta.bytes_per_bucket as u64);

        if let Some(ref mtx) = self.f_ro {
            let mut f = mtx.lock().map_err(|_| anyhow!("bloom ro handle poisoned"))?;
            f.seek(SeekFrom::Start(off))?;
            f.read_exact(&mut bits)?;
        } else {
            let mut f = OpenOptions::new().read(true).open(&self.path)?;
            f.seek(SeekFrom::Start(off))?;
            f.read_exact(&mut bits)?;
        }

        bloom_cache_put(&self.path, bucket, self.meta.last_lsn, bits.clone());
        Ok(self.test_in_bits(key, &bits))
    }

    /// Обновить last_lsn (v2) — под lock + перезагрузка view.
    pub fn set_last_lsn(&mut self, last_lsn: u64) -> Result<()> {
        if self.version != super::VERSION_V2 {
            self.meta.last_lsn = 0;
            return Ok(());
        }
        let _lk = lock_bloom_file(&self.root)?;
        let mut f = OpenOptions::new().read(true).write(true).open(&self.path)?;
        self.write_header_last_lsn_locked(&mut f, last_lsn)?;
        let _ = f.sync_all();
        self.meta.last_lsn = last_lsn;
        self.reload_views()?;
        Ok(())
    }

    /// Проверка свежести Bloom по last_lsn БД.
    #[inline]
    pub fn is_fresh_for_db(&self, db: &Db) -> bool {
        self.version == super::VERSION_V2 && self.meta.last_lsn == db.pager.meta.last_lsn
    }

    /// Delta‑update API: обновить биты бакета (keys) и last_lsn, затем обновить view.
    pub fn update_bucket_bits(&mut self, bucket: u32, keys: &[&[u8]], new_last_lsn: u64) -> Result<()> {
        if bucket >= self.meta.buckets {
            return Err(anyhow!(
                "bucket {} out of range 0..{}",
                bucket,
                self.meta.buckets - 1
            ));
        }

        let _lk = lock_bloom_file(&self.root)?;
        let mut f = OpenOptions::new().read(true).write(true).open(&self.path)?;

        // Read current bits
        let mut bits = vec![0u8; self.meta.bytes_per_bucket as usize];
        let off = self.hdr_size_u64 + (bucket as u64) * (self.meta.bytes_per_bucket as u64);
        f.seek(SeekFrom::Start(off))?;
        f.read_exact(&mut bits)?;

        // Apply keys
        for k in keys {
            if !k.is_empty() {
                self.add_key_to_bits(k, &mut bits);
            }
        }

        // Write bits
        f.seek(SeekFrom::Start(off))?;
        f.write_all(&bits)?;
        let _ = f.sync_all();

        // Update header.last_lsn (v2)
        if self.version == super::VERSION_V2 {
            self.write_header_last_lsn_locked(&mut f, new_last_lsn)?;
            let _ = f.sync_all();
            self.meta.last_lsn = new_last_lsn;
        }

        // Обновим RAM/mmap view и LRU
        self.reload_views()?;
        bloom_cache_put(&self.path, bucket, self.meta.last_lsn, bits);

        // NEW: учёт delta‑обновления Bloom — считаем байты на бакет
        record_bloom_update(self.meta.bytes_per_bucket as u64);

        Ok(())
    }
}