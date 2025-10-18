use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashMap;

use crate::dir::NO_PAGE;
use crate::page::kv::kv_for_each_record; // packed-aware обход “новые → старые”
use crate::page::kv_pack::{KvPackItem, KvPagePacker};
use crate::page::{kv_header_read_v3, KV_HDR_MIN, PAGE_MAGIC, PAGE_TYPE_KV_RH3, TRAILER_LEN};
use crate::util::now_secs;
// Bloom side-car для delta-update после компактации
use crate::bloom::BloomSidecar;
// NEW: метрики компактации
use crate::metrics::{
    record_compaction_keys_deleted, record_compaction_keys_selected, record_compaction_pages_packed,
};

use super::core::Db;

#[derive(Debug, Default, Clone)]
pub struct CompactBucketReport {
    pub bucket: u32,
    pub old_chain_len: u64,
    pub keys_kept: u64,
    pub keys_deleted: u64,
    pub pages_written: u64,
    pub new_head: u64,
}

#[derive(Debug, Default, Clone)]
pub struct CompactSummary {
    pub buckets_total: u32,
    pub buckets_compacted: u32,
    pub old_chain_len_sum: u64,
    pub keys_kept_sum: u64,
    pub keys_deleted_sum: u64,
    pub pages_written_sum: u64,
}

impl Db {
    /// Быстрая односканная компактация одного бакета:
    /// - Проход head→tail, на каждой странице “новые→старые” записи (kv_for_each_record).
    /// - Для ключа принимается первое валидное (не tombstone, не истёкшее) вхождение.
    /// - Значения не разворачиваются: OVERFLOW placeholder переносится как есть.
    /// - Результат упаковывается в KV‑страницы через KvPagePacker (несколько записей на страницу).
    /// - После коммита выполняется Bloom delta‑update по валидным ключам и выставляется fresh last_lsn.
    /// - NEW: метрики компактации (выбранные/удалённые ключи и упакованные страницы).
    pub fn compact_bucket(&mut self, bucket: u32) -> Result<CompactBucketReport> {
        let mut rep = CompactBucketReport {
            bucket,
            ..Default::default()
        };

        let head = self.dir.head(bucket)?;
        if head == NO_PAGE {
            return Ok(rep);
        }

        let ps = self.pager.meta.page_size as usize;
        let now = now_secs();

        // Сбор финального состояния ключей в один проход:
        // key -> Some(value bytes) (Selected) или None (Deleted).
        let mut final_map: HashMap<Vec<u8>, Option<Vec<u8>>> = HashMap::new();

        let mut pid = head;
        let mut page = vec![0u8; ps];

        while pid != NO_PAGE {
            self.pager.read_page(pid, &mut page)?;
            if &page[0..4] != PAGE_MAGIC {
                break;
            }
            let ptype = LittleEndian::read_u16(&page[6..8]);
            if ptype != PAGE_TYPE_KV_RH3 {
                break;
            }

            rep.old_chain_len = rep.old_chain_len.saturating_add(1);

            let h = kv_header_read_v3(&page)?;
            // Обходим записи "новые → старые".
            let mut touched = false;
            kv_for_each_record(&page, |k, v, expires_at_sec, vflags| {
                touched = true;
                // Если уже принято решение по ключу — пропускаем.
                if final_map.contains_key(k) {
                    return;
                }

                let is_tomb = (vflags & 0x1) == 1;
                if is_tomb {
                    // Tombstone имеет приоритет — фиксируем удаление
                    final_map.insert(k.to_vec(), None);
                    return;
                }

                // TTL: 0 — бессрочно; если истёк — ищем глубже (ничего не пишем в map).
                let ttl_ok = expires_at_sec == 0 || now < expires_at_sec;
                if ttl_ok {
                    // Валидное значение — переносим как есть (включая плейсхолдер OVERFLOW).
                    final_map.insert(k.to_vec(), Some(v.to_vec()));
                }
            });

            // Безопасный fallback для single-record страницы (только если слотов нет):
            // читаем запись вручную, проверяя границы data-area.
            if !touched && h.table_slots == 0 {
                let data_end = ps.saturating_sub(TRAILER_LEN);
                let off = KV_HDR_MIN;
                // [klen u16][vlen u32][expires u32][vflags u8]
                if off + 11 <= data_end {
                    let klen = LittleEndian::read_u16(&page[off..off + 2]) as usize;
                    let vlen = LittleEndian::read_u32(&page[off + 2..off + 6]) as usize;
                    let expires_at_sec = LittleEndian::read_u32(&page[off + 6..off + 10]);
                    let vflags = page[off + 10];
                    let base = off + 11;
                    let end = base.saturating_add(klen).saturating_add(vlen);
                    if end <= data_end {
                        let key = &page[base..base + klen];
                        if !final_map.contains_key(key) {
                            let is_tomb = (vflags & 0x1) == 1;
                            if is_tomb {
                                final_map.insert(key.to_vec(), None);
                            } else {
                                let ttl_ok = expires_at_sec == 0 || now < expires_at_sec;
                                if ttl_ok {
                                    let val = &page[base + klen..base + klen + vlen];
                                    final_map.insert(key.to_vec(), Some(val.to_vec()));
                                }
                            }
                        }
                    }
                }
            }

            pid = h.next_page_id;
        }

        // Подсчёты итогов (kept/deleted)
        let mut keys_kept = 0u64;
        let mut keys_deleted = 0u64;
        for (_k, st) in final_map.iter() {
            match st {
                Some(_) => keys_kept += 1,
                None => keys_deleted += 1,
            }
        }
        rep.keys_kept = keys_kept;
        rep.keys_deleted = keys_deleted;

        // Метрики компактации: итоговое число выбранных/удалённых ключей
        record_compaction_keys_selected(keys_kept);
        record_compaction_keys_deleted(keys_deleted);

        // Если не осталось валидных значений — head = NO_PAGE.
        if keys_kept == 0 {
            self.dir.set_head(bucket, NO_PAGE)?;
            rep.new_head = NO_PAGE;
            return Ok(rep);
        }

        // Упаковываем валидные пары в KV‑страницы через KvPagePacker.
        let mut packer = KvPagePacker::new(ps);
        let mut pages: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut current_head: u64 = NO_PAGE;

        // Для стабильности порядка отсортируем ключи по лексикографическому порядку.
        let mut selected: Vec<(Vec<u8>, Vec<u8>)> = final_map
            .into_iter()
            .filter_map(|(k, st)| st.map(|v| (k, v)))
            .collect();
        selected.sort_unstable_by(|a, b| a.0.cmp(&b.0));

        // Помощник: сбросить packer в новую страницу и подвесить к текущей голове.
        let flush_page = |packer: &mut KvPagePacker,
                          pages_acc: &mut Vec<(u64, Vec<u8>)>,
                          cur_head: &mut u64,
                          db: &mut Db|
         -> Result<()> {
            if packer.is_empty() {
                return Ok(());
            }
            let pid_new = db.pager.allocate_one_page()?;
            let page = packer.finalize_into_page(pid_new, *cur_head, 0 /*codec_id*/)?;
            pages_acc.push((pid_new, page));
            *cur_head = pid_new;
            // Метрика: одна упакованная страница
            record_compaction_pages_packed(1);
            Ok(())
        };

        for (k, vbytes) in selected.iter() {
            let item = KvPackItem {
                key: k.clone(),
                value: vbytes.clone(),
                expires_at_sec: 0,
                vflags: 0,
            };
            if !packer.try_add(item) {
                // Текущая страница переполнена — сбросим и начнём новую
                flush_page(&mut packer, &mut pages, &mut current_head, self)?;
                // После flush элемент должен поместиться (иначе это сверхстраничная запись — маловероятно)
                let item2 = KvPackItem {
                    key: k.clone(),
                    value: vbytes.clone(),
                    expires_at_sec: 0,
                    vflags: 0,
                };
                if !packer.try_add(item2) {
                    // Чрезвычайно редкий случай: запись не помещается даже на пустую страницу.
                    // Сформируем одиночную KV‑страницу вручную.
                    let pid_new = self.pager.allocate_one_page()?;
                    let mut page = vec![0u8; ps];
                    crate::page::kv_init_v3(&mut page, pid_new, 0)?;
                    self.write_single_record_kv_page(&mut page, k, vbytes)?;
                    {
                        let mut h = kv_header_read_v3(&mut page)?;
                        h.next_page_id = current_head;
                        crate::page::kv_header_write_v3(&mut page, &h)?;
                    }
                    pages.push((pid_new, page));
                    current_head = pid_new;
                    // Метрика: одна упакованная страница (одиночная)
                    record_compaction_pages_packed(1);
                }
            }
        }

        // Сбросим хвост packer, если там есть данные
        flush_page(&mut packer, &mut pages, &mut current_head, self)?;

        // Коммит одним батчем + обновление головы
        let mut for_commit: Vec<(u64, &mut [u8])> = Vec::with_capacity(pages.len());
        for (pid, buf) in pages.iter_mut() {
            for_commit.push((*pid, buf.as_mut_slice()));
        }
        let updates = vec![(bucket, current_head)];
        self.pager
            .commit_pages_batch_with_heads(&mut for_commit, &updates)?;
        self.dir.set_head(bucket, current_head)?;

        rep.pages_written = for_commit.len() as u64;
        rep.new_head = current_head;

        // Bloom delta‑update — отмечаем все оставшиеся ключи и делаем фильтр “fresh”.
        if !selected.is_empty() {
            if let Ok(mut sidecar) = BloomSidecar::open_or_create_for_db(self, 4096, 6) {
                let new_lsn = self.pager.meta.last_lsn;
                let key_slices: Vec<&[u8]> = selected.iter().map(|(k, _)| k.as_slice()).collect();
                let _ = sidecar.update_bucket_bits(bucket, &key_slices, new_lsn);
            }
        }

        Ok(rep)
    }

    /// Компактация всей БД.
    pub fn compact_all(&mut self) -> Result<CompactSummary> {
        let mut sum = CompactSummary {
            buckets_total: self.dir.bucket_count,
            ..Default::default()
        };
        for b in 0..self.dir.bucket_count {
            let head = self.dir.head(b)?;
            if head == NO_PAGE {
                continue;
            }
            let rep = self.compact_bucket(b)?;
            if rep.old_chain_len > 0 {
                sum.buckets_compacted += 1;
            }
            sum.old_chain_len_sum += rep.old_chain_len;
            sum.keys_kept_sum += rep.keys_kept;
            sum.keys_deleted_sum += rep.keys_deleted;
            sum.pages_written_sum += rep.pages_written;
        }
        Ok(sum)
    }
}
