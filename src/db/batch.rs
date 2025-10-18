//! db/batch — Batch API для Db с KV‑packing и атомарным коммитом HEADS_UPDATE.
//!
//! Что делает:
//! - Буферизует операции put/del.
//! - Группирует по бакетам и пакует мелкие записи на страницу (KvPagePacker).
//! - Большие значения уносятся в OVERFLOW3 цепочки, а в KV кладётся placeholder.
//! - Формирует минимальное число KV‑страниц и линкует их к текущей голове бакета.
//! - Коммит одним батчем: BEGIN → IMAGE(OVF+KV) → HEADS_UPDATE → COMMIT.
//! - NEW: Bloom delta‑update — после коммита обновляет биты для изменённых бакетов и
//!        выставляет last_lsn в заголовке bloom.bin (фильтр становится “fresh”).
//! - NEW: Auto‑fallback в OVERFLOW, если запись не помещается на страницу даже после flush.
//! - NEW: (опционально) Lazy compaction после коммита батча — если включено ENV
//!        P1_LAZY_COMPACT_ON_WRITE=1 и длина цепочки достигла порога (см. maintenance.rs).
//!
//! ENV:
//! - P1_PACK_THRESHOLD_BYTES (usize): порог «малой» записи для упаковки (default = ps/8).
//! - P1_LAZY_COMPACT_ON_WRITE=1|true|yes|on — включить ленивую компактацию по факту записи.
//!   Порог задаётся ENV P1_LAZY_COMPACT_THRESHOLD (см. maintenance.rs; default 64).

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::collections::HashMap;
use std::sync::OnceLock;

use crate::dir::NO_PAGE;
use crate::page::{
    kv_pack::{KvPackItem, KvPagePacker},
    ovf_header_read_v3, ovf_header_write_v3, ovf_init_v3, OVF_HDR_MIN, TRAILER_LEN,
};
// NEW: метрики packing + bloom delta-update
use crate::metrics::{record_bloom_update, record_pack_page};
// NEW: bloom side-car для delta-update
use crate::bloom::BloomSidecar;

use super::core::Db;

// ---------------- ENV helpers ----------------

#[inline]
fn lazy_compact_on_write() -> bool {
    static ON: OnceLock<bool> = OnceLock::new();
    *ON.get_or_init(|| {
        std::env::var("P1_LAZY_COMPACT_ON_WRITE")
            .ok()
            .map(|s| s.to_ascii_lowercase())
            .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
            .unwrap_or(false)
    })
}

// ---------------- Pending ops model ----------------

#[derive(Clone)]
enum OpKind {
    Put { key: Vec<u8>, value: Vec<u8> },
    Del { key: Vec<u8> },
}

#[derive(Clone)]
struct PendingOp {
    bucket: u32,
    kind: OpKind,
}

pub struct Batch<'a> {
    db: &'a mut Db,
    pending_ops: Vec<PendingOp>,
}

impl Db {
    /// Реальный Batch API: готовит страницы (с KV‑packing) и коммитит одним WAL‑батчем.
    pub fn batch<F>(&mut self, f: F) -> Result<()>
    where
        F: FnOnce(&mut Batch<'_>) -> Result<()>,
    {
        if self.readonly {
            return Err(anyhow!("Db is read-only"));
        }
        let mut b = Batch::new(self);
        f(&mut b)?;
        b.finish()
    }
}

impl<'a> Batch<'a> {
    pub fn new(db: &'a mut Db) -> Self {
        Self {
            db,
            pending_ops: Vec::new(),
        }
    }

    /// put внутри batch: буферизация операции (без немедленной аллокации).
    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<()> {
        if key.len() > u16::MAX as usize {
            return Err(anyhow!("key too long (> u16::MAX)"));
        }
        let bucket = self.db.dir.bucket_of_key(key, self.db.pager.meta.hash_kind);
        self.pending_ops.push(PendingOp {
            bucket,
            kind: OpKind::Put {
                key: key.to_vec(),
                value: value.to_vec(),
            },
        });
        Ok(())
    }

    /// del внутри batch: буферизация tombstone‑операции.
    pub fn del(&mut self, key: &[u8]) -> Result<bool> {
        if key.len() > u16::MAX as usize {
            return Err(anyhow!("key too long (> u16::MAX)"));
        }
        let bucket = self.db.dir.bucket_of_key(key, self.db.pager.meta.hash_kind);
        let existed = self.db.dir.head(bucket)? != NO_PAGE;

        self.pending_ops.push(PendingOp {
            bucket,
            kind: OpKind::Del { key: key.to_vec() },
        });
        Ok(existed)
    }

    /// Завершить batch: сборка OVF и KV страниц с упаковкой + один батч коммита.
    pub fn finish(mut self) -> Result<()> {
        if self.pending_ops.is_empty() {
            return Ok(());
        }

        let ps = self.db.pager.meta.page_size as usize;
        let pack_threshold = std::env::var("P1_PACK_THRESHOLD_BYTES")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or_else(|| std::cmp::max(1, ps / 8));

        // ВАЖНО: не двигаем частично self — изымаем вектор операций целиком и оставляем пустой.
        let ops_owned: Vec<PendingOp> = std::mem::take(&mut self.pending_ops);

        // 1) Сгруппируем операции по бакетам.
        let mut by_bucket: HashMap<u32, Vec<PendingOp>> = HashMap::new();
        for op in ops_owned {
            by_bucket.entry(op.bucket).or_default().push(op);
        }

        // 2) Общие накопители для коммита: страницы (OVF+KV) и обновления голов.
        let mut pages_to_commit: Vec<(u64, Vec<u8>)> = Vec::new();
        let mut new_heads: HashMap<u32, u64> = HashMap::new();

        // NEW: ключи для bloom delta-update (только put-операции)
        let mut bloom_keys: HashMap<u32, Vec<Vec<u8>>> = HashMap::new();

        // 3) Обработаем каждый бакет отдельно.
        for (&bucket, ops) in &by_bucket {
            let prev_head = self.db.dir.head(bucket)?;
            let mut current_head = prev_head;

            // Буфер записей для текущей KV‑страницы (packer)
            let mut packer = KvPagePacker::new(ps);

            for op in ops.iter() {
                match &op.kind {
                    OpKind::Put { key, value } => {
                        // накапливаем ключи для bloom delta-update
                        bloom_keys.entry(bucket).or_default().push(key.clone());

                        let k = key.as_slice();
                        let v = value.as_slice();

                        if v.len() > pack_threshold {
                            // Большое значение → OVERFLOW + placeholder
                            let (ovf_head, mut ovf_pages) =
                                self.build_overflow_chain_pages_full(v)?;
                            pages_to_commit.append(&mut ovf_pages);

                            let placeholder = make_ovf_placeholder_v3(v.len() as u64, ovf_head);
                            self.ensure_add_kv(
                                &mut packer,
                                &mut pages_to_commit,
                                &mut current_head,
                                k,
                                placeholder.as_slice(),
                                0,
                                0,
                            )?;
                        } else {
                            // Малая запись → inline в packer (с возможным авто‑fallback в OVF при нехватке места)
                            self.ensure_add_kv(
                                &mut packer,
                                &mut pages_to_commit,
                                &mut current_head,
                                k,
                                v,
                                0,
                                0,
                            )?;
                        }
                    }
                    OpKind::Del { key } => {
                        // Tombstone — короткая запись, пакуем нормально (value=[])
                        let k = key.as_slice();
                        self.ensure_add_kv(
                            &mut packer,
                            &mut pages_to_commit,
                            &mut current_head,
                            k,
                            &[],
                            0,
                            1, // tombstone
                        )?;
                    }
                }
            }

            // Сбросим хвостовой packer (если набралось что‑то)
            self.flush_packer_to_pages(&mut packer, &mut pages_to_commit, &mut current_head)?;

            // Если создали новые KV страницы — зафиксируем новую голову
            if current_head != prev_head {
                new_heads.insert(bucket, current_head);
            }
        }

        if pages_to_commit.is_empty() && new_heads.is_empty() {
            return Ok(());
        }

        // 4) Коммит одним батчем (OVF + KV) + HEADS_UPDATE
        let mut for_commit: Vec<(u64, &mut [u8])> = Vec::with_capacity(pages_to_commit.len());
        for (pid, buf) in pages_to_commit.iter_mut() {
            for_commit.push((*pid, buf.as_mut_slice()));
        }

        let mut updates: Vec<(u32, u64)> = new_heads.into_iter().collect();
        updates.sort_by_key(|e| e.0);

        self.db
            .pager
            .commit_pages_batch_with_heads(&mut for_commit, &updates)?;

        // 5) Мгновенная видимость читателям
        if !updates.is_empty() {
            self.db.dir.set_heads_bulk(&updates)?;
        }

        // 6) NEW: Bloom delta-update (best-effort) — без двойного учёта метрик
        if !updates.is_empty() {
            if !bloom_keys.is_empty() {
                if let Ok(mut sidecar) = BloomSidecar::open_or_create_for_db(&self.db, 4096, 6) {
                    let new_lsn = self.db.pager.meta.last_lsn;

                    for (bucket, keys_vec) in bloom_keys.into_iter() {
                        // соберём &[&[u8]]
                        let key_slices: Vec<&[u8]> =
                            keys_vec.iter().map(|k| k.as_slice()).collect();
                        // Метрика record_bloom_update(..) уже учитывается внутри update_bucket_bits()
                        let _ = sidecar.update_bucket_bits(bucket, &key_slices, new_lsn);
                    }
                }
            } else {
                // Delete-only batch: сделаем фильтр “fresh”, обновив только last_lsn
                if let Ok(mut sidecar) = BloomSidecar::open_or_create_for_db(&self.db, 4096, 6) {
                    let new_lsn = self.db.pager.meta.last_lsn;
                    if sidecar.set_last_lsn(new_lsn).is_ok() {
                        // метрика — без байтового апдейта
                        record_bloom_update(0);
                    }
                }
            }
        }

        // 7) NEW: ленивый вызов компактора по затронутым бакетам (если включено)
        if lazy_compact_on_write() && !updates.is_empty() {
            for (bucket, _new_head) in &updates {
                let _ = self.db.lazy_compact_bucket_if_needed(*bucket);
            }
        }

        Ok(())
    }

    // ---------- helpers ----------

    /// Сбросить текущий packer в страницу, подвесить к текущей голове и очистить packer.
    fn flush_packer_to_pages(
        &mut self,
        packer: &mut KvPagePacker,
        pages_acc: &mut Vec<(u64, Vec<u8>)>,
        cur_head: &mut u64,
    ) -> Result<()> {
        if packer.is_empty() {
            return Ok(());
        }
        // Метрики: сколько записей упаковано на страницу
        let slots = packer.len() as u64;

        let pid = self.db.pager.allocate_one_page()?;
        let page = packer.finalize_into_page(pid, *cur_head, 0)?;
        pages_acc.push((pid, page));
        *cur_head = pid;

        // Запишем метрику для этой страницы упаковки
        record_pack_page(slots);

        Ok(())
    }

    /// Гарантированно добавить одну KV‑запись в packer, при необходимости предварительно
    /// сбросив текущую страницу. Ключ/значение клонируются внутрь страницы.
    ///
    /// NEW: если запись не помещается даже на пустую страницу (после flush) и это не tombstone,
    /// автоматически уходим в OVERFLOW (строим цепочку и кладём placeholder).
    fn ensure_add_kv(
        &mut self,
        packer: &mut KvPagePacker,
        pages_acc: &mut Vec<(u64, Vec<u8>)>,
        cur_head: &mut u64,
        key: &[u8],
        value: &[u8],
        expires_at_sec: u32,
        vflags: u8,
    ) -> Result<()> {
        let item = KvPackItem {
            key: key.to_vec(),
            value: value.to_vec(),
            expires_at_sec,
            vflags,
        };
        if packer.try_add(item) {
            return Ok(());
        }

        // Не влезло — сбросим страницу и повторим
        self.flush_packer_to_pages(packer, pages_acc, cur_head)?;
        let item2 = KvPackItem {
            key: key.to_vec(),
            value: value.to_vec(),
            expires_at_sec,
            vflags,
        };
        if packer.try_add(item2) {
            return Ok(());
        }

        // Всё ещё не влезает: если это не tombstone и value непустой — auto‑fallback в OVERFLOW
        if vflags == 0 && !value.is_empty() {
            let (ovf_head, mut ovf_pages) = self.build_overflow_chain_pages_full(value)?;
            pages_acc.append(&mut ovf_pages);

            let placeholder = make_ovf_placeholder_v3(value.len() as u64, ovf_head);
            let ph_item = KvPackItem {
                key: key.to_vec(),
                value: placeholder,
                expires_at_sec,
                vflags: 0,
            };

            // Попробуем снова (на этой же пустой странице — после flush)
            if packer.try_add(ph_item) {
                return Ok(());
            }

            // Если вдруг placeholder не влез (чрезвычайно маловероятно) — сбросим ещё одну страницу и добавим
            self.flush_packer_to_pages(packer, pages_acc, cur_head)?;
            let ph_item2 = KvPackItem {
                key: key.to_vec(),
                value: make_ovf_placeholder_v3(value.len() as u64, ovf_head),
                expires_at_sec,
                vflags: 0,
            };
            if packer.try_add(ph_item2) {
                return Ok(());
            }
        }

        Err(anyhow!(
            "record too large for KV page even after flush (and OVF fallback)"
        ))
    }

    /// Построить OVERFLOW3‑цепочку целиком в памяти (все страницы),
    /// вернуть (head_pid, pages).
    fn build_overflow_chain_pages_full(
        &mut self,
        value: &[u8],
    ) -> Result<(u64, Vec<(u64, Vec<u8>)>)> {
        let ps = self.db.pager.meta.page_size as usize;
        let header_min = OVF_HDR_MIN;
        let cap = ps - header_min - TRAILER_LEN;
        if cap == 0 {
            return Err(anyhow!("overflow page capacity is zero"));
        }
        let chunks: Vec<&[u8]> = value.chunks(cap).collect();
        let n = chunks.len();
        let start_pid = self.db.pager.allocate_pages(n as u64)?;

        let mut out: Vec<(u64, Vec<u8>)> = Vec::with_capacity(n);
        let codec_default = self.db.pager.meta.codec_default;

        for i in 0..n {
            let pid = start_pid + i as u64;
            let next = if i + 1 < n {
                start_pid + (i as u64) + 1
            } else {
                NO_PAGE
            };

            let (codec_id, payload) = if codec_default == 1 {
                match zstd::bulk::compress(chunks[i], 0) {
                    Ok(comp) if comp.len() <= cap => (1u16, comp),
                    _ => (0u16, chunks[i].to_vec()),
                }
            } else {
                (0u16, chunks[i].to_vec())
            };

            let mut page = vec![0u8; ps];
            ovf_init_v3(&mut page, pid, codec_id)?;
            {
                let mut h = ovf_header_read_v3(&page)?;
                h.chunk_len = payload.len() as u32;
                h.next_page_id = next;
                ovf_header_write_v3(&mut page, &h)?;
            }
            page[header_min..header_min + payload.len()].copy_from_slice(&payload);
            out.push((pid, page));
        }
        Ok((start_pid, out))
    }
}

// ----------------- TLV placeholder (OVF_CHAIN) -----------------

fn make_ovf_placeholder_v3(total_len: u64, head_pid: u64) -> Vec<u8> {
    // [tag u8=0x01][len u8=16][total_len u64][head_pid u64]
    let mut out = vec![0u8; 1 + 1 + 16];
    out[0] = 0x01;
    out[1] = 16;
    LittleEndian::write_u64(&mut out[2..10], total_len);
    LittleEndian::write_u64(&mut out[10..18], head_pid);
    out
}
