//! page/kv_pack — упаковка нескольких KV записей в одну KV_RH3 страницу (v3).
//!
//! Идея:
//! - Используем текущий формат v3 со слот‑таблицей (KV_SLOT_SIZE=6).
//! - Пишем несколько записей формата [klen u16][vlen u32][expires u32][vflags u8][key][value]
//!   в data‑область подряд, начиная c KV_HDR_MIN.
//! - В хвосте размещаем слот‑таблицу из N слотов; для каждого слота пишем:
//!     [off u32][fp u8][dist u8], где fp = kv_fp8(key) (низшие 8 бит xxhash64(seed=0)).
//!   dist остаётся 0 (подготовка к Robin Hood, сейчас не используется).
//! - Заголовок: data_start = конец данных, table_slots = used_slots = N, next_page_id задаёт вызывающий код.
//!
//! Совместимость:
//! - Ранее fp всегда был 0. Читающая сторона трактует fp=0 как «без отпечатка» (wildcard),
//!   поэтому старые страницы читаются как раньше. Новые страницы получают быстрый отбор слотов по fp.
//!
//! Замечания:
//! - Никакого сжатия KV на уровне страницы (codec_id оставляем как в init_v3).
//! - Проверка вместимости: KV_HDR_MIN + sum(records) ≤ ps - TRAILER_LEN - N*KV_SLOT_SIZE.

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};

use super::kv::{kv_init_v3, kv_header_read_v3, kv_header_write_v3};
// NEW: fingerprint ключа для слота
use super::kv::record::kv_fp8;
use super::common::{KV_HDR_MIN, KV_SLOT_SIZE, TRAILER_LEN};

/// Одна KV запись для упаковки.
#[derive(Debug, Clone)]
pub struct KvPackItem {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
    pub expires_at_sec: u32,
    pub vflags: u8, // bit0=tombstone
}

impl KvPackItem {
    #[inline]
    pub fn encoded_len(&self) -> usize {
        // [klen u16][vlen u32][expires u32][vflags u8] + key + value
        2 + 4 + 4 + 1 + self.key.len() + self.value.len()
    }
}

/// Пакер страниц: накапливает записи и проверяет, помещаются ли они на одну страницу.
#[derive(Debug)]
pub struct KvPagePacker {
    ps: usize,
    items: Vec<KvPackItem>,
    data_bytes: usize, // суммарная длина закодированных записей
}

impl KvPagePacker {
    /// Создать пакер для заданного page_size.
    pub fn new(page_size: usize) -> Self {
        Self {
            ps: page_size,
            items: Vec::new(),
            data_bytes: 0,
        }
    }

    /// Проверка, влезет ли ещё одна запись на текущую страницу.
    fn fits_with(&self, add_data: usize, add_slots: usize) -> bool {
        // Требование: KV_HDR_MIN + (data_bytes + add_data) <= ps - TRAILER_LEN - (slots + add_slots)*KV_SLOT_SIZE
        let data_total = self.data_bytes + add_data;
        // слотов станет items.len() + add_slots
        let slots_total = self.items.len() + add_slots;
        let tail_slots_bytes = slots_total * KV_SLOT_SIZE;

        KV_HDR_MIN + data_total <= self.ps.saturating_sub(TRAILER_LEN + tail_slots_bytes)
    }

    /// Попробовать добавить запись. Возвращает true, если добавлена; false — если не влезает.
    pub fn try_add(&mut self, item: KvPackItem) -> bool {
        let need = item.encoded_len();
        if !self.fits_with(need, 1) {
            return false;
        }
        self.data_bytes += need;
        self.items.push(item);
        true
    }

    /// Сколько элементов уже в пакере.
    #[inline]
    pub fn len(&self) -> usize {
        self.items.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }

    /// Собрать готовую страницу KV_RH3 (v3) и очистить пакер.
    /// - Не потребляет self: после генерации страницы пакер очищается (items.clear/data_bytes=0).
    pub fn finalize_into_page(&mut self, page_id: u64, next_page_id: u64, codec_id: u16) -> Result<Vec<u8>> {
        if self.items.is_empty() {
            return Err(anyhow!("KvPagePacker::finalize_into_page: no items"));
        }
        let ps = self.ps;

        // Буфер страницы
        let mut page = vec![0u8; ps];
        kv_init_v3(&mut page, page_id, codec_id)?;

        // Рассчитаем начало таблицы слотов и предел данных
        let slots = self.items.len();
        let tail_slots_bytes = slots * KV_SLOT_SIZE;
        let table_start = ps
            .checked_sub(TRAILER_LEN + tail_slots_bytes)
            .ok_or_else(|| anyhow!("page too small for slots"))?;
        let data_limit = table_start; // данные должны закончиться до таблицы

        // Записываем записи подряд начиная с KV_HDR_MIN
        let mut off = KV_HDR_MIN;
        let mut record_offsets: Vec<u32> = Vec::with_capacity(slots);
        let mut record_fps: Vec<u8> = Vec::with_capacity(slots);

        for it in &self.items {
            let need = it.encoded_len();
            if off + need > data_limit {
                return Err(anyhow!("KvPagePacker overflow: miscalculated fit"));
            }

            // klen u16
            LittleEndian::write_u16(&mut page[off..off + 2], it.key.len() as u16);
            // vlen u32
            LittleEndian::write_u32(&mut page[off + 2..off + 6], it.value.len() as u32);
            // expires u32
            LittleEndian::write_u32(&mut page[off + 6..off + 10], it.expires_at_sec);
            // vflags u8
            page[off + 10] = it.vflags;

            let base = off + 11;
            // key
            page[base..base + it.key.len()].copy_from_slice(&it.key);
            // value
            page[base + it.key.len()..base + it.key.len() + it.value.len()]
                .copy_from_slice(&it.value);

            record_offsets.push(off as u32);
            // NEW: вычислим fingerprint для слота
            record_fps.push(kv_fp8(&it.key));

            off = base + it.key.len() + it.value.len();
        }

        // Заполним заголовок
        {
            let mut h = kv_header_read_v3(&page)?;
            h.data_start = off as u32;
            h.table_slots = slots as u32;
            h.used_slots = slots as u32;
            h.next_page_id = next_page_id;
            kv_header_write_v3(&mut page, &h)?;
        }

        // Слот‑таблица: [off u32][fp u8][dist u8]
        let mut slot_off = table_start;
        for (rec_off, fp) in record_offsets.into_iter().zip(record_fps.into_iter()) {
            // off u32
            LittleEndian::write_u32(&mut page[slot_off..slot_off + 4], rec_off);
            // fp u8 = kv_fp8(key); 0 допустим (wildcard на чтении)
            page[slot_off + 4] = fp;
            // dist u8 = 0 (пока не используем RH-метаданные)
            page[slot_off + 5] = 0;

            slot_off += KV_SLOT_SIZE;
        }

        // Очистим состояние пакера
        self.items.clear();
        self.data_bytes = 0;

        Ok(page)
    }

    /// Вариант, потребляющий self (оставлен для совместимости).
    pub fn finalize(self, page_id: u64, next_page_id: u64, codec_id: u16) -> Result<Vec<u8>> {
        let mut me = self;
        me.finalize_into_page(page_id, next_page_id, codec_id)
    }
}