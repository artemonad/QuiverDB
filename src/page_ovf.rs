//! Overflow pages (v2): цепочки страниц для хранения больших значений.
//!
//! Формат заголовка — общий v2 (64 байта), тип страницы = PAGE_TYPE_OVERFLOW.
//! Поля используем так:
//! - version: 2
//! - page_type: PAGE_TYPE_OVERFLOW
//! - page_id: u64
//! - data_start (u16): длина полезных данных (chunk_len) на странице
//! - table_slots (u16): 0
//! - used_slots (u16): 0
//! - flags (u16): произвольные флаги (пока 0)
//! - next_page_id (u64): id следующей страницы цепочки или NO_PAGE
//! - lsn (u64): LSN записи для WAL/replay gating
//! - seed (u64): 0
//! - crc32 (u32): как у v2-страниц (последние 4 байта хедера, offset = 60)
//!
//! Плейсхолдер в значении RH-страницы:
//! [tag u8=0xFF][pad u8=0][total_len u64][head_page_id u64]  => 18 байт
//!
//! Инварианты:
//! - chunk_len <= ps - PAGE_HDR_V2_SIZE
//! - Последняя страница цепочки имеет next_page_id = NO_PAGE.
//! - LSN вписывается при commit_page (Pager).

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::cmp;

use crate::consts::{NO_PAGE, PAGE_HDR_V2_SIZE, PAGE_MAGIC, PAGE_TYPE_OVERFLOW};
use crate::pager::Pager;
use crate::page_rh::{rh_page_update_crc, rh_page_verify_crc};
use crate::metrics::{record_overflow_chain_created, record_overflow_chain_freed};

/// Смещение полей в v2 header (совпадает с RH).
const OFF_VERSION: usize = 4;      // u16
const OFF_TYPE: usize = 6;         // u16
const OFF_PAGE_ID: usize = 12;     // u64
const OFF_DATA_START: usize = 20;  // u16 (используем как chunk_len)
const OFF_TABLE_SLOTS: usize = 22; // u16 = 0
const OFF_USED_SLOTS: usize = 24;  // u16 = 0
const OFF_FLAGS: usize = 26;       // u16
const OFF_NEXT_PID: usize = 28;    // u64
const OFF_LSN: usize = 36;         // u64
const OFF_SEED: usize = 44;        // u64 (0)

/// Плейсхолдер overflow-значения в KV: тег + длина + head pid.
pub const OVF_PLACEHOLDER_TAG: u8 = 0xFF;
pub const OVF_PLACEHOLDER_LEN: usize = 18; // [tag u8][pad u8][total_len u64][head_pid u64]

#[derive(Debug, Clone, Copy)]
pub struct OvfPageHeader {
    pub version: u16,     // 2
    pub page_type: u16,   // PAGE_TYPE_OVERFLOW
    pub page_id: u64,
    pub chunk_len: u16,   // bytes of data payload on this page
    pub flags: u16,
    pub next_page_id: u64,
    pub lsn: u64,
}

pub fn ovf_header_read(buf: &[u8]) -> Result<OvfPageHeader> {
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page buffer too small for v2 header"));
    }
    if &buf[..4] != PAGE_MAGIC {
        return Err(anyhow!("bad page magic"));
    }
    let version = LittleEndian::read_u16(&buf[OFF_VERSION..OFF_VERSION + 2]);
    let page_type = LittleEndian::read_u16(&buf[OFF_TYPE..OFF_TYPE + 2]);
    if version != 2 || page_type != PAGE_TYPE_OVERFLOW {
        return Err(anyhow!(
            "not an overflow v2 page (version={}, type={})",
            version,
            page_type
        ));
    }
    let page_id = LittleEndian::read_u64(&buf[OFF_PAGE_ID..OFF_PAGE_ID + 8]);
    let chunk_len = LittleEndian::read_u16(&buf[OFF_DATA_START..OFF_DATA_START + 2]);
    let flags = LittleEndian::read_u16(&buf[OFF_FLAGS..OFF_FLAGS + 2]);
    let next_page_id = LittleEndian::read_u64(&buf[OFF_NEXT_PID..OFF_NEXT_PID + 8]);
    let lsn = LittleEndian::read_u64(&buf[OFF_LSN..OFF_LSN + 8]);
    Ok(OvfPageHeader {
        version,
        page_type,
        page_id,
        chunk_len,
        flags,
        next_page_id,
        lsn,
    })
}

pub fn ovf_header_write(buf: &mut [u8], h: &OvfPageHeader) -> Result<()> {
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page buffer too small for v2 header"));
    }
    for b in &mut buf[..PAGE_HDR_V2_SIZE] {
        *b = 0;
    }
    buf[..4].copy_from_slice(PAGE_MAGIC);
    LittleEndian::write_u16(&mut buf[OFF_VERSION..OFF_VERSION + 2], h.version);
    LittleEndian::write_u16(&mut buf[OFF_TYPE..OFF_TYPE + 2], h.page_type);
    LittleEndian::write_u64(&mut buf[OFF_PAGE_ID..OFF_PAGE_ID + 8], h.page_id);
    LittleEndian::write_u16(&mut buf[OFF_DATA_START..OFF_DATA_START + 2], h.chunk_len);
    LittleEndian::write_u16(&mut buf[OFF_TABLE_SLOTS..OFF_TABLE_SLOTS + 2], 0);
    LittleEndian::write_u16(&mut buf[OFF_USED_SLOTS..OFF_USED_SLOTS + 2], 0);
    LittleEndian::write_u16(&mut buf[OFF_FLAGS..OFF_FLAGS + 2], h.flags);
    LittleEndian::write_u64(&mut buf[OFF_NEXT_PID..OFF_NEXT_PID + 8], h.next_page_id);
    LittleEndian::write_u64(&mut buf[OFF_LSN..OFF_LSN + 8], h.lsn);
    LittleEndian::write_u64(&mut buf[OFF_SEED..OFF_SEED + 8], 0);
    // CRC — снаружи (rh_page_update_crc)
    Ok(())
}

/// Инициализировать пустую overflow-страницу.
pub fn ovf_page_init(buf: &mut [u8], page_id: u64) -> Result<()> {
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page buffer too small"));
    }
    let h = OvfPageHeader {
        version: 2,
        page_type: PAGE_TYPE_OVERFLOW,
        page_id,
        chunk_len: 0,
        flags: 0,
        next_page_id: NO_PAGE,
        lsn: 0,
    };
    ovf_header_write(buf, &h)
}

/// Сколько байт полезной нагрузки помещается на одной overflow-странице.
#[inline]
pub fn ovf_chunk_capacity(page_size: usize) -> usize {
    page_size.saturating_sub(PAGE_HDR_V2_SIZE)
}

/// Создать плейсхолдер для значения в RH-странице.
pub fn ovf_make_placeholder(total_len: u64, head_page_id: u64) -> [u8; OVF_PLACEHOLDER_LEN] {
    let mut out = [0u8; OVF_PLACEHOLDER_LEN];
    out[0] = OVF_PLACEHOLDER_TAG;
    out[1] = 0; // pad
    LittleEndian::write_u64(&mut out[2..10], total_len);
    LittleEndian::write_u64(&mut out[10..18], head_page_id);
    out
}

/// Распарсить плейсхолдер. None, если не узнаем формат.
pub fn ovf_parse_placeholder(v: &[u8]) -> Option<(u64, u64)> {
    if v.len() != OVF_PLACEHOLDER_LEN {
        return None;
    }
    if v[0] != OVF_PLACEHOLDER_TAG {
        return None;
    }
    let total_len = LittleEndian::read_u64(&v[2..10]);
    let head_pid = LittleEndian::read_u64(&v[10..18]);
    Some((total_len, head_pid))
}

/// Записать данные в цепочку overflow-страниц. Возвращает head_page_id.
/// Метрики: один вызов фиксирует одну созданную цепочку.
pub fn ovf_write_chain(pager: &mut Pager, data: &[u8]) -> Result<u64> {
    let ps = pager.meta.page_size as usize;
    let cap = ovf_chunk_capacity(ps);
    if cap == 0 {
        return Err(anyhow!("overflow page capacity is zero (page_size too small)"));
    }

    let mut offset = 0usize;
    let mut head: Option<u64> = None;
    let mut prev: u64 = NO_PAGE;

    while offset < data.len() {
        let pid = pager.allocate_one_page()?;
        let mut buf = vec![0u8; ps];
        ovf_page_init(&mut buf, pid)?;

        let take = cmp::min(cap, data.len() - offset);
        let chunk = &data[offset..offset + take];
        offset += take;

        // заголовок
        let mut h = ovf_header_read(&buf)?;
        h.chunk_len = take as u16; // page_size <= u16::MAX гарантирует валидацией meta
        h.next_page_id = NO_PAGE;  // временно
        ovf_header_write(&mut buf, &h)?;

        // тело
        buf[PAGE_HDR_V2_SIZE..PAGE_HDR_V2_SIZE + take].copy_from_slice(chunk);

        // если есть prev — обновим у него next_page_id и закоммитим
        if prev != NO_PAGE {
            let mut pbuf = vec![0u8; ps];
            pager.read_page(prev, &mut pbuf)?;
            let mut ph = ovf_header_read(&pbuf)?;
            ph.next_page_id = pid;
            ovf_header_write(&mut pbuf, &ph)?;
            rh_page_update_crc(&mut pbuf)?; // общий CRC для v2
            pager.commit_page(prev, &mut pbuf)?;
        }

        // CRC и коммит текущей страницы
        rh_page_update_crc(&mut buf)?;
        pager.commit_page(pid, &mut buf)?;

        if head.is_none() {
            head = Some(pid);
        }
        prev = pid;
    }

    if let Some(h) = head {
        record_overflow_chain_created();
        Ok(h)
    } else {
        Err(anyhow!("empty data for overflow chain"))
    }
}

/// Прочитать всю цепочку overflow-страниц в буфер.
pub fn ovf_read_chain(pager: &Pager, head_page_id: u64, expected_len: Option<usize>) -> Result<Vec<u8>> {
    let ps = pager.meta.page_size as usize;
    let cap = ovf_chunk_capacity(ps);
    if cap == 0 {
        return Err(anyhow!("overflow page capacity is zero"));
    }

    let mut out = Vec::with_capacity(expected_len.unwrap_or(0));

    let mut pid = head_page_id;
    while pid != NO_PAGE {
        let mut buf = vec![0u8; ps];
        pager.read_page(pid, &mut buf)?;
        // проверим CRC (read_page уже это делает для v2, но повторно — дешево)
        let ok = rh_page_verify_crc(&buf)?;
        if !ok {
            return Err(anyhow!("overflow page {} CRC mismatch", pid));
        }

        let h = ovf_header_read(&buf)?;
        let take = h.chunk_len as usize;
        if take > cap {
            return Err(anyhow!(
                "overflow page {} chunk_len={} exceeds capacity {}",
                pid,
                take,
                cap
            ));
        }
        out.extend_from_slice(&buf[PAGE_HDR_V2_SIZE..PAGE_HDR_V2_SIZE + take]);
        pid = h.next_page_id;
    }

    if let Some(exp) = expected_len {
        if out.len() != exp {
            return Err(anyhow!(
                "overflow read length mismatch: got {}, expected {}",
                out.len(),
                exp
            ));
        }
    }
    Ok(out)
}

/// Освободить всю цепочку overflow-страниц (добавить в free-list).
/// Возвращает, сколько страниц освобождено. Метрика: учитывает одну освобожденную цепочку.
pub fn ovf_free_chain(pager: &Pager, head_page_id: u64) -> Result<usize> {
    let ps = pager.meta.page_size as usize;
    let mut freed = 0usize;

    let mut pid = head_page_id;
    while pid != NO_PAGE {
        let mut buf = vec![0u8; ps];
        pager.read_page(pid, &mut buf)?;
        let h = ovf_header_read(&buf)?;
        let next = h.next_page_id;

        pager.free_page(pid)?;
        freed += 1;
        pid = next;
    }

    if freed > 0 {
        record_overflow_chain_freed();
    }
    Ok(freed)
}

/// Утилиты для работы с плейсхолдером.

#[inline]
pub fn ovf_is_placeholder(v: &[u8]) -> bool {
    v.len() == OVF_PLACEHOLDER_LEN && v[0] == OVF_PLACEHOLDER_TAG
}

#[inline]
pub fn ovf_placeholder_total_len(v: &[u8]) -> Option<u64> {
    ovf_parse_placeholder(v).map(|(n, _)| n)
}

#[inline]
pub fn ovf_placeholder_head_pid(v: &[u8]) -> Option<u64> {
    ovf_parse_placeholder(v).map(|(_, pid)| pid)
}