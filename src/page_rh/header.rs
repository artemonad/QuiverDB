//! Заголовок страницы v2 (Robin Hood), init и CRC.

use crate::consts::{NO_PAGE, PAGE_HDR_V2_SIZE, PAGE_MAGIC, PAGE_TYPE_KV_RH};
use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};

/// Смещение CRC32 в заголовке v2 (последние 4 байта 64-байтного хедера).
pub const RH_PAGE_CRC_OFF: usize = 60; // last 4 bytes of 64-byte header

#[derive(Debug, Clone, Copy)]
pub struct RhPageHeader {
    pub version: u16,      // must be 2
    pub page_type: u16,    // PAGE_TYPE_KV_RH
    pub page_id: u64,
    pub data_start: u16,   // next free byte for data area
    pub table_slots: u16,  // number of slots allocated at page end (capacity)
    pub used_slots: u16,   // number of occupied slots
    pub flags: u16,
    pub next_page_id: u64,
    pub lsn: u64,          // WAL LSN stored on page (используется при реплее)
    pub seed: u64,         // per-page hash mixing
}

#[inline]
fn derive_seed(page_id: u64) -> u64 {
    // Простая детерминированная мешалка для page-local seed.
    let mut x = page_id ^ 0x9E37_79B9_7F4A_7C15u64;
    x ^= x >> 27;
    x = x.wrapping_mul(0x3C79_AC49u64);
    x ^= x >> 33;
    x
}

pub fn rh_header_read(buf: &[u8]) -> Result<RhPageHeader> {
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page buffer too small for v2 header"));
    }
    if &buf[..4] != PAGE_MAGIC {
        return Err(anyhow!("bad page magic"));
    }
    let version = LittleEndian::read_u16(&buf[4..6]);
    let page_type = LittleEndian::read_u16(&buf[6..8]);
    if version != 2 || page_type != PAGE_TYPE_KV_RH {
        return Err(anyhow!(
            "not a v2 KV RH page (version={}, type={})",
            version,
            page_type
        ));
    }
    let page_id = LittleEndian::read_u64(&buf[12..20]);
    let data_start = LittleEndian::read_u16(&buf[20..22]);
    let table_slots = LittleEndian::read_u16(&buf[22..24]);
    let used_slots = LittleEndian::read_u16(&buf[24..26]);
    let flags = LittleEndian::read_u16(&buf[26..28]);
    let next_page_id = LittleEndian::read_u64(&buf[28..36]);
    let lsn = LittleEndian::read_u64(&buf[36..44]);
    let seed = LittleEndian::read_u64(&buf[44..52]);
    Ok(RhPageHeader {
        version,
        page_type,
        page_id,
        data_start,
        table_slots,
        used_slots,
        flags,
        next_page_id,
        lsn,
        seed,
    })
}

pub fn rh_header_write(buf: &mut [u8], h: &RhPageHeader) -> Result<()> {
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page buffer too small for v2 header"));
    }
    for b in &mut buf[..PAGE_HDR_V2_SIZE] {
        *b = 0;
    }
    buf[..4].copy_from_slice(PAGE_MAGIC);
    LittleEndian::write_u16(&mut buf[4..6], h.version);
    LittleEndian::write_u16(&mut buf[6..8], h.page_type);
    LittleEndian::write_u64(&mut buf[12..20], h.page_id);
    LittleEndian::write_u16(&mut buf[20..22], h.data_start);
    LittleEndian::write_u16(&mut buf[22..24], h.table_slots);
    LittleEndian::write_u16(&mut buf[24..26], h.used_slots);
    LittleEndian::write_u16(&mut buf[26..28], h.flags);
    LittleEndian::write_u64(&mut buf[28..36], h.next_page_id);
    LittleEndian::write_u64(&mut buf[36..44], h.lsn);
    LittleEndian::write_u64(&mut buf[44..52], h.seed);
    // CRC пишется снаружи (rh_page_update_crc)
    Ok(())
}

pub fn rh_page_init(buf: &mut [u8], page_id: u64) -> Result<()> {
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page buffer too small"));
    }
    let h = RhPageHeader {
        version: 2,
        page_type: PAGE_TYPE_KV_RH,
        page_id,
        data_start: PAGE_HDR_V2_SIZE as u16,
        table_slots: 0,
        used_slots: 0,
        flags: 0,
        next_page_id: NO_PAGE,
        lsn: 0,
        seed: derive_seed(page_id),
    };
    rh_header_write(buf, &h)
}

pub fn rh_page_is_kv(buf: &[u8]) -> bool {
    if buf.len() < PAGE_HDR_V2_SIZE {
        return false;
    }
    if &buf[..4] != PAGE_MAGIC {
        return false;
    }
    let ver = LittleEndian::read_u16(&buf[4..6]);
    let t = LittleEndian::read_u16(&buf[6..8]);
    ver == 2 && t == PAGE_TYPE_KV_RH
}

// CRC по всей странице с обнулённым полем CRC.
pub fn rh_page_update_crc(buf: &mut [u8]) -> Result<()> {
    use crc32fast::Hasher as Crc32;
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page too small for CRC"));
    }
    let mut hasher = Crc32::new();
    hasher.update(&buf[..RH_PAGE_CRC_OFF]);
    hasher.update(&[0, 0, 0, 0]);
    hasher.update(&buf[RH_PAGE_CRC_OFF + 4..]);
    let crc = hasher.finalize();
    LittleEndian::write_u32(&mut buf[RH_PAGE_CRC_OFF..RH_PAGE_CRC_OFF + 4], crc);
    Ok(())
}

pub fn rh_page_verify_crc(buf: &[u8]) -> Result<bool> {
    use crc32fast::Hasher as Crc32;
    if buf.len() < PAGE_HDR_V2_SIZE {
        return Err(anyhow!("page too small for CRC verify"));
    }
    let stored = LittleEndian::read_u32(&buf[RH_PAGE_CRC_OFF..RH_PAGE_CRC_OFF + 4]);
    if stored == 0 {
        return Ok(true); // допускаем ноль для совместимости
    }
    let mut hasher = Crc32::new();
    hasher.update(&buf[..RH_PAGE_CRC_OFF]);
    hasher.update(&[0, 0, 0, 0]);
    hasher.update(&buf[RH_PAGE_CRC_OFF + 4..]);
    let calc = hasher.finalize();
    Ok(calc == stored)
}