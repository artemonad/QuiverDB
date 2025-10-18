//! page/ovf/header — заголовок OVERFLOW3 (v3) и базовые операции.

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};

use crate::page::common::{
    OFF_MAGIC, OFF_PAGE_ID, OFF_TYPE, OFF_VERSION, OVF_HDR_MIN, OVF_OFF_CHUNK_LEN,
    OVF_OFF_CODEC_ID, OVF_OFF_LSN, OVF_OFF_NEXT_PID, OVF_OFF_RESERVED, PAGE_MAGIC,
    PAGE_TYPE_OVERFLOW3, PAGE_VERSION_V3, TRAILER_LEN,
};

/// Заголовок OVERFLOW3 страницы (v3).
#[derive(Debug, Clone)]
pub struct OvfHeaderV3 {
    pub version: u16,   // == 3
    pub page_type: u16, // == PAGE_TYPE_OVERFLOW3
    pub page_id: u64,
    pub chunk_len: u32, // байт payload на этой странице (сжатая длина, если codec!=0)
    pub next_page_id: u64, // u64::MAX — конец цепочки
    pub lsn: u64,
    pub codec_id: u16, // 0=none, 1=zstd, 2=lz4 (резерв)
}

/// Инициализировать пустую OVERFLOW3 страницу.
/// - Обнуляет область заголовка и трейлер.
/// - Записывает MAGIC/версию/тип/ид.
/// - Ставит chunk_len=0, reserved=0, next_page_id=u64::MAX, lsn=0, codec_id.
pub fn ovf_init_v3(page: &mut [u8], page_id: u64, codec_id: u16) -> Result<()> {
    if page.len() < OVF_HDR_MIN + TRAILER_LEN {
        return Err(anyhow!("page buffer too small for OVERFLOW3 header"));
    }

    // Обнулим область заголовка
    for b in &mut page[..OVF_HDR_MIN] {
        *b = 0;
    }

    // Общий префикс
    page[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(PAGE_MAGIC);
    LittleEndian::write_u16(&mut page[OFF_VERSION..OFF_VERSION + 2], PAGE_VERSION_V3);
    LittleEndian::write_u16(&mut page[OFF_TYPE..OFF_TYPE + 2], PAGE_TYPE_OVERFLOW3);
    LittleEndian::write_u64(&mut page[OFF_PAGE_ID..OFF_PAGE_ID + 8], page_id);

    // OVF поля
    LittleEndian::write_u32(&mut page[OVF_OFF_CHUNK_LEN..OVF_OFF_CHUNK_LEN + 4], 0);
    LittleEndian::write_u32(&mut page[OVF_OFF_RESERVED..OVF_OFF_RESERVED + 4], 0);
    LittleEndian::write_u64(&mut page[OVF_OFF_NEXT_PID..OVF_OFF_NEXT_PID + 8], u64::MAX);
    LittleEndian::write_u64(&mut page[OVF_OFF_LSN..OVF_OFF_LSN + 8], 0);
    LittleEndian::write_u16(&mut page[OVF_OFF_CODEC_ID..OVF_OFF_CODEC_ID + 2], codec_id);

    // Обнулим трейлер (checksum считается снаружи)
    let ps = page.len();
    for b in &mut page[ps - TRAILER_LEN..ps] {
        *b = 0;
    }
    Ok(())
}

/// Прочитать заголовок OVERFLOW3 (валидация MAGIC/версия/тип).
pub fn ovf_header_read_v3(page: &[u8]) -> Result<OvfHeaderV3> {
    if page.len() < OVF_HDR_MIN + TRAILER_LEN {
        return Err(anyhow!("page buffer too small for OVERFLOW3 header"));
    }
    if &page[OFF_MAGIC..OFF_MAGIC + 4] != PAGE_MAGIC {
        return Err(anyhow!("bad page magic"));
    }
    let version = LittleEndian::read_u16(&page[OFF_VERSION..OFF_VERSION + 2]);
    let page_type = LittleEndian::read_u16(&page[OFF_TYPE..OFF_TYPE + 2]);
    if version != PAGE_VERSION_V3 || page_type != PAGE_TYPE_OVERFLOW3 {
        return Err(anyhow!(
            "not an OVERFLOW3 v3 page (version={}, type={})",
            version,
            page_type
        ));
    }

    Ok(OvfHeaderV3 {
        version,
        page_type,
        page_id: LittleEndian::read_u64(&page[OFF_PAGE_ID..OFF_PAGE_ID + 8]),
        chunk_len: LittleEndian::read_u32(&page[OVF_OFF_CHUNK_LEN..OVF_OFF_CHUNK_LEN + 4]),
        next_page_id: LittleEndian::read_u64(&page[OVF_OFF_NEXT_PID..OVF_OFF_NEXT_PID + 8]),
        lsn: LittleEndian::read_u64(&page[OVF_OFF_LSN..OVF_OFF_LSN + 8]),
        codec_id: LittleEndian::read_u16(&page[OVF_OFF_CODEC_ID..OVF_OFF_CODEC_ID + 2]),
    })
}

/// Записать заголовок OVERFLOW3 (без пересчёта checksum).
pub fn ovf_header_write_v3(page: &mut [u8], h: &OvfHeaderV3) -> Result<()> {
    if page.len() < OVF_HDR_MIN + TRAILER_LEN {
        return Err(anyhow!("page buffer too small for OVERFLOW3 header"));
    }
    page[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(PAGE_MAGIC);
    LittleEndian::write_u16(&mut page[OFF_VERSION..OFF_VERSION + 2], h.version);
    LittleEndian::write_u16(&mut page[OFF_TYPE..OFF_TYPE + 2], h.page_type);
    LittleEndian::write_u64(&mut page[OFF_PAGE_ID..OFF_PAGE_ID + 8], h.page_id);

    LittleEndian::write_u32(
        &mut page[OVF_OFF_CHUNK_LEN..OVF_OFF_CHUNK_LEN + 4],
        h.chunk_len,
    );
    LittleEndian::write_u32(&mut page[OVF_OFF_RESERVED..OVF_OFF_RESERVED + 4], 0); // всегда 0
    LittleEndian::write_u64(
        &mut page[OVF_OFF_NEXT_PID..OVF_OFF_NEXT_PID + 8],
        h.next_page_id,
    );
    LittleEndian::write_u64(&mut page[OVF_OFF_LSN..OVF_OFF_LSN + 8], h.lsn);
    LittleEndian::write_u16(
        &mut page[OVF_OFF_CODEC_ID..OVF_OFF_CODEC_ID + 2],
        h.codec_id,
    );
    Ok(())
}
