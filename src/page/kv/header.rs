use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};

use crate::page::common::{
    KV_HDR_MIN, KV_OFF_CODEC_ID, KV_OFF_DATA_START, KV_OFF_FLAGS, KV_OFF_LSN, KV_OFF_NEXT_PID,
    KV_OFF_TABLE_SLOTS, KV_OFF_USED_SLOTS, OFF_MAGIC, OFF_PAGE_ID, OFF_TYPE, OFF_VERSION,
    PAGE_MAGIC, PAGE_TYPE_KV_RH3, PAGE_VERSION_V3, TRAILER_LEN,
};

/// Заголовок KV_RH3 страницы (v3).
#[derive(Debug, Clone)]
pub struct KvHeaderV3 {
    pub version: u16,   // == 3
    pub page_type: u16, // == PAGE_TYPE_KV_RH3
    pub page_id: u64,
    pub data_start: u32,
    pub table_slots: u32,
    pub used_slots: u32,
    pub flags: u32,
    pub next_page_id: u64,
    pub lsn: u64,
    pub codec_id: u16, // 0=none,1=zstd,2=lz4 (резерв)
}

/// Инициализировать пустую KV_RH3 страницу (минимальный заголовок).
pub fn kv_init_v3(page: &mut [u8], page_id: u64, codec_id: u16) -> Result<()> {
    if page.len() < KV_HDR_MIN + TRAILER_LEN {
        return Err(anyhow!("page buffer too small for KV_RH3 header"));
    }

    // Обнулим область заголовка
    for b in &mut page[..KV_HDR_MIN] {
        *b = 0;
    }

    // Общий префикс
    page[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(PAGE_MAGIC);
    LittleEndian::write_u16(&mut page[OFF_VERSION..OFF_VERSION + 2], PAGE_VERSION_V3);
    LittleEndian::write_u16(&mut page[OFF_TYPE..OFF_TYPE + 2], PAGE_TYPE_KV_RH3);
    LittleEndian::write_u64(&mut page[OFF_PAGE_ID..OFF_PAGE_ID + 8], page_id);

    // KV поля
    LittleEndian::write_u32(
        &mut page[KV_OFF_DATA_START..KV_OFF_DATA_START + 4],
        KV_HDR_MIN as u32,
    );
    LittleEndian::write_u32(&mut page[KV_OFF_TABLE_SLOTS..KV_OFF_TABLE_SLOTS + 4], 0);
    LittleEndian::write_u32(&mut page[KV_OFF_USED_SLOTS..KV_OFF_USED_SLOTS + 4], 0);
    LittleEndian::write_u32(&mut page[KV_OFF_FLAGS..KV_OFF_FLAGS + 4], 0);
    LittleEndian::write_u64(&mut page[KV_OFF_NEXT_PID..KV_OFF_NEXT_PID + 8], u64::MAX);
    LittleEndian::write_u64(&mut page[KV_OFF_LSN..KV_OFF_LSN + 8], 0);
    LittleEndian::write_u16(&mut page[KV_OFF_CODEC_ID..KV_OFF_CODEC_ID + 2], codec_id);

    // Обнулим трейлер (checksum/tag пишется снаружи)
    let ps = page.len();
    for b in &mut page[ps - TRAILER_LEN..ps] {
        *b = 0;
    }
    Ok(())
}

/// Прочитать заголовок KV_RH3 (валидация MAGIC/версия/тип).
pub fn kv_header_read_v3(page: &[u8]) -> Result<KvHeaderV3> {
    if page.len() < KV_HDR_MIN + TRAILER_LEN {
        return Err(anyhow!("page buffer too small for KV_RH3 header"));
    }
    if &page[OFF_MAGIC..OFF_MAGIC + 4] != PAGE_MAGIC {
        return Err(anyhow!("bad page magic"));
    }
    let version = LittleEndian::read_u16(&page[OFF_VERSION..OFF_VERSION + 2]);
    let page_type = LittleEndian::read_u16(&page[OFF_TYPE..OFF_TYPE + 2]);
    if version != PAGE_VERSION_V3 || page_type != PAGE_TYPE_KV_RH3 {
        return Err(anyhow!(
            "not a KV_RH3 v3 page (version={}, type={})",
            version,
            page_type
        ));
    }

    Ok(KvHeaderV3 {
        version,
        page_type,
        page_id: LittleEndian::read_u64(&page[OFF_PAGE_ID..OFF_PAGE_ID + 8]),
        data_start: LittleEndian::read_u32(&page[KV_OFF_DATA_START..KV_OFF_DATA_START + 4]),
        table_slots: LittleEndian::read_u32(&page[KV_OFF_TABLE_SLOTS..KV_OFF_TABLE_SLOTS + 4]),
        used_slots: LittleEndian::read_u32(&page[KV_OFF_USED_SLOTS..KV_OFF_USED_SLOTS + 4]),
        flags: LittleEndian::read_u32(&page[KV_OFF_FLAGS..KV_OFF_FLAGS + 4]),
        next_page_id: LittleEndian::read_u64(&page[KV_OFF_NEXT_PID..KV_OFF_NEXT_PID + 8]),
        lsn: LittleEndian::read_u64(&page[KV_OFF_LSN..KV_OFF_LSN + 8]),
        codec_id: LittleEndian::read_u16(&page[KV_OFF_CODEC_ID..KV_OFF_CODEC_ID + 2]),
    })
}

/// Записать заголовок KV_RH3 (без пересчёта checksum/тега).
pub fn kv_header_write_v3(page: &mut [u8], h: &KvHeaderV3) -> Result<()> {
    if page.len() < KV_HDR_MIN + TRAILER_LEN {
        return Err(anyhow!("page buffer too small for KV_RH3 header"));
    }
    page[OFF_MAGIC..OFF_MAGIC + 4].copy_from_slice(PAGE_MAGIC);
    LittleEndian::write_u16(&mut page[OFF_VERSION..OFF_VERSION + 2], h.version);
    LittleEndian::write_u16(&mut page[OFF_TYPE..OFF_TYPE + 2], h.page_type);
    LittleEndian::write_u64(&mut page[OFF_PAGE_ID..OFF_PAGE_ID + 8], h.page_id);

    LittleEndian::write_u32(
        &mut page[KV_OFF_DATA_START..KV_OFF_DATA_START + 4],
        h.data_start,
    );
    LittleEndian::write_u32(
        &mut page[KV_OFF_TABLE_SLOTS..KV_OFF_TABLE_SLOTS + 4],
        h.table_slots,
    );
    LittleEndian::write_u32(
        &mut page[KV_OFF_USED_SLOTS..KV_OFF_USED_SLOTS + 4],
        h.used_slots,
    );
    LittleEndian::write_u32(&mut page[KV_OFF_FLAGS..KV_OFF_FLAGS + 4], h.flags);
    LittleEndian::write_u64(
        &mut page[KV_OFF_NEXT_PID..KV_OFF_NEXT_PID + 8],
        h.next_page_id,
    );
    LittleEndian::write_u64(&mut page[KV_OFF_LSN..KV_OFF_LSN + 8], h.lsn);
    LittleEndian::write_u16(&mut page[KV_OFF_CODEC_ID..KV_OFF_CODEC_ID + 2], h.codec_id);
    Ok(())
}
