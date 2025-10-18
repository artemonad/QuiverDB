//! pager/replay — обёртка WAL v2 реплея с LSN‑гейтингом (до ensure_allocated).

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};

use crate::page::{
    KV_HDR_MIN, KV_OFF_LSN, OFF_TYPE, OVF_OFF_LSN, PAGE_MAGIC, PAGE_TYPE_KV_RH3,
    PAGE_TYPE_OVERFLOW3,
};
use crate::wal::wal_replay_if_any;

use super::core::Pager;

impl Pager {
    /// Применить WAL v2 к диску (реплей на старте writer’а), с LSN‑гейтингом до ensure_allocated.
    ///
    /// NEW: после успешного реплея актуализирует meta.next_page_id на диске,
    ///      чтобы последующие открывания могли читать восстановленные страницы.
    pub fn wal_replay_with_pager(root: &std::path::Path) -> Result<()> {
        let mut pager = Pager::open(root)?;
        let page_sz = pager.meta.page_size as usize;

        // Фактическая верхняя граница выделенных страниц в процессе реплея.
        // pager.ensure_allocated() обновляет pager.meta.next_page_id в памяти, но meta на диске
        // не меняется. Мы зафиксируем это после wal_replay_if_any.
        wal_replay_if_any(root, |_wal_lsn, page_id, payload| {
            let new_lsn = v3_page_lsn(payload);

            let mut apply = true;
            if page_id < pager.meta.next_page_id {
                let mut cur = vec![0u8; page_sz];
                if pager.read_page(page_id, &mut cur).is_ok() {
                    if let (Some(nl), Some(cl)) = (new_lsn, v3_page_lsn(&cur)) {
                        if cl >= nl {
                            apply = false;
                        }
                    }
                }
            }

            if apply {
                pager.ensure_allocated(page_id)?;
                pager.write_page_raw(page_id, payload)?;
                // ensure_allocated продвигает next_page_id в памяти. Строго на всякий.
                let need_next = page_id.saturating_add(1);
                if need_next > pager.meta.next_page_id {
                    pager.meta.next_page_id = need_next;
                }
            }
            Ok(())
        })?;

        // WAL-replay уже выставил last_lsn и clean_shutdown=true в meta на диске.
        // Допроставим корректный next_page_id (если вырос).
        {
            use crate::meta::{read_meta, write_meta_overwrite};
            let mut m = read_meta(root)?;
            if pager.meta.next_page_id > m.next_page_id {
                m.next_page_id = pager.meta.next_page_id;
                // Сохраняем last_lsn/clean_shutdown как есть (их уже установил wal_replay_if_any).
                write_meta_overwrite(root, &m)?;
            }
        }

        Ok(())
    }
}

// ---------------- helpers ----------------

fn v3_page_lsn(buf: &[u8]) -> Option<u64> {
    // Достаточно иметь минимальный заголовок страницы (64 байта для v3 KV/OVF).
    if buf.len() < KV_HDR_MIN {
        return None;
    }
    if &buf[..4] != PAGE_MAGIC {
        return None;
    }
    let ptype = LittleEndian::read_u16(&buf[OFF_TYPE..OFF_TYPE + 2]);
    match ptype {
        t if t == PAGE_TYPE_KV_RH3 => {
            Some(LittleEndian::read_u64(&buf[KV_OFF_LSN..KV_OFF_LSN + 8]))
        }
        t if t == PAGE_TYPE_OVERFLOW3 => {
            Some(LittleEndian::read_u64(&buf[OVF_OFF_LSN..OVF_OFF_LSN + 8]))
        }
        _ => None,
    }
}
