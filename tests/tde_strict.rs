use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use byteorder::{ByteOrder, LittleEndian};

use QuiverDB::Db;
use QuiverDB::dir::NO_PAGE;
use QuiverDB::pager::Pager;
use QuiverDB::page::{kv_init_v3, kv_header_read_v3, kv_header_write_v3, page_update_checksum, KV_HDR_MIN};

#[test]
fn tde_strict_blocks_crc_fallback() -> Result<()> {
    // Подготовим чистый корень
    let root = unique_root("tde-strict");
    fs::create_dir_all(&root)?;

    // init DB
    let page_size = 64 * 1024;
    let buckets = 32;
    Db::init(&root, page_size, buckets)?;

    // 1) Создаём страницу с валидным CRC (TDE выключен), коммитим
    let pid = {
        let mut pager = Pager::open(&root)?;
        let ps = pager.meta.page_size as usize;

        let pid = pager.allocate_one_page()?;
        let mut page = vec![0u8; ps];
        kv_init_v3(&mut page, pid, 0)?;
        write_single_record_kv(&mut page, b"tde", b"crc", 0, 0)?;
        {
            let mut h = kv_header_read_v3(&page)?;
            h.next_page_id = NO_PAGE;
            kv_header_write_v3(&mut page, &h)?;
        }
        // ВАЖНО: посчитаем CRC — чтобы fallback был корректным
        page_update_checksum(&mut page, 1 /*crc32c*/)?;
        pager.commit_page(pid, &mut page)?; // выставит LSN и окончательный трейлер (CRC остается)
        pid
    };

    // Подготовим ключ для TDE в ENV (32 байта 0x11)
    std::env::set_var("P1_TDE_KEY_HEX", "1111111111111111111111111111111111111111111111111111111111111111");
    std::env::remove_var("P1_TDE_KID"); // используем default

    // 2) TDE on, STRICT off: fallback на CRC должен сработать — чтение OK
    std::env::remove_var("P1_TDE_STRICT");
    {
        let mut pager = Pager::open(&root)?;
        pager.set_tde_config(true, None);
        pager.ensure_tde_key()?; // загрузим ключ
        let ps = pager.meta.page_size as usize;
        let mut buf = vec![0u8; ps];
        pager.read_page(pid, &mut buf)?; // AEAD не пройдёт, но CRC fallback должен пройти
    }

    // 3) TDE on, STRICT on: чтение должно упасть (без CRC‑fallback)
    std::env::set_var("P1_TDE_STRICT", "1");
    {
        let mut pager = Pager::open(&root)?;
        pager.set_tde_config(true, None);
        pager.ensure_tde_key()?; // загрузим ключ
        let ps = pager.meta.page_size as usize;
        let mut buf = vec![0u8; ps];
        let err = pager.read_page(pid, &mut buf).unwrap_err();
        let msg = err.to_string().to_ascii_lowercase();
        assert!(
            msg.contains("aead") || msg.contains("strict"),
            "expected TDE strict read error, got: {}",
            err
        );
    }

    Ok(())
}

// ---------- helpers ----------

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}

fn write_single_record_kv(page: &mut [u8], key: &[u8], value: &[u8], expires_at_sec: u32, vflags: u8) -> Result<()> {
    if page.len() < KV_HDR_MIN + 16 {
        anyhow::bail!("page too small for KV record");
    }
    let off = KV_HDR_MIN;
    let need = off + 2 + 4 + 4 + 1 + key.len() + value.len();
    if need > page.len() {
        anyhow::bail!("record does not fit in page");
    }
    LittleEndian::write_u16(&mut page[off..off + 2], key.len() as u16);
    LittleEndian::write_u32(&mut page[off + 2..off + 6], value.len() as u32);
    LittleEndian::write_u32(&mut page[off + 6..off + 10], expires_at_sec);
    page[off + 10] = vflags;
    let base = off + 11;
    page[base..base + key.len()].copy_from_slice(key);
    page[base + key.len()..base + key.len() + value.len()].copy_from_slice(value);

    // Обновим data_start
    let mut h = kv_header_read_v3(page)?;
    h.data_start = (base + key.len() + value.len()) as u32;
    kv_header_write_v3(page, &h)?;
    Ok(())
}