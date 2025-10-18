use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use byteorder::{ByteOrder, LittleEndian};

use QuiverDB::db::Db;
use QuiverDB::dir::Directory;
use QuiverDB::meta::{init_meta_v4, CKSUM_CRC32C, CODEC_NONE, HASH_KIND_XX64_SEED0};
use QuiverDB::page::{
    kv_header_read_v3, kv_header_write_v3, kv_init_v3, page_update_checksum, KV_HDR_MIN,
};
use QuiverDB::pager::Pager;

#[test]
fn ttl_read_side_skips_expired_and_returns_older_valid() -> Result<()> {
    let root = unique_root("ttl");
    fs::create_dir_all(&root)?;

    // meta v4 + dir v2
    let page_size = 64 * 1024;
    let buckets = 128;
    init_meta_v4(
        &root,
        page_size,
        HASH_KIND_XX64_SEED0,
        CODEC_NONE,
        CKSUM_CRC32C,
    )?;
    Directory::create(&root, buckets)?;

    let mut pager = Pager::open(&root)?;
    let ps = pager.meta.page_size as usize;

    let key = b"ttl-key";

    // Страница "старого" значения (валидного, бессрочного)
    let old_pid = pager.allocate_one_page()?;
    let mut old_page = vec![0u8; ps];
    kv_init_v3(&mut old_page, old_pid, 0)?;
    write_single_record_kv(&mut old_page, key, b"old", 0, 0)?;
    {
        let mut h = kv_header_read_v3(&old_page)?;
        h.next_page_id = u64::MAX; // хвост
        kv_header_write_v3(&mut old_page, &h)?;
    }
    page_update_checksum(&mut old_page, pager.meta.checksum_kind)?;
    pager.commit_page(old_pid, &mut old_page)?;

    // Страница "нового" значения (просроченного)
    let new_pid = pager.allocate_one_page()?;
    let mut new_page = vec![0u8; ps];
    kv_init_v3(&mut new_page, new_pid, 0)?;
    let now = now_secs();
    let expired_at = now.saturating_sub(10); // истёк 10 секунд назад
    write_single_record_kv(&mut new_page, key, b"expired", expired_at, 0)?;
    {
        let mut h = kv_header_read_v3(&new_page)?;
        h.next_page_id = old_pid; // head -> older
        kv_header_write_v3(&mut new_page, &h)?;
    }
    page_update_checksum(&mut new_page, pager.meta.checksum_kind)?;
    pager.commit_page(new_pid, &mut new_page)?;

    // Обновим head бакета на "новую" страницу через writer‑обёртку Db
    {
        let mut db = Db::open(&root)?;
        let bucket = db.dir.bucket_of_key(key, db.pager.meta.hash_kind);
        db.set_dir_head(bucket, new_pid)?;
    }

    // Чтение через Db::get (read-side TTL должен пропустить "expired" и вернуть "old")
    let db_ro = Db::open_ro(&root)?;
    let got = db_ro.get(key)?.expect("value must be returned");
    assert_eq!(got.as_slice(), b"old");

    Ok(())
}

fn write_single_record_kv(
    page: &mut [u8],
    key: &[u8],
    value: &[u8],
    expires_at_sec: u32,
    vflags: u8,
) -> Result<()> {
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

fn now_secs() -> u32 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default();
    (now.as_secs() as u64).min(u32::MAX as u64) as u32
}

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}
