use anyhow::Result;
use std::fs;
use std::path::PathBuf;

use byteorder::{ByteOrder, LittleEndian};

use QuiverDB::db::Db;
use QuiverDB::dir::Directory;
use QuiverDB::meta::{init_meta_v4, HASH_KIND_XX64_SEED0, CODEC_ZSTD, CKSUM_CRC32C};
use QuiverDB::pager::Pager;
use QuiverDB::page::{kv_header_read_v3, ovf_header_read_v3, KV_HDR_MIN};

#[test]
fn overflow_zstd_roundtrip() -> Result<()> {
    let root = unique_root("ovf-zstd");
    fs::create_dir_all(&root)?;

    let page_size = 64 * 1024;
    let buckets = 128;
    init_meta_v4(&root, page_size, HASH_KIND_XX64_SEED0, CODEC_ZSTD, CKSUM_CRC32C)?;
    Directory::create(&root, buckets)?;

    let big_len = page_size as usize * 2 + page_size as usize / 2;
    let mut big = vec![0xAB; big_len];
    big[0] = 0xAB;
    big[big_len / 2] = 0xBA;
    big[big_len - 1] = 0xAB;

    let key = b"big-zstd";
    {
        let mut db = Db::open(&root)?;
        db.batch(|b| {
            b.put(key, &big)?;
            Ok(())
        })?;
    }

    {
        let db_ro = Db::open_ro(&root)?;
        let got = db_ro.get(key)?.expect("value must exist");
        assert_eq!(got.len(), big.len(), "roundtrip length mismatch");
        assert_eq!(got, big, "roundtrip content mismatch");
    }

    let pager = Pager::open(&root)?;
    let dir = Directory::open(&root)?;
    let bucket = dir.bucket_of_key(key, pager.meta.hash_kind);
    let head_kv = dir.head(bucket)?;
    let ps = pager.meta.page_size as usize;

    let mut kv = vec![0u8; ps];
    pager.read_page(head_kv, &mut kv)?;
    let _h = kv_header_read_v3(&kv)?;
    let off = KV_HDR_MIN;
    let klen = LittleEndian::read_u16(&kv[off..off + 2]) as usize;
    let vlen = LittleEndian::read_u32(&kv[off + 2..off + 6]) as usize;
    let base = off + 11;
    let v_bytes = &kv[base + klen..base + klen + vlen];

    assert!(v_bytes.len() == 18 && v_bytes[0] == 0x01 && v_bytes[1] == 16);
    let head_ovf = LittleEndian::read_u64(&v_bytes[10..18]);

    let mut ovf_page = vec![0u8; ps];
    pager.read_page(head_ovf, &mut ovf_page)?;
    let h_ovf = ovf_header_read_v3(&ovf_page)?;
    assert_eq!(h_ovf.codec_id, 1, "overflow codec_id must be zstd (1)");

    Ok(())
}

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("qdb2-{}-{}-{}", prefix, pid, t))
}