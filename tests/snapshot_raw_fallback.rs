// tests/snapshot_raw_fallback.rs
//
// Проверяет RAW FALLBACK под снапшотом:
// - Если live-страница выглядит как v2 (MAGIC + ver>=2), но её RH/OVF-хедер не парсится,
//   snapshot сначала пытается frozen (после одного refresh индекса) и, если он есть,
//   возвращает старую версию.
// - Мы создаём нормальную frozen-страницу (через обычный update после snapshot_begin),
//   затем "ломаем" live-страницу: ставим неизвестный тип в поле type, пересчитываем CRC,
//   и записываем страницу raw. Snapshot должен вернуть старое значение.

use anyhow::Result;
use byteorder::{ByteOrder, LittleEndian};
use QuiverDB::{init_db, Db, Directory, read_meta};
use QuiverDB::consts::{PAGE_MAGIC};
use QuiverDB::pager::Pager;
use QuiverDB::page_rh::{rh_page_update_crc};

#[test]
fn snapshot_reads_frozen_when_live_v2_header_unrecognized() -> Result<()> {
    // Уберём дребезг LSN/fsync для стабильности
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = std::env::temp_dir().join(format!(
        "qdbtest-raw-fallback-{}",
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos()
    ));

    std::fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 128)?;

    // 1) Базовая запись
    {
        let mut db = Db::open(&root)?;
        db.put(b"k", b"v0")?;
    }

    // 2) Начинаем snapshot
    let mut db = Db::open(&root)?;
    let mut snap = db.snapshot_begin()?;

    // 3) Обновляем ключ (freeze старой версии должен появиться)
    db.put(b"k", b"v1")?;

    // 4) Найдём страницу head для bucket ключа "k" и "сломаем" её type в хедере v2.
    //    Сохраним MAGIC, ver=2, пересчитаем CRC, чтобы read_page не падал по CRC.
    {
        let dir = Directory::open(&root)?;
        let mut pager = Pager::open(&root)?;
        let bucket = dir.bucket_of_key(b"k");
        let pid = dir.head(bucket)?;
        assert!(pid < pager.meta.next_page_id, "head page must exist");

        let ps = pager.meta.page_size as usize;
        let mut buf = vec![0u8; ps];
        pager.read_page(pid, &mut buf)?; // текущая (уже новая) страница

        // Убедимся, что это v2 (MAGIC и ver>=2)
        assert_eq!(&buf[..4], PAGE_MAGIC, "expected v2 MAGIC on head page");
        let ver = LittleEndian::read_u16(&buf[4..6]);
        assert!(ver >= 2, "expected v2 page");

        // Поставим "неизвестный" тип (не KV_RH=2 и не OVERFLOW=3), например 9999.
        LittleEndian::write_u16(&mut buf[6..8], 9999u16);

        // Пересчитаем CRC (наша утилита считает CRC по v2‑правилам).
        rh_page_update_crc(&mut buf)?;

        // Запишем страницу сырым методом (без WAL), чтобы не менять LSN/мета.
        pager.write_page_raw(pid, &buf)?;
    }

    // 5) Снимок должен видеть старую версию (v0), используя frozen sidecar,
    //    т.к. live v2 теперь "не распознаётся" и с LSN>snapshot_lsn.
    let got = snap.get(b"k")?.expect("k must exist under snapshot");
    assert_eq!(got.as_slice(), b"v0", "snapshot must return OLD value via frozen RAW FALLBACK");

    // Для контроля: live видит новую версию (если бы мы попробовали), но мы не вызываем,
    // чтобы не упереться в испорченный type. Проверим только, что LSN продвинулся.
    let m = read_meta(&root)?;
    assert!(m.last_lsn >= 2, "after update last_lsn must be >= 2");

    // 6) Заканчиваем снапшот
    db.snapshot_end(&mut snap)?;

    Ok(())
}