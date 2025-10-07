// tests/crash_recovery_matrix.rs
//
// Crash-recovery matrix test (без failpoints):
// В одном сценарии формируем WAL, содержащий:
// - валидные PAGE_IMAGE кадры для KV и OVERFLOW страниц (данные на сегментах отсутствуют),
// - неизвестный кадр (type=99) посередине,
// - частичный хвост последнего кадра (обрыв payload).
// Помечаем clean_shutdown=false и открываем writer: wal_replay_if_any должен
// применить валидные кадры (включая overflow-цепочку), проигнорировать unknown,
// остановиться на partial tail, и после успешного реплея усечь WAL до заголовка.
//
// Проверка состояния выполняется через реконструкцию KV напрямую из страниц (без directory).

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use crc32fast::Hasher as Crc32;
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use QuiverDB::{init_db, read_meta, set_clean_shutdown, Db, Directory};
use QuiverDB::consts::{
    WAL_FILE, WAL_HDR_SIZE, WAL_MAGIC, WAL_REC_HDR_SIZE, WAL_REC_OFF_CRC32, WAL_REC_OFF_LEN,
    WAL_REC_OFF_LSN, WAL_REC_OFF_PAGE_ID, WAL_REC_PAGE_IMAGE,
};
use QuiverDB::pager::Pager;
use QuiverDB::page_rh::{
    rh_header_read, rh_header_write, rh_kv_insert, rh_page_init, rh_page_is_kv, rh_page_update_crc,
    rh_kv_list,
};
use QuiverDB::page_ovf::{ovf_header_write, OvfPageHeader, ovf_parse_placeholder, ovf_read_chain, ovf_make_placeholder};
use QuiverDB::consts::{PAGE_HDR_V2_SIZE, PAGE_TYPE_OVERFLOW, NO_PAGE};

fn nanos() -> u128 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos()
}

fn unique_root(prefix: &str) -> PathBuf {
    let pid = std::process::id();
    let t = nanos();
    std::env::temp_dir().join(format!("qdbtest-crash-matrix-{prefix}-{pid}-{t}"))
}

// Реконструировать KV состояние напрямую из страниц (tail-wins + overflow)
fn reconstruct_state_without_directory(root: &PathBuf) -> Result<std::collections::HashMap<Vec<u8>, Vec<u8>>> {
    let pager = Pager::open(root)?;
    let ps = pager.meta.page_size as usize;
    let mut out = std::collections::HashMap::new();
    let mut page = vec![0u8; ps];

    for pid in 0..pager.meta.next_page_id {
        if pager.read_page(pid, &mut page).is_ok() && rh_page_is_kv(&page) {
            if let Ok(items) = rh_kv_list(&page) {
                for (k, v) in items {
                    let val = if let Some((total_len, head_pid)) = ovf_parse_placeholder(&v) {
                        ovf_read_chain(&pager, head_pid, Some(total_len as usize))?
                    } else {
                        v.to_vec()
                    };
                    out.insert(k.to_vec(), val);
                }
            }
        }
    }
    Ok(out)
}

// Сконструировать payload RH-страницы с одной записью (k->v), опционально — placeholder для overflow.
// Возвращает буфер страницы (page_size) с выставленным header.lsn и актуальным CRC.
fn build_kv_page_payload(
    page_size: usize,
    page_id: u64,
    hash_kind: QuiverDB::hash::HashKind,
    lsn: u64,
    key: &[u8],
    value_inline: Option<&[u8]>,
    ovf_placeholder: Option<[u8; 18]>,
) -> Result<Vec<u8>> {
    let mut buf = vec![0u8; page_size];
    rh_page_init(&mut buf, page_id)?;
    let to_insert = match (value_inline, ovf_placeholder) {
        (Some(v), _) => v.to_vec(),
        (_, Some(ph)) => ph.to_vec(),
        _ => return Err(anyhow!("either value_inline or ovf_placeholder must be provided")),
    };
    let ok = rh_kv_insert(&mut buf, hash_kind, key, &to_insert)?;
    if !ok {
        return Err(anyhow!("kv insert did not fit into empty page"));
    }
    // проставим LSN и CRC
    let mut h = rh_header_read(&buf)?;
    h.lsn = lsn;
    rh_header_write(&mut buf, &h)?;
    rh_page_update_crc(&mut buf)?;
    Ok(buf)
}

// Сконструировать payload overflow-страницы (одиночной) с k байтами (chunk),
// со ссылкой next_page_id (по цепочке), и lsn, crc.
fn build_ovf_page_payload(
    page_size: usize,
    page_id: u64,
    lsn: u64,
    chunk: &[u8],
    next_page_id: u64,
) -> Result<Vec<u8>> {
    if chunk.len() + PAGE_HDR_V2_SIZE > page_size {
        return Err(anyhow!("chunk too large for single overflow page"));
    }
    let mut buf = vec![0u8; page_size];
    let h = OvfPageHeader {
        version: 2,
        page_type: PAGE_TYPE_OVERFLOW,
        page_id,
        chunk_len: chunk.len() as u16,
        flags: 0,
        next_page_id,
        lsn,
    };
    ovf_header_write(&mut buf, &h)?;
    buf[PAGE_HDR_V2_SIZE..PAGE_HDR_V2_SIZE + chunk.len()].copy_from_slice(chunk);
    // общий CRC для v2
    rh_page_update_crc(&mut buf)?;
    Ok(buf)
}

// Приклеить кадр (PAGE_IMAGE) в конец WAL через низкоуровневое API (ручная сборка header + payload), с корректным CRC.
fn append_wal_frame(path: &PathBuf, rec_type: u8, lsn: u64, page_id: u64, payload: &[u8]) -> Result<()> {
    let mut f = OpenOptions::new().read(true).write(true).open(path)?;
    f.seek(SeekFrom::End(0))?;

    let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
    hdr[0] = rec_type;
    hdr[1] = 0;
    LittleEndian::write_u16(&mut hdr[2..4], 0);
    LittleEndian::write_u64(&mut hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8], lsn);
    LittleEndian::write_u64(&mut hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8], page_id);
    LittleEndian::write_u32(&mut hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4], payload.len() as u32);

    let mut hasher = Crc32::new();
    hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
    hasher.update(payload);
    let crc = hasher.finalize();
    LittleEndian::write_u32(&mut hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4], crc);

    f.write_all(&hdr)?;
    f.write_all(payload)?;
    f.sync_all()?;
    Ok(())
}

// Добавить "неизвестную" запись (type=99) с пустым payload (len=0).
fn append_unknown_record(path: &PathBuf) -> Result<()> {
    let mut f = OpenOptions::new().read(true).write(true).open(path)?;
    f.seek(SeekFrom::End(0))?;

    let mut hdr = vec![0u8; WAL_REC_HDR_SIZE];
    hdr[0] = 99; // неизвестный тип
    hdr[1] = 0;
    LittleEndian::write_u16(&mut hdr[2..4], 0);
    LittleEndian::write_u64(&mut hdr[WAL_REC_OFF_LSN..WAL_REC_OFF_LSN + 8], 0);
    LittleEndian::write_u64(&mut hdr[WAL_REC_OFF_PAGE_ID..WAL_REC_OFF_PAGE_ID + 8], 0);
    LittleEndian::write_u32(&mut hdr[WAL_REC_OFF_LEN..WAL_REC_OFF_LEN + 4], 0);

    let mut hasher = Crc32::new();
    hasher.update(&hdr[..WAL_REC_OFF_CRC32]);
    let crc = hasher.finalize();
    LittleEndian::write_u32(&mut hdr[WAL_REC_OFF_CRC32..WAL_REC_OFF_CRC32 + 4], crc);

    f.write_all(&hdr)?;
    f.sync_all()?;
    Ok(())
}

#[test]
fn crash_recovery_matrix_applies_and_truncates() -> Result<()> {
    std::env::set_var("P1_WAL_COALESCE_MS", "0");

    let root = unique_root("root");
    fs::create_dir_all(&root)?;
    init_db(&root, 4096)?;
    Directory::create(&root, 64)?;

    // page size / hash kind
    let ps = read_meta(&root)?.page_size as usize;
    let hash_kind = read_meta(&root)?.hash_kind;

    // 1) Сформируем полезные нагрузки:
    // - KV page id=0: key "a" -> overflow placeholder (head=10, total_len=len_big)
    // - OVERFLOW chain: pages 10..12 (три кусочка)
    // - KV page id=1: key "b" -> "v2" inline
    let key_a = b"a";
    let key_b = b"b";

    let cap = ps - PAGE_HDR_V2_SIZE;
    let big_len = (cap * 2) + (cap / 2); // 2.5 страницы overflow
    let mut big = vec![0u8; big_len];
    for i in 0..big.len() {
        big[i] = (i as u8).wrapping_mul(7).wrapping_add(13);
    }

    // overflow chain pages: 10 -> 11 -> 12
    let chunk1 = &big[0..cap];
    let chunk2 = &big[cap..cap * 2];
    let chunk3 = &big[cap * 2..];

    let ovf_p10 = build_ovf_page_payload(ps, 10, 1, chunk1, 11)?;
    let ovf_p11 = build_ovf_page_payload(ps, 11, 2, chunk2, 12)?;
    let ovf_p12 = build_ovf_page_payload(ps, 12, 3, chunk3, NO_PAGE)?;

    let placeholder = ovf_make_placeholder(big.len() as u64, 10);
    let kv_p0 = build_kv_page_payload(ps, 0, hash_kind, 4, key_a, None, Some(placeholder))?;
    let kv_p1 = build_kv_page_payload(ps, 1, hash_kind, 5, key_b, Some(b"v2"), None)?;

    // 2) Откроем WAL и приклеим кадры вручную: header уже есть (init_db).
    // Путь к WAL
    let wal_path = root.join(WAL_FILE);

    // Проверим, что WAL начинается с магии и длина = header
    {
        let mut f = OpenOptions::new().read(true).open(&wal_path)?;
        let mut hdr16 = vec![0u8; WAL_HDR_SIZE];
        f.read_exact(&mut hdr16)?;
        assert_eq!(&hdr16[..8], WAL_MAGIC);
        assert_eq!(f.metadata()?.len(), WAL_HDR_SIZE as u64);
    }

    // Добавим кадры: порядок не принципиален, но для LSN‑монотонности пусть растёт
    append_wal_frame(&wal_path, WAL_REC_PAGE_IMAGE, 1, 10, &ovf_p10)?; // ovf
    append_wal_frame(&wal_path, WAL_REC_PAGE_IMAGE, 2, 11, &ovf_p11)?;
    append_wal_frame(&wal_path, WAL_REC_PAGE_IMAGE, 3, 12, &ovf_p12)?;
    append_wal_frame(&wal_path, WAL_REC_PAGE_IMAGE, 4, 0, &kv_p0)?;  // kv 'a' -> placeholder
    append_wal_frame(&wal_path, WAL_REC_PAGE_IMAGE, 5, 1, &kv_p1)?;  // kv 'b' -> "v2"

    // Вставим неизвестный кадр
    append_unknown_record(&wal_path)?;

    // Добавим ещё один валидный кадр, а затем испортим хвост (обрезанный payload)
    let kv_p1_v3 = build_kv_page_payload(ps, 1, hash_kind, 6, key_b, Some(b"v3"), None)?;
    append_wal_frame(&wal_path, WAL_REC_PAGE_IMAGE, 6, 1, &kv_p1_v3)?;

    // Обрежем последний кадр: оставим только заголовок и первые байты payload
    {
        let mut f = OpenOptions::new().read(true).write(true).open(&wal_path)?;
        let len = f.metadata()?.len();
        // Отмотаем на начало последнего кадра: найдём позицию (len - (hdr+payload) последнего).
        // У нас нет прямой позиции, упростим: перезапишем конец файла на "частичный хвост":
        // Считаем последние байты: прочтём весь WAL и вручную укоротим.
        let mut all = Vec::with_capacity(len as usize);
        f.seek(SeekFrom::Start(0))?;
        f.read_to_end(&mut all)?;
        // По формату: последний полный кадр = header(28) + payload(ps)
        // Обрежем до header + 8 байт payload (меньше нормального).
        // Найдём с конца позицию начала кадра: len - (28 + ps)
        let cut_from = len.saturating_sub((WAL_REC_HDR_SIZE + ps) as u64) as usize;
        let keep = WAL_REC_HDR_SIZE + 8;
        let new_len = (cut_from + keep).min(all.len());
        f.set_len(new_len as u64)?;
        f.sync_all()?;
    }

    // 3) Смоделируем "crash": clean_shutdown=false. Файлы сегментов отсутствуют.
    set_clean_shutdown(&root, false)?;

    // 4) Writer open -> wal_replay_if_any должен:
    // - применить все валидные кадры,
    // - проигнорировать unknown,
    // - остановиться на partial tail,
    // - усечь WAL до заголовка.
    {
        let _db = Db::open(&root)?; // вызывает wal_replay_if_any
    }

    // Проверка: WAL усечён до заголовка
    {
        let len2 = fs::metadata(&wal_path)?.len();
        assert_eq!(len2, WAL_HDR_SIZE as u64, "WAL must be truncated to header after replay");
    }

    // Реконструкция состояния из страниц
    let state = reconstruct_state_without_directory(&root)?;
    // Должны быть ключи "a" и "b"
    let va = state.get(b"a" as &[u8]).expect("key 'a' must exist");
    let vb = state.get(b"b" as &[u8]).expect("key 'b' must exist");

    // 'a' — overflow, сравним побайтно
    assert_eq!(va.len(), big.len(), "overflow value length mismatch");
    assert_eq!(&va[..], &big[..], "overflow value content mismatch");

    // 'b' — с учётом partial tail: последний полный кадр для 'b' — LSN=5 ("v2").
    // Кадр с LSN=6 был обрезан => должен быть проигнорирован.
    assert_eq!(vb.as_slice(), b"v2", "key 'b' must equal last complete frame (v2)");

    Ok(())
}