//! db/doctor — проверка целостности страниц (CRC/IO), типизация страниц и отчёт JSON.
//!
//! Дополнено:
//! - Счётчик zero_checksum — страницы, у которых трейлер checksum хранит 0 (валидно
//!   для pager.read_page, но это “нулевые/неинициализированные” страницы).
//!   Полезно отличать их от реально валидных страниц.
//! - Строгий режим через ENV P1_DOCTOR_STRICT=1|true|yes|on:
//!   * zero_checksum не считаются ok_pages (всегда);
//!   * а также учитываются как crc_fail (строгое трактование).
//!
//! NEW:
//! - Отчёт теперь печатает режим верификации трейлера: mode="aead" или "crc".
//! - Печатаются строгие флаги: doctor_strict, zero_checksum_strict, tde_strict.
//! - В AEAD-режиме zero_checksum не применяется и каждое успешное чтение считает страницу ok.
//!
//! Семантика:
//! - Проходим все page_id ∈ [0 .. meta.next_page_id).
//! - Сначала пытаемся прочитать через pager.read_page (с проверкой трейлера):
//!   * В режиме 2.0 — CRC32C, в режиме TDE — AEAD‑tag (AES‑GCM).
//!   * Успех → типизируем страницу (KV/OVF/other_magic/no_magic).
//!             Для CRC: если stored CRC32 == 0 → zero_checksum++; (ok_pages не увеличиваем);
//!             если doctor_strict=true → zero_checksum также учитываются как crc_fail.
//!             Для AEAD: zero_checksum не применяется; ok_pages++.
//!   * Ошибка → различаем нарушения целостности (crc_fail/AEAD tag) от ошибок ввода‑вывода (io_fail):
//!       - Сообщения, содержащие "checksum" или "tag" (AEAD tag mismatch) → crc_fail;
//!       - Иначе → io_fail.
//!     Дополнительно пробуем raw-read для классификации magic.
//!
//! Вывод:
//! - doctor(json=false) — человекочитаемый отчёт;
//! - doctor(json=true)  — JSON-объект на одной строке.

use anyhow::{Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::fs::OpenOptions;
use std::io::{Read, Seek, SeekFrom};

use crate::page::{OFF_TYPE, PAGE_MAGIC, PAGE_TYPE_KV_RH3, PAGE_TYPE_OVERFLOW3, TRAILER_LEN};
use crate::pager::{DATA_SEG_EXT, DATA_SEG_PREFIX, SEGMENT_SIZE};

use super::core::Db;

// --- локальные чтения ENV для строгих режимов (для отчёта) ---

#[inline]
fn doctor_strict_env() -> bool {
    std::env::var("P1_DOCTOR_STRICT")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

#[inline]
fn zero_cksum_strict_env() -> bool {
    std::env::var("P1_ZERO_CHECKSUM_STRICT")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

#[inline]
fn tde_strict_env() -> bool {
    std::env::var("P1_TDE_STRICT")
        .ok()
        .map(|s| s.to_ascii_lowercase())
        .map(|s| s == "1" || s == "true" || s == "yes" || s == "on")
        .unwrap_or(false)
}

impl Db {
    /// Doctor-скан: проверка CRC/IO, типизация страниц и отчёт (json=false|true).
    pub fn doctor(&self, json: bool) -> Result<()> {
        let ps = self.pager.meta.page_size as usize;
        let pages_total = self.pager.meta.next_page_id;

        // Строгие режимы
        let doctor_strict = doctor_strict_env();
        let zero_cksum_strict = zero_cksum_strict_env();
        let tde_strict = tde_strict_env();

        // Режим верификации трейлера
        let mode_aead = self.pager.tde_enabled();
        let mode_str = if mode_aead { "aead" } else { "crc" };

        let mut ok_pages = 0u64;
        let mut crc_fail = 0u64;
        let mut io_fail = 0u64;

        let mut kv_pages = 0u64;
        let mut ovf_pages = 0u64;
        let mut other_magic = 0u64;
        let mut no_magic = 0u64;

        // Отдельный счётчик страниц с нулевым checksum (CRC режим)
        let mut zero_checksum = 0u64;

        for pid in 0..pages_total {
            let mut buf = vec![0u8; ps];
            match self.pager.read_page(pid, &mut buf) {
                Ok(()) => {
                    // Типизация
                    classify_bytes(
                        &buf,
                        &mut kv_pages,
                        &mut ovf_pages,
                        &mut other_magic,
                        &mut no_magic,
                    );

                    if mode_aead {
                        // AEAD-режим: zero_checksum не применим, страница засчитывается как ok
                        ok_pages += 1;
                    } else {
                        // CRC-режим: проверяем stored checksum (низшие 4 байта трейлера)
                        let stored32 = if ps >= TRAILER_LEN {
                            LittleEndian::read_u32(&buf[ps - TRAILER_LEN..ps - TRAILER_LEN + 4])
                        } else {
                            0
                        };

                        if stored32 == 0 {
                            zero_checksum += 1;
                            // В строгом режиме doctor_strict считаем как нарушение целостности
                            if doctor_strict {
                                crc_fail += 1;
                            }
                            // zero_checksum НЕ учитываем как ok_pages
                        } else {
                            ok_pages += 1;
                        }
                    }
                }
                Err(e) => {
                    let msg = e.to_string();
                    let msg_l = msg.to_ascii_lowercase();
                    // Нарушение целостности: "checksum mismatch" (CRC) или "tag mismatch" (AEAD)
                    if msg_l.contains("checksum") || msg_l.contains("tag") {
                        crc_fail += 1;
                    } else {
                        io_fail += 1;
                    }
                    // Попробуем raw-read, чтобы классифицировать magic.
                    if let Ok(raw) = raw_read_page(self, pid, ps) {
                        classify_bytes(
                            &raw,
                            &mut kv_pages,
                            &mut ovf_pages,
                            &mut other_magic,
                            &mut no_magic,
                        );
                    } else {
                        no_magic += 1; // не смогли прочитать даже raw — учитываем как no_magic
                    }
                }
            }
        }

        if json {
            println!(
                "{{\
                    \"mode\":\"{}\",\
                    \"strict\":{{\"doctor_strict\":{},\"zero_checksum_strict\":{},\"tde_strict\":{}}},\
                    \"pages_total\":{},\
                    \"ok_pages\":{},\
                    \"zero_checksum\":{},\
                    \"crc_fail\":{},\
                    \"io_fail\":{},\
                    \"kv_pages\":{},\
                    \"overflow_pages\":{},\
                    \"other_magic\":{},\
                    \"no_magic\":{}\
                }}",
                mode_str,
                doctor_strict,
                zero_cksum_strict,
                tde_strict,
                pages_total,
                ok_pages,
                // zero_checksum информативен только в CRC-режиме; в AEAD остаётся 0
                zero_checksum,
                crc_fail,
                io_fail,
                kv_pages,
                ovf_pages,
                other_magic,
                no_magic
            );
        } else {
            println!(
                "Doctor report (doctor_strict={}, mode={}, zero_checksum_strict={}, tde_strict={}):",
                doctor_strict, mode_str, zero_cksum_strict, tde_strict
            );
            println!("  pages_total    = {}", pages_total);
            println!("  ok_pages       = {}", ok_pages);
            if mode_aead {
                println!("  zero_checksum  = (n/a for AEAD)");
            } else {
                println!("  zero_checksum  = {}", zero_checksum);
            }
            println!("  crc_fail       = {}", crc_fail);
            println!("  io_fail        = {}", io_fail);
            println!("  kv_pages       = {}", kv_pages);
            println!("  overflow_pages = {}", ovf_pages);
            println!("  other_magic    = {}", other_magic);
            println!("  no_magic       = {}", no_magic);
        }
        Ok(())
    }
}

// ---------- helpers ----------

fn classify_bytes(
    page: &[u8],
    kv_pages: &mut u64,
    ovf_pages: &mut u64,
    other_magic: &mut u64,
    no_magic: &mut u64,
) {
    if page.len() < 12 {
        *no_magic += 1;
        return;
    }
    if &page[0..4] != PAGE_MAGIC {
        *no_magic += 1;
        return;
    }
    let page_type = LittleEndian::read_u16(&page[OFF_TYPE..OFF_TYPE + 2]);
    match page_type {
        t if t == PAGE_TYPE_KV_RH3 => *kv_pages += 1,
        t if t == PAGE_TYPE_OVERFLOW3 => *ovf_pages += 1,
        _ => *other_magic += 1,
    }
}

fn raw_read_page(db: &Db, page_id: u64, page_size: usize) -> Result<Vec<u8>> {
    // Рассчитаем (seg_no, off) как в locate()
    let ps_u64 = page_size as u64;
    let pps = (SEGMENT_SIZE / ps_u64).max(1);
    let seg_no = (page_id / pps) + 1;
    let off = (page_id % pps) * ps_u64;

    let path = db
        .root
        .join(format!("{}{:06}.{}", DATA_SEG_PREFIX, seg_no, DATA_SEG_EXT));
    let mut f = OpenOptions::new()
        .read(true)
        .open(&path)
        .with_context(|| format!("open segment {}", path.display()))?;
    let mut buf = vec![0u8; page_size];
    f.seek(SeekFrom::Start(off))?;
    f.read_exact(&mut buf)?;
    Ok(buf)
}
