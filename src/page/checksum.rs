//! page/checksum — 16-байтовый трейлер страницы.
//!
//! 2.0 (по умолчанию):
//! - Фиксированный CRC32C: низшие 4 байта трейлера — digest (LE), оставшиеся 12 — нули.
//! - stored==0 трактуется как “OK” для совместимости/нулевых страниц.
//!
//! 2.1 (TDE prep, AEAD-tag):
//! - Альтернативный режим — AES-256-GCM: весь трейлер (16 байт) — это AEAD-tag.
//! - Никакого шифрования payload не выполняется (tag-only режим).
//! - Nonce формируется детерминированно из (page_id, lsn) с помощью derive_gcm_nonce.
//! - NEW: AAD (Associated Data) добавлена для усиления: b"P2AEAD01" || page[0..16] (MAGIC, version, type, page_id).
//!
//! NEW (perf toggle):
//! - Можно полностью отключить расчёт и проверку CRC32C для страниц (бенч/разработка):
//!   * P1_PAGE_CHECKSUM=0|false|off|no  — отключить;
//!   * или P1_DISABLE_PAGE_CHECKSUM=1|true|yes|on — отключить.
//! - При отключении update — зануление трейлера, verify — всегда Ok(true).
//!
//! Примечание: текущий pager/page код продолжает использовать CRC32C-функции,
//! а AEAD-функции задействуются, когда включён TDE (pager.tde_enabled=true).

use anyhow::{anyhow, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::sync::OnceLock;

use super::common::TRAILER_LEN;

/// Магия для AAD в AEAD-режиме (версионируемая).
const AEAD_AAD_MAGIC: &[u8; 8] = b"P2AEAD01";

// ---------- ENV toggle: disable page checksum ----------

fn page_checksum_disabled() -> bool {
    static DISABLED: OnceLock<bool> = OnceLock::new();
    *DISABLED.get_or_init(|| {
        // Основной переключатель
        if let Ok(v) = std::env::var("P1_PAGE_CHECKSUM") {
            let s = v.trim().to_ascii_lowercase();
            if s == "0" || s == "false" || s == "off" || s == "no" {
                return true;
            }
        }
        // Альтернативный флаг (для совместимости)
        if let Ok(v) = std::env::var("P1_DISABLE_PAGE_CHECKSUM") {
            let s = v.trim().to_ascii_lowercase();
            if s == "1" || s == "true" || s == "yes" || s == "on" {
                return true;
            }
        }
        false
    })
}

// ---------- CRC32C (2.0, по умолчанию) ----------

#[inline]
fn compute_crc32c(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// Обновить трейлер чексуммы страницы (CRC32C).
/// Требуется: page.len() >= TRAILER_LEN.
/// Если page checksum отключён через ENV — трейлер зануляется без расчёта.
pub fn page_update_checksum(page: &mut [u8], _checksum_kind: u8) -> Result<()> {
    if page.len() < TRAILER_LEN {
        return Err(anyhow!("page buffer too small for checksum"));
    }
    let ps = page.len();

    if page_checksum_disabled() {
        // Просто занулим трейлер — verify в этом режиме пройдёт без расчёта.
        for b in &mut page[ps - TRAILER_LEN..ps] {
            *b = 0;
        }
        return Ok(());
    }

    // Обнулить трейлер перед расчётом
    for b in &mut page[ps - TRAILER_LEN..ps] {
        *b = 0;
    }

    // CRC32C по всей странице (с нулённым трейлером)
    let digest32 = compute_crc32c(&page[..]);

    // Записать digest (LE) и занулить оставшиеся 12 байт
    LittleEndian::write_u32(&mut page[ps - TRAILER_LEN..ps - TRAILER_LEN + 4], digest32);
    for b in &mut page[ps - TRAILER_LEN + 4..ps] {
        *b = 0;
    }
    Ok(())
}

/// Проверить трейлер чексуммы страницы (CRC32C). true = ок.
/// Если page checksum отключён через ENV — всегда Ok(true).
pub fn page_verify_checksum(page: &[u8], _checksum_kind: u8) -> Result<bool> {
    if page.len() < TRAILER_LEN {
        return Err(anyhow!("page buffer too small for checksum verify"));
    }
    if page_checksum_disabled() {
        return Ok(true);
    }

    let ps = page.len();

    // Считаем stored==0 как “OK” (совместимость для нулевых/неинициализированных страниц).
    let stored32 = LittleEndian::read_u32(&page[ps - TRAILER_LEN..ps - TRAILER_LEN + 4]);
    if stored32 == 0 {
        return Ok(true);
    }

    // Скопировать страницу, занулить трейлер и пересчитать CRC32C
    let mut copy = page.to_vec();
    for b in &mut copy[ps - TRAILER_LEN..ps] {
        *b = 0;
    }

    let calc32 = compute_crc32c(&copy[..]);
    Ok(stored32 == calc32)
}

// ---------- AES-256-GCM tag-only (TDE) ----------

use aes_gcm::{
    aead::{AeadInPlace, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use crate::crypto::derive_gcm_nonce;

/// Построить AAD для AEAD-тега:
///   AAD = "P2AEAD01" || page[0..16] (MAGIC[4], version u16, type u16, page_id u64)
#[inline]
fn build_aead_aad(page: &[u8]) -> Result<[u8; 24]> {
    if page.len() < 16 {
        return Err(anyhow!("page too small (<16) for AEAD AAD"));
    }
    let mut aad = [0u8; 24];
    aad[0..8].copy_from_slice(AEAD_AAD_MAGIC);
    aad[8..24].copy_from_slice(&page[0..16]);
    Ok(aad)
}

/// Константно‑временное сравнение двух буферов одинаковой длины.
#[inline]
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut acc = 0u8;
    for i in 0..a.len() {
        acc |= a[i] ^ b[i];
    }
    acc == 0
}

/// Обновить трейлер страницы 16-байтовым AEAD-tag (AES-256-GCM).
/// - Никакое шифрование данных не выполняется (tag-only).
/// - Nonce (12 байт) передаётся явно.
/// - Тег вычисляется над всей страницей с занулённым трейлером (AAD = MAGIC "P2AEAD01" + header[0..16]).
pub fn page_update_trailer_aead_tag(page: &mut [u8], key_32: &[u8; 32], nonce12: [u8; 12]) -> Result<()> {
    if page.len() < TRAILER_LEN {
        return Err(anyhow!("page buffer too small for AEAD trailer"));
    }
    let ps = page.len();

    // Сформируем копию с занулённым трейлером для вычисления тега
    let mut tmp = page.to_vec();
    for b in &mut tmp[ps - TRAILER_LEN..ps] {
        *b = 0;
    }

    // AES-256-GCM
    let key = Key::<Aes256Gcm>::from_slice(key_32);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(&nonce12);

    // AAD
    let aad = build_aead_aad(&tmp)?; // header уже в tmp идентичен page

    // encrypt_in_place_detached возвращает tag, не меняя tmp (мы не шифруем payload)
    let tag = cipher
        .encrypt_in_place_detached(nonce, &aad, &mut tmp)
        .map_err(|e| anyhow!("aead tag compute failed: {}", e))?;

    // Запишем tag (16 байт) в трейлер
    let tag_bytes = tag.as_slice();
    if tag_bytes.len() != TRAILER_LEN {
        return Err(anyhow!("aead tag len != trailer len ({} != {})", tag_bytes.len(), TRAILER_LEN));
    }
    page[ps - TRAILER_LEN..ps].copy_from_slice(tag_bytes);
    Ok(())
}

/// Проверить 16-байтовый AEAD-tag в трейлере (AES-256-GCM).
/// - Nonce (12 байт) передаётся явно.
/// - Возвращает true при совпадении, иначе false.
/// - stored == 0..0 (все нули) возвращает false (строго).
pub fn page_verify_trailer_aead_tag(page: &[u8], key_32: &[u8; 32], nonce12: [u8; 12]) -> Result<bool> {
    if page.len() < TRAILER_LEN {
        return Err(anyhow!("page buffer too small for AEAD verify"));
    }
    let ps = page.len();

    let stored = &page[ps - TRAILER_LEN..ps];
    // Для тега нулевой трейлер считаем невалидным (строго)
    if stored.iter().all(|&b| b == 0) {
        return Ok(false);
    }

    // Скопируем и занулим трейлер для вычисления нового тега
    let mut tmp = page.to_vec();
    for b in &mut tmp[ps - TRAILER_LEN..ps] {
        *b = 0;
    }

    // AES-256-GCM
    let key = Key::<Aes256Gcm>::from_slice(key_32);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(&nonce12);

    // AAD
    let aad = build_aead_aad(&tmp)?;

    // Рассчитаем ожидаемый tag
    let tag = cipher
        .encrypt_in_place_detached(nonce, &aad, &mut tmp)
        .map_err(|e| anyhow!("aead tag compute failed: {}", e))?;

    Ok(constant_time_eq(stored, tag.as_slice()))
}

/// Удобная обёртка: обновить трейлер AEAD-tag, дернув nonce из (page_id, lsn).
#[inline]
pub fn page_update_trailer_aead_with(page: &mut [u8], key_32: &[u8; 32], page_id: u64, lsn: u64) -> Result<()> {
    let nonce = derive_gcm_nonce(page_id, lsn);
    page_update_trailer_aead_tag(page, key_32, nonce)
}

/// Удобная обёртка: проверить AEAD-tag, дернув nonce из (page_id, lsn).
#[inline]
pub fn page_verify_trailer_aead_with(page: &[u8], key_32: &[u8; 32], page_id: u64, lsn: u64) -> Result<bool> {
    let nonce = derive_gcm_nonce(page_id, lsn);
    page_verify_trailer_aead_tag(page, key_32, nonce)
}

// ---------- Helpers (new): чтение поля CRC из трейлера ----------

/// Прочитать низшие 4 байта трейлера страницы как u32 (LE).
/// Полезно для диагностики и «строгой» проверки нулевого трейлера в CRC‑режиме.
/// Возвращает Err, если буфер меньше TRAILER_LEN.
#[inline]
pub fn page_trailer_crc32_le(page: &[u8]) -> Result<u32> {
    if page.len() < TRAILER_LEN {
        return Err(anyhow!("page buffer too small for trailer read"));
    }
    let ps = page.len();
    Ok(LittleEndian::read_u32(&page[ps - TRAILER_LEN..ps - TRAILER_LEN + 4]))
}

/// Проверка «нулевого трейлера» для CRC‑режима (stored CRC32 == 0).
/// Для TDE/AEAD этот хелпер неприменим (там весь трейлер — tag).
#[inline]
pub fn page_trailer_is_zero_crc32(page: &[u8]) -> Result<bool> {
    Ok(page_trailer_crc32_le(page)? == 0)
}