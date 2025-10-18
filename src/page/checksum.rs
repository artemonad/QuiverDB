//! page/checksum — 16-байтовый трейлер страницы.
//!
//! CRC режим (по умолчанию и единственный): CRC32C (Castagnoli).
//! - trailer[0..4] — CRC32C (LE) по всей странице с занулённым трейлером;
//! - trailer[4..16] — нули;
//! - stored == 0 считается допустимым (нулевая/пустая страница), если не включён ZERO_CHECKSUM_STRICT;
//! - ENV P1_PAGE_CHECKSUM=0|false|off|no (или P1_DISABLE_PAGE_CHECKSUM=1) — полностью выключает расчёт/проверку (бенчи/разработка).
//!
//! AEAD режим (TDE): AES‑256‑GCM tag‑only (интегритет без шифрования payload).
//! - trailer = 16‑байтовый тег;
//! - AAD = "P2AEAD01" || page[0..16] (MAGIC, version, type, page_id);
//! - Nonce = derive_gcm_nonce(page_id, lsn);
//! - update/verify считают тег над копией страницы с занулённым трейлером — данные страницы не изменяются.
//!
//! Примечание (переходный шаг):
//! - В сигнатурах page_update_checksum/page_verify_checksum параметр checksum_kind сохранён,
//!   но игнорируется (всегда CRC32C). В следующем шаге он будет удалён из API и из meta.
//!
//! Strict helper verify_page_crc_strict_kind также сведён к CRC32C и игнорирует параметр kind.

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

// ---------- CRC32C (единственный режим) ----------

#[inline]
fn compute_crc32c(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// Обновить трейлер чексуммы страницы (CRC32C единственный).
/// Параметр checksum_kind игнорируется (переходный шаг).
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

/// Проверить трейлер чексуммы страницы (CRC32C единственный). true = ок.
/// Параметр checksum_kind игнорируется (переходный шаг).
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

use crate::crypto::derive_gcm_nonce;
use aes_gcm::{
    aead::{AeadInPlace, KeyInit},
    Aes256Gcm, Key, Nonce,
};

/// Построить AAD для AEAD-тега:
///   AAD = "P2AEAD01" || page[0..16] (MAGIC, version, type, page_id)
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

/// Вычислить 16‑байтовый AEAD‑тег для страницы:
/// - Копируем страницу и зануляем её трейлер;
/// - AAD = "P2AEAD01" || header[0..16];
/// - Возвращаем tag (payload страницы не модифицируется).
#[inline]
fn compute_aead_tag_for_page(
    page: &[u8],
    key_32: &[u8; 32],
    nonce12: &[u8; 12],
) -> Result<[u8; 16]> {
    if page.len() < TRAILER_LEN {
        return Err(anyhow!("page buffer too small for AEAD tag compute"));
    }
    let ps = page.len();

    // Копия с занулённым трейлером
    let mut tmp = page.to_vec();
    for b in &mut tmp[ps - TRAILER_LEN..ps] {
        *b = 0;
    }

    let key = Key::<Aes256Gcm>::from_slice(key_32);
    let cipher = Aes256Gcm::new(key);
    let nonce = Nonce::from_slice(nonce12);

    // AAD по копии (эквивалентно исходной странице для [0..16])
    let aad = build_aead_aad(&tmp)?;

    // encrypt_in_place_detached возвращает tag; изменённый tmp нас не волнует (это копия)
    let tag = cipher
        .encrypt_in_place_detached(nonce, &aad, &mut tmp)
        .map_err(|e| anyhow!("aead tag compute failed: {}", e))?;

    let mut out = [0u8; 16];
    out.copy_from_slice(tag.as_slice());
    Ok(out)
}

/// Обновить трейлер страницы 16-байтовым AEAD‑тегом (AES-256‑GCM).
/// Данные страницы не модифицируются — тег считается на копии.
pub fn page_update_trailer_aead_tag(
    page: &mut [u8],
    key_32: &[u8; 32],
    nonce12: [u8; 12],
) -> Result<()> {
    if page.len() < TRAILER_LEN {
        return Err(anyhow!("page buffer too small for AEAD trailer"));
    }
    let ps = page.len();
    let tag = compute_aead_tag_for_page(page, key_32, &nonce12)?;
    page[ps - TRAILER_LEN..ps].copy_from_slice(&tag);
    Ok(())
}

/// Проверить 16-байтовый AEAD‑тег в трейлере (AES-256‑GCM).
/// - Возвращает true при совпадении, иначе false.
/// - stored == 0..0 (все нули) возвращает false (строго).
pub fn page_verify_trailer_aead_tag(
    page: &[u8],
    key_32: &[u8; 32],
    nonce12: [u8; 12],
) -> Result<bool> {
    if page.len() < TRAILER_LEN {
        return Err(anyhow!("page buffer too small for AEAD verify"));
    }
    let ps = page.len();

    let stored = &page[ps - TRAILER_LEN..ps];
    // Для тега нулевой трейлер считаем невалидным (строго)
    if stored.iter().all(|&b| b == 0) {
        return Ok(false);
    }

    let calc = compute_aead_tag_for_page(page, key_32, &nonce12)?;
    Ok(constant_time_eq(stored, &calc))
}

/// Удобная обёртка: обновить трейлер AEAD‑tag, дернув nonce из (page_id, lsn).
#[inline]
pub fn page_update_trailer_aead_with(
    page: &mut [u8],
    key_32: &[u8; 32],
    page_id: u64,
    lsn: u64,
) -> Result<()> {
    let nonce = derive_gcm_nonce(page_id, lsn);
    page_update_trailer_aead_tag(page, key_32, nonce)
}

/// Удобная обёртка: проверить AEAD‑tag, дернув nonce из (page_id, lsn).
#[inline]
pub fn page_verify_trailer_aead_with(
    page: &[u8],
    key_32: &[u8; 32],
    page_id: u64,
    lsn: u64,
) -> Result<bool> {
    let nonce = derive_gcm_nonce(page_id, lsn);
    page_verify_trailer_aead_tag(page, key_32, nonce)
}

// ---------- Helpers (чтение поля CRC из трейлера) ----------

/// Прочитать низшие 4 байта трейлера страницы как u32 (LE).
#[inline]
pub fn page_trailer_crc32_le(page: &[u8]) -> Result<u32> {
    if page.len() < TRAILER_LEN {
        return Err(anyhow!("page buffer too small for trailer read"));
    }
    let ps = page.len();
    Ok(LittleEndian::read_u32(
        &page[ps - TRAILER_LEN..ps - TRAILER_LEN + 4],
    ))
}

/// Проверка «нулевого трейлера» для CRC‑режима (stored CRC32 == 0).
#[inline]
pub fn page_trailer_is_zero_crc32(page: &[u8]) -> Result<bool> {
    Ok(page_trailer_crc32_le(page)? == 0)
}

// ---------- Strict verify helper (используется в pager/io) ----------

/// Строгая CRC‑проверка трейлера страницы, игнорирующая ENV‑тумблеры.
/// Всегда использует CRC32C (Castagnoli). Параметр checksum_kind игнорируется.
pub fn verify_page_crc_strict_kind(page: &[u8], _checksum_kind: u8) -> Result<bool> {
    if page.len() < TRAILER_LEN {
        return Err(anyhow!("page buffer too small for strict CRC verify"));
    }
    let ps = page.len();

    let stored32 = LittleEndian::read_u32(&page[ps - TRAILER_LEN..ps - TRAILER_LEN + 4]);
    if stored32 == 0 {
        return Ok(false);
    }

    let mut copy = page.to_vec();
    for b in &mut copy[ps - TRAILER_LEN..ps] {
        *b = 0;
    }

    let calc = compute_crc32c(&copy[..]);
    Ok(stored32 == calc)
}
