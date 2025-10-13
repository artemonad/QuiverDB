//! crypto/kms — KMS skeleton (envelope для DEK) на AES‑256‑GCM.
//!
//! Цели 2.1:
//! - Минимальный интерфейс KMS для wrap/unwrap DEK.
//! - Провайдер из ENV (EnvKmsProvider), пригодный для dev/CI.
//!
//! Формат обёртки (LE):
//!   MAGIC8 = "P2KMS01\0"
//!   u32 version = 1
//!   u16 kid_len
//!   [kid bytes (UTF‑8)]
//!   [nonce 12]
//!   [tag 16]
//!   u32 ct_len
//!   [ciphertext bytes]
//!
//! AAD = "P2KMS01" || kid (bytes).
//! Nonce = 12 случайных байт (rand::rngs::OsRng).
//!
//! ENV:
//!   P1_KMS_KEK_HEX / P1_KMS_KEK_BASE64 — 32‑байтовый KEK
//!   P1_KMS_KEK_KID — KID KEK’а (по умолчанию "kek-default")

use anyhow::{anyhow, Result};
use aes_gcm::{
    aead::{generic_array::GenericArray, AeadInPlace, KeyInit},
    Aes256Gcm, Key, Nonce,
};
use base64::Engine;
use byteorder::{ByteOrder, LittleEndian};
use rand::rngs::OsRng;
use rand::RngCore;
use zeroize::Zeroize;

const MAGIC: &[u8; 8] = b"P2KMS01\0";
const VERSION_V1: u32 = 1;
const AAD_PREFIX: &[u8; 7] = b"P2KMS01";

/// Публичный интерфейс KMS провайдера (KEK → wrap/unwrap DEK).
pub trait KmsProvider: Send + Sync {
    /// KID KEK’а по умолчанию (например, "kek-default").
    fn default_kek_kid(&self) -> &str;

    /// Обернуть DEK выбранным KEK (kid). Возвращает сериализованный envelope.
    fn wrap(&self, kek_kid: &str, dek: &[u8]) -> Result<Vec<u8>>;

    /// Распаковать envelope, вернуть (kid, dek).
    fn unwrap(&self, wrapped: &[u8]) -> Result<(String, Vec<u8>)>;
}

/// Провайдер на ENV (KEK из переменных окружения).
///
/// ENV:
///   P1_KMS_KEK_HEX / P1_KMS_KEK_BASE64 — KEK 32 байта
///   P1_KMS_KEK_KID — KID (по умолчанию "kek-default")
#[derive(Clone, Debug)]
pub struct EnvKmsProvider {
    kid: String,
    kek: [u8; 32],
}

impl EnvKmsProvider {
    pub fn from_env() -> Result<Self> {
        let kid = std::env::var("P1_KMS_KEK_KID").unwrap_or_else(|_| "kek-default".to_string());
        // Порядок: HEX -> BASE64
        if let Ok(hex) = std::env::var("P1_KMS_KEK_HEX") {
            let v = decode_hex_trimmed(&hex)?;
            let kek = slice32(&v)?;
            return Ok(Self { kid, kek });
        }
        if let Ok(b64) = std::env::var("P1_KMS_KEK_BASE64") {
            let v = decode_base64_trimmed(&b64)?;
            let kek = slice32(&v)?;
            return Ok(Self { kid, kek });
        }
        Err(anyhow!(
            "EnvKmsProvider: set P1_KMS_KEK_HEX or P1_KMS_KEK_BASE64 (32 bytes)"
        ))
    }
}

impl KmsProvider for EnvKmsProvider {
    fn default_kek_kid(&self) -> &str {
        &self.kid
    }

    fn wrap(&self, kek_kid: &str, dek: &[u8]) -> Result<Vec<u8>> {
        if kek_kid != self.kid {
            return Err(anyhow!("unknown KEK KID '{}'", kek_kid));
        }
        let mut pt = dek.to_vec(); // plaintext (будет зашифрован in place)
        let mut nonce = [0u8; 12];
        OsRng.fill_bytes(&mut nonce);

        let key = Key::<Aes256Gcm>::from_slice(&self.kek);
        let cipher = Aes256Gcm::new(key);
        let aad = build_aad(kek_kid);

        let tag = cipher
            .encrypt_in_place_detached(Nonce::from_slice(&nonce), &aad, &mut pt)
            .map_err(|e| anyhow!("kms wrap aes-gcm: {}", e))?;

        // Сериализуем envelope
        let mut out = Vec::with_capacity(8 + 4 + 2 + kek_kid.len() + 12 + 16 + 4 + pt.len());
        out.extend_from_slice(MAGIC);
        let mut buf4 = [0u8; 4];
        LittleEndian::write_u32(&mut buf4, VERSION_V1);
        out.extend_from_slice(&buf4);
        let kid_len = kek_kid.len();
        if kid_len > u16::MAX as usize {
            return Err(anyhow!("KEK KID too long"));
        }
        let mut buf2 = [0u8; 2];
        LittleEndian::write_u16(&mut buf2, kid_len as u16);
        out.extend_from_slice(&buf2);
        out.extend_from_slice(kek_kid.as_bytes());
        out.extend_from_slice(&nonce);
        out.extend_from_slice(tag.as_slice());
        LittleEndian::write_u32(&mut buf4, pt.len() as u32);
        out.extend_from_slice(&buf4);
        out.extend_from_slice(&pt);

        // очистим pt копию (на всякий случай)
        pt.zeroize();

        Ok(out)
    }

    fn unwrap(&self, wrapped: &[u8]) -> Result<(String, Vec<u8>)> {
        if wrapped.len() < 8 + 4 + 2 {
            return Err(anyhow!("wrapped KMS blob too short"));
        }
        if &wrapped[..8] != MAGIC {
            return Err(anyhow!("bad KMS magic"));
        }
        let mut off = 8usize;
        let ver = LittleEndian::read_u32(&wrapped[off..off + 4]);
        off += 4;
        if ver != VERSION_V1 {
            return Err(anyhow!("unsupported KMS version {}", ver));
        }
        let kid_len = LittleEndian::read_u16(&wrapped[off..off + 2]) as usize;
        off += 2;
        if wrapped.len() < off + kid_len + 12 + 16 + 4 {
            return Err(anyhow!("wrapped KMS blob truncated (header)"));
        }
        let kid_bytes = &wrapped[off..off + kid_len];
        let kid = String::from_utf8(kid_bytes.to_vec()).map_err(|e| anyhow!("kid utf8: {}", e))?;
        off += kid_len;

        let nonce = &wrapped[off..off + 12];
        off += 12;
        let tag = &wrapped[off..off + 16];
        off += 16;
        let ct_len = LittleEndian::read_u32(&wrapped[off..off + 4]) as usize;
        off += 4;
        if wrapped.len() < off + ct_len {
            return Err(anyhow!("wrapped KMS blob truncated (ciphertext)"));
        }
        let mut ct = wrapped[off..off + ct_len].to_vec();

        if kid != self.kid {
            return Err(anyhow!("unknown KEK KID '{}'", kid));
        }

        let key = Key::<Aes256Gcm>::from_slice(&self.kek);
        let cipher = Aes256Gcm::new(key);
        let aad = build_aad(&kid);

        let tag_ga = GenericArray::from_slice(tag);
        cipher
            .decrypt_in_place_detached(Nonce::from_slice(nonce), &aad, &mut ct, tag_ga)
            .map_err(|e| anyhow!("kms unwrap aes-gcm: {}", e))?;

        Ok((kid, ct))
    }
}

impl Drop for EnvKmsProvider {
    fn drop(&mut self) {
        self.kek.zeroize();
        self.kid.zeroize();
    }
}

// ---------------- helpers ----------------

fn build_aad(kid: &str) -> Vec<u8> {
    let mut aad = Vec::with_capacity(AAD_PREFIX.len() + kid.len());
    aad.extend_from_slice(AAD_PREFIX);
    aad.extend_from_slice(kid.as_bytes());
    aad
}

fn slice32(bytes: &[u8]) -> Result<[u8; 32]> {
    if bytes.len() != 32 {
        return Err(anyhow!("key must be exactly 32 bytes, got {}", bytes.len()));
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(bytes);
    Ok(out)
}

fn decode_hex_trimmed(s: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return Err(anyhow!("hex key must have even length"));
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for i in (0..bytes.len()).step_by(2) {
        let h = (bytes[i] as char)
            .to_digit(16)
            .ok_or_else(|| anyhow!("invalid hex at pos {}", i))?;
        let l = (bytes[i + 1] as char)
            .to_digit(16)
            .ok_or_else(|| anyhow!("invalid hex at pos {}", i + 1))?;
        out.push(((h << 4) | l) as u8);
    }
    Ok(out)
}

fn decode_base64_trimmed(s: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    let bytes = base64::engine::general_purpose::STANDARD
        .decode(s.as_bytes())
        .map_err(|e| anyhow!("base64 decode: {}", e))?;
    Ok(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn kms_env_roundtrip() {
        // Установим тестовый KEK/КID
        std::env::set_var("P1_KMS_KEK_KID", "dev-kek");
        std::env::set_var("P1_KMS_KEK_HEX", "22".repeat(32)); // 32 байта 0x22

        let kms = EnvKmsProvider::from_env().expect("kms from env");
        assert_eq!(kms.default_kek_kid(), "dev-kek");

        let dek = b"super-secret-dek-32bytes-please........";
        let wrapped = kms.wrap("dev-kek", dek).expect("wrap");
        let (_kid, dek_out) = kms.unwrap(&wrapped).expect("unwrap");
        assert_eq!(dek_out, dek);
    }
}