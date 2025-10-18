//! crypto — подготовительный модуль для TDE (per-page AES-GCM).
//!
//! Цели:
//! - Единый KeyProvider (KID -> 32-байтный ключ).
//! - Нормализованный nonce для AES-GCM: derive_gcm_nonce(page_id, lsn).
//!
//! Примечания:
//! - Здесь нет шифрования/дешифрования страниц, только API и утилиты.
//! - EnvKeyProvider читает ключ из ENV (HEX или BASE64).
//! - StaticKeyProvider удобен для тестов.
//!
//! NEW:
//! - key_journal — журнал KID-эпох (TDE key rotation).
//! - безопасное обнуление ключей (Zeroize) в Drop провайдеров и KeyMaterial.
//! - KMS skeleton для envelope-обёртки DEK (см. модуль kms).
//! - KeyRing — стор для обёрнутых DEK (kms-оболочки) по KID.
//!
//! Использование:
//!   let kid = kp.default_kid().to_string();
//!   let key = kp.key(&kid)?.key; // [u8; 32]
//!   let nonce = derive_gcm_nonce(page_id, lsn); // [u8; 12]

use anyhow::{anyhow, Result};
use base64::Engine;
use std::sync::OnceLock;
use zeroize::Zeroize;

// Журнал KID-эпох (TDE key rotation)
mod key_journal;
pub use key_journal::KeyJournal;

// KMS skeleton (envelope DEK)
pub mod kms;
pub use kms::{EnvKmsProvider, KmsProvider};

// KeyRing — стор для обёрнутых DEK
pub mod keyring;
pub use keyring::KeyRing;

/// 32-байтный материал ключа + его KID (идентификатор).
#[derive(Clone, Debug)]
pub struct KeyMaterial {
    pub kid: String,
    pub key: [u8; 32],
}

// Безопасное обнуление: при уничтожении структуры стираем секреты из памяти.
impl Drop for KeyMaterial {
    fn drop(&mut self) {
        self.key.zeroize();
        self.kid.zeroize();
    }
}

/// Источник ключей для TDE. Thread-safe.
pub trait KeyProvider: Send + Sync {
    /// Вернуть KeyMaterial для указанного KID.
    fn key(&self, kid: &str) -> Result<KeyMaterial>;
    /// Возвратить KID по умолчанию (например, "default").
    fn default_kid(&self) -> &str;
}

/// Простой in-memory провайдер на один ключ (удобен для тестов).
#[derive(Clone, Debug)]
pub struct StaticKeyProvider {
    kid: String,
    key: [u8; 32],
}

impl StaticKeyProvider {
    pub fn new<S: Into<String>>(kid: S, key: [u8; 32]) -> Self {
        Self {
            kid: kid.into(),
            key,
        }
    }
}

impl KeyProvider for StaticKeyProvider {
    fn key(&self, kid: &str) -> Result<KeyMaterial> {
        if kid == self.kid {
            Ok(KeyMaterial {
                kid: self.kid.clone(),
                key: self.key,
            })
        } else {
            Err(anyhow!("unknown KID '{}'", kid))
        }
    }
    fn default_kid(&self) -> &str {
        &self.kid
    }
}

impl Drop for StaticKeyProvider {
    fn drop(&mut self) {
        self.key.zeroize();
        self.kid.zeroize();
    }
}

/// Провайдер из переменных окружения:
/// - P1_TDE_KEY_HEX     — ключ 32 байта в hex.
/// - P1_TDE_KEY_BASE64  — альтернативно, ключ в base64.
/// - P1_TDE_KID         — KID (по умолчанию "default").
#[derive(Clone, Debug)]
pub struct EnvKeyProvider {
    kid: String,
    key: [u8; 32],
}

impl EnvKeyProvider {
    pub fn from_env() -> Result<Self> {
        let kid = std::env::var("P1_TDE_KID").unwrap_or_else(|_| "default".to_string());
        if let Ok(hex) = std::env::var("P1_TDE_KEY_HEX") {
            let key_vec = decode_hex_trimmed(&hex)?;
            let key = slice32(&key_vec)?;
            return Ok(Self { kid, key });
        }
        if let Ok(b64) = std::env::var("P1_TDE_KEY_BASE64") {
            let key_vec = decode_base64_trimmed(&b64)?;
            let key = slice32(&key_vec)?;
            return Ok(Self { kid, key });
        }
        Err(anyhow!(
            "EnvKeyProvider: set P1_TDE_KEY_HEX or P1_TDE_KEY_BASE64"
        ))
    }
}

impl KeyProvider for EnvKeyProvider {
    fn key(&self, kid: &str) -> Result<KeyMaterial> {
        if kid == self.kid {
            Ok(KeyMaterial {
                kid: self.kid.clone(),
                key: self.key,
            })
        } else {
            Err(anyhow!("unknown KID '{}'", kid))
        }
    }
    fn default_kid(&self) -> &str {
        &self.kid
    }
}

impl Drop for EnvKeyProvider {
    fn drop(&mut self) {
        self.key.zeroize();
        self.kid.zeroize();
    }
}

/// Порог предупреждения для LSN: 2^48 - 2^20.
const LSN_WARN_THRESHOLD: u64 = (1u64 << 48) - (1u64 << 20);
static LSN_WARN_ONCE: OnceLock<()> = OnceLock::new();

#[inline]
fn warn_lsn_wrap_if_needed(lsn: u64) {
    if lsn >= LSN_WARN_THRESHOLD {
        if LSN_WARN_ONCE.set(()).is_ok() {
            eprintln!(
                "[WARN] TDE nonce uses low 48 bits of LSN; current LSN={} is close to 2^48.",
                lsn
            );
        }
    }
}

/// Нормализованный nonce (12 байт) для AES-GCM по (page_id, lsn).
#[inline]
pub fn derive_gcm_nonce(page_id: u64, lsn: u64) -> [u8; 12] {
    warn_lsn_wrap_if_needed(lsn);
    let mut n = [0u8; 12];
    let pid_le = page_id.to_le_bytes();
    let lsn_le = lsn.to_le_bytes();
    n[0..6].copy_from_slice(&pid_le[0..6]); // low 48 bits of page_id
    n[6..12].copy_from_slice(&lsn_le[0..6]); // low 48 bits of lsn
    n
}

// ---------------------- helpers ----------------------

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
