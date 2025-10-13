//! wal/net — защищённый CDC транспорт (фрейминг + HMAC‑PSK) и TLS‑клиент.
//!
//! Назначение:
//! - PSK‑фрейминг: HMAC‑SHA256 поверх payload + seq для целостности/аутентичности.
//! - TLS/mTLS клиент (native-tls): защищённый транспорт поверх TCP, совместимый с PSK‑фреймингом.
//!
//! Формат фрейма (LE):
//!   header(44) = [len u32][seq u64][mac[32]]; payload[len]
//!
//! ENV (PSK):
//!   P1_CDC_PSK_HEX / P1_CDC_PSK_BASE64 / P1_CDC_PSK
//!   — добавлено: минимальная длина PSK = 16 байт
//!
//! ENV (TLS):
//!   P1_TLS_DOMAIN               — переопределить SNI/hostname (по умолчанию host из "host:port")
//!   P1_TLS_CA_FILE             — PEM‑файл c CA (поддерживаются блоки CERTIFICATE/TRUSTED CERTIFICATE)
//!   P1_TLS_CLIENT_PFX          — путь к PFX/PKCS#12 (mTLS; опц.)
//!   P1_TLS_CLIENT_PFX_PASSWORD — пароль к PFX (mTLS; опц.)
//!
//! Изменения:
//! - Убран доступ к приватным полям pem::Pem (tag/contents). Реализован свой
//!   parser PEM‑сертификатов (parse_pem_certs) → DER → native_tls::Certificate.
//! - Введена проверка минимальной длины PSK (>=16 байт).

use anyhow::{anyhow, Result};
use base64::Engine;
use byteorder::{ByteOrder, LittleEndian};
use hmac::{Hmac, Mac};
use sha2::Sha256;
use std::io::{Read, Write};
use std::net::TcpStream;

type HmacSha256 = Hmac<Sha256>;

const MAGIC: &[u8] = b"P2PSK001";
const HDR_LEN: usize = 4 + 8 + 32; // len u32 + seq u64 + mac[32]

// -------------------- PSK key --------------------

pub fn load_psk_from_env() -> Result<Vec<u8>> {
    // HEX имеет приоритет, затем BASE64, затем raw
    if let Ok(hex) = std::env::var("P1_CDC_PSK_HEX") {
        let bytes = decode_hex(hex.trim())?;
        validate_psk_len(&bytes)?;
        return Ok(bytes);
    }
    if let Ok(b64) = std::env::var("P1_CDC_PSK_BASE64") {
        let bytes = base64::engine::general_purpose::STANDARD
            .decode(b64.trim().as_bytes())
            .map_err(|e| anyhow!("P1_CDC_PSK_BASE64 decode: {}", e))?;
        validate_psk_len(&bytes)?;
        return Ok(bytes);
    }
    if let Ok(raw) = std::env::var("P1_CDC_PSK") {
        let bytes = raw.into_bytes();
        validate_psk_len(&bytes)?;
        return Ok(bytes);
    }
    Err(anyhow!(
        "CDC PSK not set: provide P1_CDC_PSK_HEX or P1_CDC_PSK_BASE64 or P1_CDC_PSK"
    ))
}

#[inline]
fn validate_psk_len(psk: &[u8]) -> Result<()> {
    if psk.len() < 16 {
        return Err(anyhow!(
            "CDC PSK is too short ({} bytes): minimal length is 16 bytes",
            psk.len()
        ));
    }
    Ok(())
}

// -------------------- PSK framing --------------------

pub fn write_framed_psk<W: Write>(w: &mut W, seq: u64, payload: &[u8], psk: &[u8]) -> Result<()> {
    if payload.len() > u32::MAX as usize {
        return Err(anyhow!("payload too large: {}", payload.len()));
    }
    let mut hdr = vec![0u8; HDR_LEN];
    LittleEndian::write_u32(&mut hdr[0..4], payload.len() as u32);
    LittleEndian::write_u64(&mut hdr[4..12], seq);

    let mac = compute_mac(psk, seq, payload)?;
    hdr[12..12 + 32].copy_from_slice(&mac);

    w.write_all(&hdr)?;
    if !payload.is_empty() {
        w.write_all(payload)?;
    }
    Ok(())
}

pub fn read_next_framed_psk<R: Read>(
    r: &mut R,
    psk: &[u8],
    max_len: usize,
) -> Result<Option<(u64, Vec<u8>)>> {
    let mut hdr = [0u8; HDR_LEN];
    match read_exact_or_eof(r, &mut hdr) {
        Ok(true) => {}
        Ok(false) => return Ok(None),
        Err(e) => return Err(e),
    }

    let len = LittleEndian::read_u32(&hdr[0..4]) as usize;
    let seq = LittleEndian::read_u64(&hdr[4..12]);
    if len > max_len {
        return Err(anyhow!("framed payload too large: {} (max {})", len, max_len));
    }
    let mac_stored = &hdr[12..12 + 32];

    let mut payload = vec![0u8; len];
    if len > 0 {
        match read_exact_or_eof(r, &mut payload) {
            Ok(true) => {}
            Ok(false) => return Ok(None),
            Err(e) => return Err(e),
        }
    }

    let mac_calc = compute_mac(psk, seq, &payload)?;
    if !constant_time_eq(mac_stored, &mac_calc) {
        return Err(anyhow!("CDC PSK MAC verify failed (seq={})", seq));
    }

    Ok(Some((seq, payload)))
}

fn compute_mac(psk: &[u8], seq: u64, payload: &[u8]) -> Result<[u8; 32]> {
    let mut mac = HmacSha256::new_from_slice(psk).map_err(|e| anyhow!("psk: {}", e))?;
    mac.update(MAGIC);

    let mut seq_le = [0u8; 8];
    LittleEndian::write_u64(&mut seq_le, seq);
    mac.update(&seq_le);

    mac.update(payload);
    let tag = mac.finalize().into_bytes();
    let mut out = [0u8; 32];
    out.copy_from_slice(&tag[..]);
    Ok(out)
}

fn read_exact_or_eof<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<bool> {
    use std::io::ErrorKind;
    let mut off = 0usize;
    while off < buf.len() {
        match r.read(&mut buf[off..]) {
            Ok(0) => return Ok(false),
            Ok(n) => off += n,
            Err(e) if e.kind() == ErrorKind::Interrupted => continue,
            Err(e) => return Err(anyhow!("read error: {}", e)),
        }
    }
    Ok(true)
}

fn decode_hex(s: &str) -> Result<Vec<u8>> {
    let s = s.trim();
    if s.len() % 2 != 0 {
        return Err(anyhow!("hex string must have even length"));
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

// ----------------------------- TLS client (native-tls) -----------------------------

use native_tls::{TlsConnector, Certificate as NtCertificate, Identity as NtIdentity, TlsStream};

/// Поток ввода/вывода: TCP или TLS.
pub enum IoStream {
    Plain(TcpStream),
    Tls(TlsStream<TcpStream>),
}

impl Read for IoStream {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        match self {
            IoStream::Plain(s) => s.read(buf),
            IoStream::Tls(s) => s.read(buf),
        }
    }
}
impl Write for IoStream {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        match self {
            IoStream::Plain(s) => s.write(buf),
            IoStream::Tls(s) => s.write(buf),
        }
    }
    fn flush(&mut self) -> std::io::Result<()> {
        match self {
            IoStream::Plain(s) => s.flush(),
            IoStream::Tls(s) => s.flush(),
        }
    }
}

/// Открыть TLS‑поток (native-tls) для адреса "host:port".
/// - Если задан P1_TLS_CA_FILE — добавляем кастомные CA (PEM).
/// - Если заданы P1_TLS_CLIENT_PFX/P1_TLS_CLIENT_PFX_PASSWORD — включаем mTLS (PKCS#12).
/// - SNI берём из P1_TLS_DOMAIN или host из addr.
pub fn open_tls_psk_stream(addr: &str) -> Result<IoStream> {
    let domain = tls_domain_for_addr(addr)?;
    let tcp = TcpStream::connect(addr).map_err(|e| anyhow!("connect({}): {}", addr, e))?;

    let mut builder = TlsConnector::builder();

    // Кастомные CA (PEM). Поддерживаем "CERTIFICATE" и "TRUSTED CERTIFICATE".
    if let Ok(ca_path) = std::env::var("P1_TLS_CA_FILE") {
        let ca_pem = std::fs::read(&ca_path)
            .map_err(|e| anyhow!("read CA file {}: {}", ca_path, e))?;

        let certs_der = parse_pem_certs(&ca_pem)?;
        if certs_der.is_empty() {
            return Err(anyhow!(
                "no certificates found in CA file {} (expect BEGIN CERTIFICATE blocks)",
                ca_path
            ));
        }
        for der in certs_der {
            let cert = NtCertificate::from_der(&der)
                .map_err(|e| anyhow!("parse DER from {}: {}", ca_path, e))?;
            builder.add_root_certificate(cert);
        }
    }

    // mTLS через PKCS#12 (PFX)
    if let (Ok(pfx_path), Ok(pfx_pwd)) = (
        std::env::var("P1_TLS_CLIENT_PFX"),
        std::env::var("P1_TLS_CLIENT_PFX_PASSWORD"),
    ) {
        let pfx_der = std::fs::read(&pfx_path)
            .map_err(|e| anyhow!("read PFX {}: {}", pfx_path, e))?;
        let id = NtIdentity::from_pkcs12(&pfx_der, &pfx_pwd)
            .map_err(|e| anyhow!("load PFX {}: {}", pfx_path, e))?;
        builder.identity(id);
    }

    let connector = builder.build().map_err(|e| anyhow!("tls build: {}", e))?;
    let tls = connector.connect(&domain, tcp)
        .map_err(|e| anyhow!("tls connect (SNI={}): {}", domain, e))?;
    Ok(IoStream::Tls(tls))
}

/// Простейший разбор PEM-файла с извлечением всех сертификатов DER.
/// Поддерживаются блоки:
///   -----BEGIN CERTIFICATE----- ... -----END CERTIFICATE-----
///   -----BEGIN TRUSTED CERTIFICATE----- ... -----END TRUSTED CERTIFICATE-----
fn parse_pem_certs(pem_bytes: &[u8]) -> Result<Vec<Vec<u8>>> {
    let text = String::from_utf8_lossy(pem_bytes);
    let mut out: Vec<Vec<u8>> = Vec::new();

    fn extract_all(s: &str, begin_tag: &str, end_tag: &str, out: &mut Vec<Vec<u8>>) -> Result<()> {
        let mut search_from = 0usize;
        loop {
            let Some(beg) = s[search_from..].find(begin_tag) else { break; };
            let start = search_from + beg + begin_tag.len();
            let Some(end_rel) = s[start..].find(end_tag) else {
                return Err(anyhow!("PEM block '{}' has no matching END", begin_tag));
            };
            let end = start + end_rel;
            // Вырежем base64‑тело, удалим пробелы/CRLF и декодируем
            let body = s[start..end]
                .chars()
                .filter(|c| !c.is_whitespace())
                .collect::<String>();
            let der = base64::engine::general_purpose::STANDARD
                .decode(body.as_bytes())
                .map_err(|e| anyhow!("PEM base64 decode: {}", e))?;
            out.push(der);
            search_from = end + end_tag.len();
        }
        Ok(())
    }

    extract_all(
        &text,
        "-----BEGIN CERTIFICATE-----",
        "-----END CERTIFICATE-----",
        &mut out,
    )?;
    extract_all(
        &text,
        "-----BEGIN TRUSTED CERTIFICATE-----",
        "-----END TRUSTED CERTIFICATE-----",
        &mut out,
    )?;

    Ok(out)
}

/// Парсер host/SNI из "host:port" и "[ipv6]:port".
fn tls_domain_for_addr(addr: &str) -> Result<String> {
    if let Ok(sni) = std::env::var("P1_TLS_DOMAIN") {
        if !sni.trim().is_empty() {
            return Ok(sni.trim().to_string());
        }
    }
    if addr.starts_with('[') {
        if let Some(end) = addr.find(']') {
            return Ok(addr[1..end].to_string());
        }
        return Err(anyhow!("invalid IPv6 literal in addr: {}", addr));
    }
    if let Some(idx) = addr.rfind(':') {
        return Ok(addr[..idx].to_string());
    }
    Ok(addr.to_string())
}