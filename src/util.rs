use anyhow::{Context, Result};
use byteorder::{ByteOrder, LittleEndian};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::consts::PAGE_MAGIC;
use crate::page_ovf::ovf_header_read;
use crate::page_rh::rh_header_read;

pub fn read_at(f: &mut File, offset: u64, buf: &mut [u8]) -> Result<()> {
    f.seek(SeekFrom::Start(offset))?;
    f.read_exact(buf)?;
    Ok(())
}

pub fn write_at(f: &mut File, offset: u64, buf: &[u8]) -> Result<()> {
    f.seek(SeekFrom::Start(offset))?;
    f.write_all(buf)?;
    Ok(())
}

pub fn hex_dump(bytes: &[u8]) -> String {
    let mut out = String::new();
    for (i, b) in bytes.iter().enumerate() {
        if i > 0 {
            if i % 16 == 0 {
                out.push('\n');
            } else {
                out.push(' ');
            }
        }
        out.push_str(&format!("{:02x}", b));
    }
    out
}

pub fn display_text(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => format!("(binary {} B)", bytes.len()),
    }
}

pub fn parse_u8_byte(s: &str) -> Result<u8, String> {
    let s = s.trim();
    if let Some(x) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        u8::from_str_radix(x, 16).map_err(|e| e.to_string())
    } else if let Some(x) = s.strip_prefix("0o").or_else(|| s.strip_prefix("0O")) {
        u8::from_str_radix(x, 8).map_err(|e| e.to_string())
    } else if let Some(x) = s.strip_prefix("0b").or_else(|| s.strip_prefix("0B")) {
        u8::from_str_radix(x, 2).map_err(|e| e.to_string())
    } else {
        s.parse::<u8>().map_err(|e| e.to_string())
    }
}

pub fn create_empty_file(path: &Path) -> Result<()> {
    let f = std::fs::OpenOptions::new()
        .create_new(true)
        .write(true)
        .open(path)
        .with_context(|| format!("create file {}", path.display()))?;
    f.sync_all()?;
    Ok(())
}

/// Extract LSN from a v2 page (KV_RH or OVERFLOW).
/// Returns:
/// - Some(lsn) if the buffer looks like a v2 page and header parsed,
/// - None for non-v2/unknown buffers.
///
/// Notes:
/// - This helper is shared by replay/apply paths to avoid duplication.
/// - CRC is NOT verified here; callers should verify CRC if needed.
pub fn v2_page_lsn(buf: &[u8]) -> Option<u64> {
    if buf.len() < 8 {
        return None;
    }
    if &buf[..4] != PAGE_MAGIC {
        return None;
    }
    let ver = LittleEndian::read_u16(&buf[4..6]);
    if ver < 2 {
        return None;
    }
    if let Ok(h) = rh_header_read(buf) {
        Some(h.lsn)
    } else if let Ok(h) = ovf_header_read(buf) {
        Some(h.lsn)
    } else {
        None
    }
}