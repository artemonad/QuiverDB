use anyhow::{anyhow, Context, Result};
use std::fs::OpenOptions;
use std::io::Read;
use std::path::PathBuf;

pub fn decode_value_arg(arg: &str) -> Result<(Vec<u8>, &'static str)> {
    if arg == "-" {
        let mut buf = Vec::new();
        std::io::stdin().read_to_end(&mut buf)?;
        return Ok((buf, "stdin"));
    }
    if let Some(p) = arg.strip_prefix('@') {
        let path = PathBuf::from(p);
        let mut f = OpenOptions::new()
            .read(true)
            .open(&path)
            .map_err(|e| anyhow!("open value file {}: {}", path.display(), e))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)?;
        return Ok((buf, "file"));
    }
    if let Some(hx) = arg.strip_prefix("hex:") {
        let v = decode_hex(hx)?;
        return Ok((v, "hex"));
    }
    Ok((arg.as_bytes().to_vec(), "literal"))
}

pub fn decode_hex(s: &str) -> Result<Vec<u8>> {
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

pub fn display_text(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => format!("(binary {} B)", bytes.len()),
    }
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

pub fn to_hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        s.push_str(&format!("{:02x}", b));
    }
    s
}

pub fn read_all(p: &PathBuf) -> Result<Vec<u8>> {
    let mut f = OpenOptions::new()
        .read(true)
        .open(p)
        .with_context(|| format!("open {}", p.display()))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(buf)
}
