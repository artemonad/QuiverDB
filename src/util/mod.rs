//! util — общие утилиты (вынесено из разных модулей).
//!
//! Содержит:
//! - now_secs(): текущее Unix-время в секундах (u32, saturating).
//! - decode_ovf_placeholder_v3(): разбор TLV плейсхолдера OVERFLOW3 (v3).
//!
//! Задача: убрать дублирование простых хелперов по коду и централизовать поведение.

use byteorder::{ByteOrder, LittleEndian};

/// Текущее Unix-время в секундах, обрезанное к u32 (saturating).
#[inline]
pub fn now_secs() -> u32 {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    (now.as_secs() as u64).min(u32::MAX as u64) as u32
}

/// Разобрать OVERFLOW3 TLV-плейсхолдер v3: [tag=0x01 u8][len=16 u8][total_len u64][head_pid u64].
///
/// Возвращает Some((total_len, head_pid)) при успехе, иначе None.
#[inline]
pub fn decode_ovf_placeholder_v3(v: &[u8]) -> Option<(u64, u64)> {
    if v.len() >= 18 && v[0] == 0x01 && v[1] == 16 {
        let total = LittleEndian::read_u64(&v[2..10]);
        let head = LittleEndian::read_u64(&v[10..18]);
        Some((total, head))
    } else {
        None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use byteorder::{ByteOrder, LittleEndian};

    #[test]
    fn now_secs_monotonic_nonzero() {
        let a = now_secs();
        let b = now_secs();
        assert!(b >= a);
    }

    #[test]
    fn decode_ovf_placeholder_ok() {
        let mut buf = vec![0u8; 18];
        buf[0] = 0x01;
        buf[1] = 16;
        LittleEndian::write_u64(&mut buf[2..10], 1234u64);
        LittleEndian::write_u64(&mut buf[10..18], 0xAABBCCDDEEFF0011u64);
        let got = decode_ovf_placeholder_v3(&buf).expect("must decode");
        assert_eq!(got.0, 1234);
        assert_eq!(got.1, 0xAABBCCDDEEFF0011);
    }

    #[test]
    fn decode_ovf_placeholder_bad() {
        assert!(decode_ovf_placeholder_v3(&[]).is_none());
        let mut buf = vec![0u8; 18];
        buf[0] = 0x02; // wrong tag
        buf[1] = 16;
        assert!(decode_ovf_placeholder_v3(&buf).is_none());
    }
}
