//! page/ovf/chain — helpers для чтения OVERFLOW3‑цепочек (v3) с учётом codec_id per page.
//!
//! Публичный API:
//! - read_overflow_chain(pager, head_pid, expected_len) -> Result<Vec<u8>>
//!   Читает цепочку, начиная с head_pid, декомпрессирует (codec_id=1=zstd) и возвращает точный буфер
//!   ожидаемой длины. Строго проверяет итоговую длину (ошибка при несовпадении).
//!
//! NEW (hardening): потоковая декомпрессия вместо decode_all
//! - Для codec_id=1 используется zstd streaming‑decoder (Read) с пошаговым чтением в небольшой буфер,
//!   чтобы не аллоцировать гигантские промежуточные вектора и не допускать DoS по памяти.
//! - Перед добавлением каждого распакованного куска проверяется, что out.len() + n ≤ expected_len.
//!
//! NEW: защита от чрезмерной аллокации/DoS:
//! - ENV P1_MAX_VALUE_BYTES (usize, по умолчанию 1 GiB) — глобальный лимит на итоговый размер значения.
//! - На каждом шаге декомпрессии проверяется невыход за expected_len (и, соответственно, за max_value_bytes).

use anyhow::{anyhow, Result};
use std::io::{Cursor, Read};
use std::sync::OnceLock;

use crate::pager::Pager;
use crate::dir::NO_PAGE;
use crate::page::{ovf_header_read_v3, OVF_HDR_MIN};

/// Максимально допустимый размер значения (байт), читаемого из OVERFLOW‑цепочки.
/// Настраивается через ENV P1_MAX_VALUE_BYTES. По умолчанию 1 GiB.
fn max_value_bytes() -> usize {
    static MAX: OnceLock<usize> = OnceLock::new();
    *MAX.get_or_init(|| {
        std::env::var("P1_MAX_VALUE_BYTES")
            .ok()
            .and_then(|s| s.trim().parse::<usize>().ok())
            .unwrap_or(1usize << 30) // 1 GiB
    })
}

/// Прочитать OVERFLOW3‑цепочку, учитывая codec_id каждой страницы.
/// Строго проверяет суммарную длину (expected_len).
pub fn read_overflow_chain(pager: &Pager, mut head: u64, expected_len: usize) -> Result<Vec<u8>> {
    // Глобальный guard на размер значения
    let max_bytes = max_value_bytes();
    if expected_len > max_bytes {
        return Err(anyhow!(
            "overflow chain expected_len {} exceeds max_value_bytes {} (set P1_MAX_VALUE_BYTES to override)",
            expected_len,
            max_bytes
        ));
    }

    let ps = pager.meta.page_size as usize;

    // Не резервируем гигантскую capacity заранее
    let mut out = Vec::with_capacity(std::cmp::min(expected_len, 8 * 1024 * 1024));
    let mut guard = 0usize;

    // Небольшой рабочий буфер для потокового чтения распакованных байт
    const TMP_BUF: usize = 64 * 1024;
    let mut tmp = vec![0u8; TMP_BUF];

    while head != NO_PAGE {
        guard += 1;
        if guard > 1_000_000 {
            return Err(anyhow!("overflow chain too long or loop detected"));
        }

        let mut page = vec![0u8; ps];
        pager.read_page(head, &mut page)?;
        let h = ovf_header_read_v3(&page)?;
        let take = h.chunk_len as usize;

        if OVF_HDR_MIN + take > ps {
            return Err(anyhow!("overflow chunk too large for page (pid={})", head));
        }
        let chunk = &page[OVF_HDR_MIN..OVF_HDR_MIN + take];

        match h.codec_id {
            0 => {
                // Без сжатия — нельзя выйти за предел expected_len
                let remaining = expected_len.saturating_sub(out.len());
                if take > remaining {
                    return Err(anyhow!(
                        "overflow plain chunk oversize (pid={}, chunk={}, remaining={})",
                        head,
                        take,
                        remaining
                    ));
                }
                out.extend_from_slice(chunk);
                // remaining пересчитывается на каждой итерации; дополнительное вычитание не требуется.
            }
            1 => {
                // zstd — потоковая декомпрессия с ограничением по expected_len
                let cursor = Cursor::new(chunk);
                let mut decoder = zstd::stream::read::Decoder::new(cursor)
                    .map_err(|e| anyhow!("zstd decoder init (pid={}): {}", head, e))?;

                loop {
                    // Если уже набрали всё ожидаемое — это допустимо только на последней странице
                    let remaining = expected_len.saturating_sub(out.len());
                    if remaining == 0 {
                        if h.next_page_id != NO_PAGE {
                            // Раньше времени закончили — цепочка длиннее, чем ожидали
                            return Err(anyhow!(
                                "overflow decoded data reached expected len before last page (pid={})",
                                head
                            ));
                        }
                        break;
                    }

                    let to_read = std::cmp::min(tmp.len(), remaining);
                    let n = decoder.read(&mut tmp[..to_read]).map_err(|e| {
                        anyhow!("zstd decode read (pid={}): {}", head, e)
                    })?;

                    if n == 0 {
                        // Конец распаковки чанка
                        break;
                    }

                    out.extend_from_slice(&tmp[..n]);

                    // Проверим инвариант: не выйти за expected_len (и max_value_bytes)
                    if out.len() > expected_len {
                        return Err(anyhow!(
                            "overflow decoded data exceeds expected length (pid={}, out={}, expected={})",
                            head,
                            out.len(),
                            expected_len
                        ));
                    }
                    if out.len() > max_bytes {
                        return Err(anyhow!(
                            "overflow decoded data exceeds max_value_bytes {} (pid={})",
                            max_bytes,
                            head
                        ));
                    }
                }
            }
            other => {
                return Err(anyhow!("unsupported overflow codec_id={} at pid={}", other, head));
            }
        }

        head = h.next_page_id;
    }

    if out.len() != expected_len {
        return Err(anyhow!(
            "overflow length mismatch: got {}, expected {}",
            out.len(),
            expected_len
        ));
    }
    Ok(out)
}