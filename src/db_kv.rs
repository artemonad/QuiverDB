use anyhow::{anyhow, Result};

use crate::consts::NO_PAGE;
use crate::db::Db;
use crate::page_ovf::{
    ovf_free_chain, ovf_make_placeholder, ovf_parse_placeholder, ovf_write_chain,
};
use crate::page_rh::{
    rh_compact_inplace, rh_header_read, rh_header_write, rh_kv_delete_inplace, rh_kv_insert,
    rh_kv_lookup, rh_page_is_kv, rh_should_compact,
};
use crate::subs::Event;

/// Вставка/обновление ключа в существующую цепочку бакета.
/// Поведение идентично старому Db::put_in_chain:
/// - попытка in-place; при нехватке места — компакт; при необходимости — добавление новой страницы в хвост;
/// - зачистка дублей ключа в последующих страницах и вырезание пустых страниц;
/// - освобождение старых overflow-цепочек при обновлении значения;
/// - публикация события подписок (put).
pub fn put_in_chain(
    db: &mut Db,
    bucket: u32,
    head: u64,
    key: &[u8],
    val: &[u8],
    need_overflow: bool,
) -> Result<()> {
    let ps = db.pager.meta.page_size as usize;

    // Создаём overflow один раз перед обходом и переиспользуем.
    let (new_value_bytes, new_ovf_head_opt) = if need_overflow {
        let ovf_head = ovf_write_chain(&mut db.pager, val)?;
        (
            ovf_make_placeholder(val.len() as u64, ovf_head).to_vec(),
            Some(ovf_head),
        )
    } else {
        (val.to_vec(), None)
    };

    let mut prev: u64 = NO_PAGE;
    let mut pid: u64 = head;

    macro_rules! cleanup_on_error {
        () => {
            if let Some(h) = new_ovf_head_opt {
                ovf_free_chain(&db.pager, h)?;
            }
        };
    }

    // Публикация put-события (инлайн, чтобы не зависеть от приватного метода)
    #[inline]
    fn publish_put(db: &Db, key: &[u8], val: &[u8]) {
        // В RO режиме сюда не попадём (put вызывается у writer)
        let ev = Event {
            key: key.to_vec(),
            value: Some(val.to_vec()),
            lsn: db.pager.meta.last_lsn,
        };
        db.subs.publish(&ev);
    }

    loop {
        let mut buf = vec![0u8; ps];
        if let Err(e) = db.pager.read_page(pid, &mut buf) {
            cleanup_on_error!();
            return Err(e);
        }
        if !rh_page_is_kv(&buf) {
            cleanup_on_error!();
            return Err(anyhow!("page {} is not KV-RH (v2) page", pid));
        }

        let existing_val = rh_kv_lookup(&buf, db.dir.hash_kind, key)?;
        let old_ovf_head = existing_val.and_then(|v| ovf_parse_placeholder(&v).map(|(_, h)| h));

        // Попытка in-place вставки/обновления
        if rh_kv_insert(&mut buf, db.dir.hash_kind, key, &new_value_bytes)? {
            db.pager.commit_page(pid, &mut buf)?;

            // Освободим старую overflow-цепочку (если была и отличается от новой)
            if let Some(old_h) = old_ovf_head {
                if let Some(new_h) = new_ovf_head_opt {
                    if new_h != old_h {
                        ovf_free_chain(&db.pager, old_h)?;
                    }
                } else {
                    ovf_free_chain(&db.pager, old_h)?;
                }
            }

            // Зачистка дублей ключа на последующих страницах цепочки и вырезание пустых
            let h_after = rh_header_read(&buf)?; // свежий next_page_id
            let mut prev2 = pid;
            let mut cur = h_after.next_page_id;
            while cur != NO_PAGE {
                let mut buf2 = vec![0u8; ps];
                db.pager.read_page(cur, &mut buf2)?;
                if !rh_page_is_kv(&buf2) {
                    return Err(anyhow!("page {} is not KV-RH (v2) page", cur));
                }

                if let Some(vdup) = rh_kv_lookup(&buf2, db.dir.hash_kind, key)? {
                    if let Some((_, head_pid)) = ovf_parse_placeholder(&vdup) {
                        ovf_free_chain(&db.pager, head_pid)?;
                    }
                    let existed = rh_kv_delete_inplace(&mut buf2, db.dir.hash_kind, key)?;
                    if existed {
                        db.pager.commit_page(cur, &mut buf2)?;
                    }
                }

                let h2 = rh_header_read(&buf2)?;
                let next2 = h2.next_page_id;
                if h2.used_slots == 0 {
                    // Вырезаем пустую страницу из цепочки
                    let mut pbuf = vec![0u8; ps];
                    db.pager.read_page(prev2, &mut pbuf)?;
                    let mut ph = rh_header_read(&pbuf)?;
                    ph.next_page_id = next2;
                    rh_header_write(&mut pbuf, &ph)?;
                    db.pager.commit_page(prev2, &mut pbuf)?;
                    db.pager.free_page(cur)?;
                    cur = next2;
                    continue;
                } else {
                    prev2 = cur;
                    cur = next2;
                }
            }

            publish_put(db, key, val);
            return Ok(());
        }

        // Компактификация и повтор
        if rh_should_compact(&buf)? {
            rh_compact_inplace(&mut buf, db.dir.hash_kind)?;
            if rh_kv_insert(&mut buf, db.dir.hash_kind, key, &new_value_bytes)? {
                db.pager.commit_page(pid, &mut buf)?;
                if let Some(old_h) = old_ovf_head {
                    if let Some(new_h) = new_ovf_head_opt {
                        if new_h != old_h {
                            ovf_free_chain(&db.pager, old_h)?;
                        }
                    } else {
                        ovf_free_chain(&db.pager, old_h)?;
                    }
                }

                // Зачистка дублей/пустых — как после in-place
                let h_after = rh_header_read(&buf)?; // next_page_id
                let mut prev2 = pid;
                let mut cur = h_after.next_page_id;
                while cur != NO_PAGE {
                    let mut buf2 = vec![0u8; ps];
                    db.pager.read_page(cur, &mut buf2)?;
                    if !rh_page_is_kv(&buf2) {
                        return Err(anyhow!("page {} is not KV-RH (v2) page", cur));
                    }

                    if let Some(vdup) = rh_kv_lookup(&buf2, db.dir.hash_kind, key)? {
                        if let Some((_, head_pid)) = ovf_parse_placeholder(&vdup) {
                            ovf_free_chain(&db.pager, head_pid)?;
                        }
                        let existed = rh_kv_delete_inplace(&mut buf2, db.dir.hash_kind, key)?;
                        if existed {
                            db.pager.commit_page(cur, &mut buf2)?;
                        }
                    }

                    let h2 = rh_header_read(&buf2)?;
                    let next2 = h2.next_page_id;
                    if h2.used_slots == 0 {
                        let mut pbuf = vec![0u8; ps];
                        db.pager.read_page(prev2, &mut pbuf)?;
                        let mut ph = rh_header_read(&pbuf)?;
                        ph.next_page_id = next2;
                        rh_header_write(&mut pbuf, &ph)?;
                        db.pager.commit_page(prev2, &mut pbuf)?;
                        db.pager.free_page(cur)?;
                        cur = next2;
                        continue;
                    } else {
                        prev2 = cur;
                        cur = next2;
                    }
                }

                publish_put(db, key, val);
                return Ok(());
            }
        }

        // Удаление пустых страниц из цепочки
        let h = rh_header_read(&buf)?;
        if h.used_slots == 0 {
            let next = h.next_page_id;
            if prev == NO_PAGE {
                db.dir.set_head(bucket, next)?;
            } else {
                let mut pbuf = vec![0u8; ps];
                db.pager.read_page(prev, &mut pbuf)?;
                let mut ph = rh_header_read(&pbuf)?;
                ph.next_page_id = next;
                rh_header_write(&mut pbuf, &ph)?;
                db.pager.commit_page(prev, &mut pbuf)?;
            }
            db.pager.free_page(pid)?;
            if next == NO_PAGE {
                break;
            }
            pid = next;
            continue;
        }

        // Корректно запомнить хвост
        if h.next_page_id == NO_PAGE {
            prev = pid; // pid — реальный хвост
            break;
        }
        prev = pid;
        pid = h.next_page_id;
    }

    // Не нашли места — создаём новую страницу и вставляем запись
    let new_pid = db.pager.allocate_one_page()?;
    let mut newb = vec![0u8; ps];
    crate::page_rh::rh_page_init(&mut newb, new_pid)?;

    if !rh_kv_insert(&mut newb, db.dir.hash_kind, key, &new_value_bytes)? {
        // Откат новой цепочки (если была)
        if let Some((_, ovf_head)) = ovf_parse_placeholder(&new_value_bytes) {
            ovf_free_chain(&db.pager, ovf_head)?;
        }
        return Err(anyhow!("empty v2 page cannot fit record"));
    }
    db.pager.commit_page(new_pid, &mut newb)?;

    // Пришивка к цепочке
    if prev == NO_PAGE {
        db.dir.set_head(bucket, new_pid)?;
    } else {
        let mut tailb = vec![0u8; ps];
        db.pager.read_page(prev, &mut tailb)?;
        let mut th = rh_header_read(&tailb)?;
        th.next_page_id = new_pid;
        rh_header_write(&mut tailb, &th)?;
        db.pager.commit_page(prev, &mut tailb)?;
    }

    // После вставки новой версии в хвост — удалим старые копии ключа на предыдущих страницах,
    // вырезая пустые страницы, чтобы не копить мусор.
    {
        let mut prev2 = NO_PAGE;
        let mut cur = db.dir.head(bucket)?;
        while cur != NO_PAGE && cur != new_pid {
            let mut buf2 = vec![0u8; ps];
            db.pager.read_page(cur, &mut buf2)?;
            if !rh_page_is_kv(&buf2) {
                return Err(anyhow!("page {} is not KV-RH (v2) page", cur));
            }

            if let Some(vdup) = rh_kv_lookup(&buf2, db.dir.hash_kind, key)? {
                if let Some((_, head_pid)) = ovf_parse_placeholder(&vdup) {
                    ovf_free_chain(&db.pager, head_pid)?;
                }
                let existed = rh_kv_delete_inplace(&mut buf2, db.dir.hash_kind, key)?;
                if existed {
                    db.pager.commit_page(cur, &mut buf2)?;
                }
            }

            let h2 = rh_header_read(&buf2)?;
            let next2 = h2.next_page_id;
            if h2.used_slots == 0 {
                if prev2 == NO_PAGE {
                    db.dir.set_head(bucket, next2)?;
                } else {
                    let mut pbuf = vec![0u8; ps];
                    db.pager.read_page(prev2, &mut pbuf)?;
                    let mut ph = rh_header_read(&pbuf)?;
                    ph.next_page_id = next2;
                    rh_header_write(&mut pbuf, &ph)?;
                    db.pager.commit_page(prev2, &mut pbuf)?;
                }
                db.pager.free_page(cur)?;
                cur = next2;
                continue;
            } else {
                prev2 = cur;
                cur = next2;
            }
        }
    }

    publish_put(db, key, val);
    Ok(())
}