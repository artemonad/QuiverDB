//! C ABI (FFI) для QuiverDB.
//! Цель: универсальная интеграция из любых языков, умеющих вызывать C-функции.
//!
//! Экспортируем базовый минимум:
//! - qdb_init_db(path, page_size, buckets)
//! - qdb_open / qdb_open_ro / qdb_close
//! - qdb_put / qdb_get / qdb_del
//! - qdb_free_buf (для освобождения буфера, возвращённого qdb_get)
//! - qdb_last_error_dup / qdb_free_string (для текста последней ошибки)
//!
//! Возвраты:
//! - Функции, возвращающие int: 0 = OK, >0 = семантический код (например, 1 = not found),
//!   -1 = ошибка (подробности через qdb_last_error_dup()).
//! - Функции, возвращающие указатели: NULL при ошибке (см. qdb_last_error_dup()).
//!
//! Важно: этот модуль будет доступен при сборке с фичей "ffi" и типом библиотеки cdylib.
//! Следующим шагом добавим соответствующие изменения в Cargo.toml и lib.rs.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uchar};
use std::path::PathBuf;
use std::ptr;

use anyhow::{Context, Result};

use crate::{Db, Directory, init_db};

struct Handle {
    db: Db,
    readonly: bool,
}

fn cstr_to_path(c: *const c_char) -> Result<PathBuf> {
    if c.is_null() {
        anyhow::bail!("null path");
    }
    let s = unsafe { CStr::from_ptr(c) }.to_str().context("path utf-8")?;
    Ok(PathBuf::from(s))
}

// -------- last_error (TLS) --------

use std::cell::RefCell;

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = RefCell::new(None);
}

fn set_last_error(e: anyhow::Error) {
    let s = e.to_string();
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(CString::new(s).unwrap_or_else(|_| CString::new("error").unwrap()));
    });
}

/// Возвращает дубликат строки последней ошибки (или NULL, если её нет).
/// Освобождать через qdb_free_string().
#[no_mangle]
pub extern "C" fn qdb_last_error_dup() -> *mut c_char {
    LAST_ERROR.with(|slot| {
        if let Some(s) = slot.borrow().as_ref() {
            // дублируем (new) и отдаём владение вызывающему
            unsafe { libc_strdup(s.as_c_str().as_ptr()) }
        } else {
            ptr::null_mut()
        }
    })
}

/// Освободить строку, возвращённую qdb_last_error_dup().
#[no_mangle]
pub extern "C" fn qdb_free_string(p: *mut c_char) {
    if !p.is_null() {
        unsafe {
            let _ = CString::from_raw(p);
        }
    }
}

// libc strdup совместимая реализация (Windows safe)
unsafe fn libc_strdup(s: *const c_char) -> *mut c_char {
    if s.is_null() {
        return ptr::null_mut();
    }
    let len = libc_strlen(s);
    let size = len + 1;
    let buf = libc_malloc(size);
    if buf.is_null() {
        return ptr::null_mut();
    }
    ptr::copy_nonoverlapping(s as *const u8, buf as *mut u8, size);
    buf as *mut c_char
}

unsafe fn libc_strlen(s: *const c_char) -> usize {
    let mut p = s;
    let mut n = 0usize;
    while *p != 0 {
        p = p.add(1);
        n += 1;
    }
    n
}

unsafe fn libc_malloc(sz: usize) -> *mut u8 {
    let layout = std::alloc::Layout::from_size_align(sz, std::mem::align_of::<usize>()).unwrap();
    std::alloc::alloc(layout)
}

// -------- init/open/close --------

/// Инициализация новой БД: создаёт meta/seg1/WAL/free и (если нужно) каталог.
/// Возвращает 0 при успехе, -1 при ошибке (см. qdb_last_error_dup()).
#[no_mangle]
pub extern "C" fn qdb_init_db(
    path: *const c_char,
    page_size: u32,
    buckets: u32,
) -> c_int {
    let res = (|| -> Result<()> {
        let root = cstr_to_path(path)?;
        if !root.exists() || !root.join("meta").exists() {
            init_db(&root, page_size)?;
        }
        if !root.join("dir").exists() {
            Directory::create(&root, buckets)?;
        }
        Ok(())
    })();

    match res {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e);
            -1
        }
    }
}

/// Открыть writer (эксклюзивный). NULL при ошибке.
#[no_mangle]
pub extern "C" fn qdb_open(path: *const c_char) -> *mut Handle {
    let res = (|| -> Result<Handle> {
        let root = cstr_to_path(path)?;
        let db = Db::open(&root)?;
        Ok(Handle { db, readonly: false })
    })();

    match res {
        Ok(h) => Box::into_raw(Box::new(h)),
        Err(e) => {
            set_last_error(e);
            ptr::null_mut()
        }
    }
}

/// Открыть reader (shared). NULL при ошибке.
#[no_mangle]
pub extern "C" fn qdb_open_ro(path: *const c_char) -> *mut Handle {
    let res = (|| -> Result<Handle> {
        let root = cstr_to_path(path)?;
        let db = Db::open_ro(&root)?;
        Ok(Handle { db, readonly: true })
    })();

    match res {
        Ok(h) => Box::into_raw(Box::new(h)),
        Err(e) => {
            set_last_error(e);
            ptr::null_mut()
        }
    }
}

/// Закрыть handle.
#[no_mangle]
pub extern "C" fn qdb_close(h: *mut Handle) {
    if !h.is_null() {
        unsafe {
            drop(Box::from_raw(h));
        }
    }
}

// -------- put/get/del --------

/// Вставка/обновление (writer). 0 = OK, -1 = ошибка, 1 = readonly.
#[no_mangle]
pub extern "C" fn qdb_put(
    h: *mut Handle,
    key_ptr: *const c_uchar,
    key_len: usize,
    val_ptr: *const c_uchar,
    val_len: usize,
) -> c_int {
    if h.is_null() {
        return -1;
    }
    let handle = unsafe { &mut *h };
    if handle.readonly {
        return 1;
    }
    let res = (|| -> Result<()> {
        if key_ptr.is_null() {
            anyhow::bail!("null key");
        }
        if val_ptr.is_null() {
            anyhow::bail!("null value");
        }
        let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
        let val = unsafe { std::slice::from_raw_parts(val_ptr, val_len) };
        handle.db.put(key, val)?;
        Ok(())
    })();
    match res {
        Ok(()) => 0,
        Err(e) => {
            set_last_error(e);
            -1
        }
    }
}

/// Получение. Возвращает:
///  0 => found (out_ptr/out_len заполнены),
///  1 => not found,
/// -1 => ошибка.
/// Буфер нужно освобождать qdb_free_buf().
#[no_mangle]
pub extern "C" fn qdb_get(
    h: *mut Handle,
    key_ptr: *const c_uchar,
    key_len: usize,
    out_ptr: *mut *mut c_uchar,
    out_len: *mut usize,
) -> c_int {
    if h.is_null() {
        return -1;
    }
    if key_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
        return -1;
    }
    let handle = unsafe { &mut *h };
    let res = (|| -> Result<c_int> {
        let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
        match handle.db.get(key)? {
            Some(v) => {
                let len = v.len();
                let mut boxed = v.into_boxed_slice();
                let ptr_u8 = boxed.as_mut_ptr();
                std::mem::forget(boxed); // отдаём владение caller'у
                unsafe {
                    *out_ptr = ptr_u8;
                    *out_len = len;
                }
                Ok(0)
            }
            None => Ok(1),
        }
    })();

    match res {
        Ok(code) => code,
        Err(e) => {
            set_last_error(e);
            -1
        }
    }
}

/// Удаление. Возвращает:
///  0 => deleted,
///  1 => not found,
/// -1 => ошибка.
#[no_mangle]
pub extern "C" fn qdb_del(
    h: *mut Handle,
    key_ptr: *const c_uchar,
    key_len: usize,
) -> c_int {
    if h.is_null() {
        return -1;
    }
    if key_ptr.is_null() {
        return -1;
    }
    let handle = unsafe { &mut *h };
    if handle.readonly {
        return 1; // трактуем как "не удалось удалить"
    }
    let res = (|| -> Result<c_int> {
        let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
        let ok = handle.db.del(key)?;
        Ok(if ok { 0 } else { 1 })
    })();

    match res {
        Ok(code) => code,
        Err(e) => {
            set_last_error(e);
            -1
        }
    }
}

/// Освобождение буфера, возвращённого qdb_get().
#[no_mangle]
pub extern "C" fn qdb_free_buf(ptr_: *mut c_uchar, len: usize) {
    if ptr_.is_null() || len == 0 {
        return;
    }
    unsafe {
        let _ = Vec::from_raw_parts(ptr_, len, len);
    }
}