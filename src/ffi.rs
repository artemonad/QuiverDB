// src/ffi.rs
#![cfg(feature = "ffi")]

//! C FFI для QuiverDB (минимальный стабильный ABI для Go/Python).
//!
//! Модель:
//! - Opaque-хэндл QdbDb управляет ресурсами Db (writer/reader).
//! - Ошибки возвращаются через int (0=OK, -1=ERR) и out_err (char**).
//! - Значения (get) возвращаются через QdbBuf {ptr,len} с явным освобождением qdb_buf_free().
//!
//! Безопасность/правила:
//! - Все указатели проверяются на NULL; out-указатели должны быть валидны.
//! - Строки (path) — нуль-терминированные C-строки (UTF-8 если возможно).
//! - Память под out-строки/буферы выделяется в Rust и освобождается qdb_string_free/qdb_buf_free.
//!
//! Компиляция:
//!   cargo build --release --features ffi
//! (crate-type cdylib/staticlib уже включены в Cargo.toml)
//!
//! Генерация заголовка C (cbindgen):
//!   cbindgen --crate QuiverDB --output quiverdb.h

use std::ffi::{CStr, CString};
use std::ptr;
use std::slice;

use libc::{c_char, c_int, c_uchar, c_uint, size_t};

use anyhow::Result;
use crate::Db; // re-export из lib.rs

// ---------- Opaque handle ----------

#[repr(C)]
pub struct QdbDb {
    inner: *mut Db,
}

impl QdbDb {
    fn from_box(b: Box<Db>) -> *mut QdbDb {
        let raw_db = Box::into_raw(b);
        let h = QdbDb { inner: raw_db };
        Box::into_raw(Box::new(h))
    }
    unsafe fn as_mut_db<'a>(&self) -> Option<&'a mut Db> {
        (self.inner as *mut Db).as_mut()
    }
}

// ---------- Common structs for FFI ----------

#[repr(C)]
pub struct QdbBuf {
    pub ptr: *mut c_uchar,
    pub len: size_t,
}

// ---------- Helpers ----------

unsafe fn cstr_to_path(c: *const c_char) -> Result<std::path::PathBuf, String> {
    if c.is_null() {
        return Err("null path".into());
    }
    let s = CStr::from_ptr(c).to_str().map_err(|_| "path is not valid UTF-8")?;
    Ok(std::path::PathBuf::from(s))
}

unsafe fn bytes_from<'a>(ptr: *const c_uchar, len: size_t) -> Result<&'a [u8], String> {
    if len == 0 {
        return Ok(&[]);
    }
    if ptr.is_null() {
        return Err("null pointer for non-empty slice".into());
    }
    Ok(slice::from_raw_parts(ptr, len as usize))
}

unsafe fn set_err(out_err: *mut *mut c_char, msg: &str) {
    if out_err.is_null() {
        return;
    }
    // освободим прежнее значение (если было) — доброжелательно
    if !(*out_err).is_null() {
        let _ = CString::from_raw(*out_err);
    }
    let c = CString::new(msg).unwrap_or_else(|_| CString::new("error").unwrap());
    *out_err = c.into_raw();
}

#[inline]
fn ret_ok() -> c_int { 0 }
#[inline]
fn ret_err() -> c_int { -1 }

// ---------- API ----------

#[no_mangle]
pub unsafe extern "C" fn qdb_init(
    path: *const c_char,
    page_size: c_uint,
    buckets: c_uint,
    out_err: *mut *mut c_char,
) -> c_int {
    match cstr_to_path(path) {
        Ok(p) => {
            if let Err(e) = crate::Db::init(&p, page_size, buckets) {
                set_err(out_err, &format!("{:#}", e));
                return ret_err();
            }
            ret_ok()
        }
        Err(e) => { set_err(out_err, &e); ret_err() }
    }
}

#[no_mangle]
pub unsafe extern "C" fn qdb_open_writer(
    path: *const c_char,
    out_db: *mut *mut QdbDb,
    out_err: *mut *mut c_char,
) -> c_int {
    if out_db.is_null() {
        set_err(out_err, "out_db is null");
        return ret_err();
    }
    *out_db = ptr::null_mut();

    match cstr_to_path(path) {
        Ok(p) => match Db::open(&p) {
            Ok(db) => { *out_db = QdbDb::from_box(Box::new(db)); ret_ok() }
            Err(e) => { set_err(out_err, &format!("{:#}", e)); ret_err() }
        },
        Err(e) => { set_err(out_err, &e); ret_err() }
    }
}

#[no_mangle]
pub unsafe extern "C" fn qdb_open_reader(
    path: *const c_char,
    out_db: *mut *mut QdbDb,
    out_err: *mut *mut c_char,
) -> c_int {
    if out_db.is_null() {
        set_err(out_err, "out_db is null");
        return ret_err();
    }
    *out_db = ptr::null_mut();

    match cstr_to_path(path) {
        Ok(p) => match Db::open_ro(&p) {
            Ok(db) => { *out_db = QdbDb::from_box(Box::new(db)); ret_ok() }
            Err(e) => { set_err(out_err, &format!("{:#}", e)); ret_err() }
        },
        Err(e) => { set_err(out_err, &e); ret_err() }
    }
}

#[no_mangle]
pub unsafe extern "C" fn qdb_close(db: *mut QdbDb) {
    if db.is_null() {
        return;
    }
    let h: Box<QdbDb> = Box::from_raw(db);
    if !h.inner.is_null() {
        let _db: Box<Db> = Box::from_raw(h.inner);
        drop(_db);
    }
    // h drop
}

#[no_mangle]
pub unsafe extern "C" fn qdb_put(
    db: *mut QdbDb,
    key_ptr: *const c_uchar,
    key_len: size_t,
    val_ptr: *const c_uchar,
    val_len: size_t,
    out_err: *mut *mut c_char,
) -> c_int {
    if db.is_null() {
        set_err(out_err, "db is null");
        return ret_err();
    }
    let db_ref = (&*db).as_mut_db().ok_or_else(|| "invalid db handle".to_string());
    let (key, val) = match (bytes_from(key_ptr, key_len), bytes_from(val_ptr, val_len)) {
        (Ok(k), Ok(v)) => (k, v),
        (Err(e), _) => { set_err(out_err, &e); return ret_err(); }
        (_, Err(e)) => { set_err(out_err, &e); return ret_err(); }
    };
    match db_ref {
        Ok(d) => match d.put(key, val) {
            Ok(_) => ret_ok(),
            Err(e) => { set_err(out_err, &format!("{:#}", e)); ret_err() }
        },
        Err(e) => { set_err(out_err, &e); ret_err() }
    }
}

#[no_mangle]
pub unsafe extern "C" fn qdb_del(
    db: *mut QdbDb,
    key_ptr: *const c_uchar,
    key_len: size_t,
    out_existed: *mut c_int,
    out_err: *mut *mut c_char,
) -> c_int {
    if db.is_null() {
        set_err(out_err, "db is null");
        return ret_err();
    }
    if out_existed.is_null() {
        set_err(out_err, "out_existed is null");
        return ret_err();
    }
    *out_existed = 0;

    let db_ref = (&*db).as_mut_db().ok_or_else(|| "invalid db handle".to_string());
    let key = match bytes_from(key_ptr, key_len) {
        Ok(k) => k,
        Err(e) => { set_err(out_err, &e); return ret_err(); }
    };
    match db_ref {
        Ok(d) => match d.del(key) {
            Ok(ex) => { *out_existed = if ex { 1 } else { 0 }; ret_ok() }
            Err(e) => { set_err(out_err, &format!("{:#}", e)); ret_err() }
        },
        Err(e) => { set_err(out_err, &e); ret_err() }
    }
}

#[no_mangle]
pub unsafe extern "C" fn qdb_exists(
    db: *mut QdbDb,
    key_ptr: *const c_uchar,
    key_len: size_t,
    out_present: *mut c_int,
    out_err: *mut *mut c_char,
) -> c_int {
    if db.is_null() {
        set_err(out_err, "db is null");
        return ret_err();
    }
    if out_present.is_null() {
        set_err(out_err, "out_present is null");
        return ret_err();
    }
    *out_present = 0;

    let db_ref = (&*db).as_mut_db().ok_or_else(|| "invalid db handle".to_string());
    let key = match bytes_from(key_ptr, key_len) {
        Ok(k) => k,
        Err(e) => { set_err(out_err, &e); return ret_err(); }
    };
    match db_ref {
        Ok(d) => match d.exists(key) {
            Ok(p) => { *out_present = if p { 1 } else { 0 }; ret_ok() }
            Err(e) => { set_err(out_err, &format!("{:#}", e)); ret_err() }
        },
        Err(e) => { set_err(out_err, &e); ret_err() }
    }
}

#[no_mangle]
pub unsafe extern "C" fn qdb_get(
    db: *mut QdbDb,
    key_ptr: *const c_uchar,
    key_len: size_t,
    out_buf: *mut QdbBuf,
    out_err: *mut *mut c_char,
) -> c_int {
    if db.is_null() {
        set_err(out_err, "db is null");
        return ret_err();
    }
    if out_buf.is_null() {
        set_err(out_err, "out_buf is null");
        return ret_err();
    }
    (*out_buf).ptr = ptr::null_mut();
    (*out_buf).len = 0;

    let db_ref = (&*db).as_mut_db().ok_or_else(|| "invalid db handle".to_string());
    let key = match bytes_from(key_ptr, key_len) {
        Ok(k) => k,
        Err(e) => { set_err(out_err, &e); return ret_err(); }
    };

    match db_ref {
        Ok(d) => match d.get(key) {
            Ok(Some(v)) => {
                let n = v.len();
                if n == 0 {
                    (*out_buf).ptr = ptr::null_mut();
                    (*out_buf).len = 0;
                    return ret_ok();
                }
                let mem = libc::malloc(n);
                if mem.is_null() {
                    set_err(out_err, "malloc failed");
                    return ret_err();
                }
                ptr::copy_nonoverlapping(v.as_ptr(), mem as *mut u8, n);
                (*out_buf).ptr = mem as *mut u8;
                (*out_buf).len = n as size_t;
                ret_ok()
            }
            Ok(None) => {
                (*out_buf).ptr = ptr::null_mut();
                (*out_buf).len = 0;
                ret_ok()
            }
            Err(e) => { set_err(out_err, &format!("{:#}", e)); ret_err() }
        },
        Err(e) => { set_err(out_err, &e); ret_err() }
    }
}

// ---------- Free helpers for foreign code ----------

#[no_mangle]
pub unsafe extern "C" fn qdb_string_free(s: *mut c_char) {
    if !s.is_null() {
        let _ = CString::from_raw(s); // drop → free
    }
}

#[no_mangle]
pub unsafe extern "C" fn qdb_buf_free(buf: QdbBuf) {
    if !buf.ptr.is_null() && buf.len > 0 {
        libc::free(buf.ptr as *mut libc::c_void);
    }
}

// ---------- Misc ----------

#[no_mangle]
pub extern "C" fn qdb_version() -> *const c_char {
    // статическая NUL-терминированная строка
    static S: &str = concat!(env!("CARGO_PKG_VERSION"), "\0");
    S.as_ptr() as *const c_char
}