//! C ABI (FFI) для QuiverDB.
//! Цель: универсальная интеграция из любых языков, умеющих вызывать C-функции.
//!
//! Базовый минимум (было):
//! - qdb_init_db(path, page_size, buckets)
//! - qdb_open / qdb_open_ro / qdb_close
//! - qdb_put / qdb_get / qdb_del
//! - qdb_free_buf, qdb_last_error_dup / qdb_free_string
//!
//! Новое (удобства интеграций):
//! - qdb_open_with_config(path, json) / qdb_open_ro_with_config(path, json)
//!   JSON-параметры частичные, перекрывают значения из env:
//!   {"wal_coalesce_ms":u64,"data_fsync":bool,"page_cache_pages":usize,"ovf_threshold_bytes":usize|null}
//! - Стриминговые сканы через колбэк:
//!   typedef void (*qdb_scan_cb)(const uint8_t*, size_t, const uint8_t*, size_t, void*);
//!   qdb_scan_all(h, cb, user), qdb_scan_prefix(h, prefix, len, cb, user)
//! - qdb_version_dup(): вернуть строку версии (освобождать qdb_free_string()).

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int, c_uchar, c_void};
use std::path::PathBuf;
use std::ptr;

use anyhow::{Context, Result};

use crate::{Db, Directory, init_db};
use crate::config::QuiverConfig;

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

/// Освободить строку, возвращённую qdb_last_error_dup() или qdb_version_dup().
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

// -------- util: CStr -> Path --------

fn cstr_to_path(c: *const c_char) -> Result<PathBuf> {
    if c.is_null() {
        anyhow::bail!("null path");
    }
    let s = unsafe { CStr::from_ptr(c) }.to_str().context("path utf-8")?;
    Ok(PathBuf::from(s))
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

// -------- NEW: open with JSON config --------

#[derive(serde::Deserialize, Default)]
struct FfiConfig {
    wal_coalesce_ms: Option<u64>,
    data_fsync: Option<bool>,
    page_cache_pages: Option<usize>,
    ovf_threshold_bytes: Option<usize>, // null → Some(None) не требуется, просто опционально перекрываем
}

fn cfg_from_json(json_ptr: *const c_char) -> Result<QuiverConfig> {
    // Начинаем с env, затем перекрываем тем, что пришло в JSON.
    let mut cfg = QuiverConfig::from_env();

    if json_ptr.is_null() {
        return Ok(cfg);
    }
    let s = unsafe { CStr::from_ptr(json_ptr) }
        .to_str()
        .context("config json utf-8")?;
    if s.trim().is_empty() {
        return Ok(cfg);
    }

    let parsed: FfiConfig = serde_json::from_str(s).context("parse config json")?;

    if let Some(ms) = parsed.wal_coalesce_ms { cfg.wal_coalesce_ms = ms; }
    if let Some(df) = parsed.data_fsync { cfg.data_fsync = df; }
    if let Some(pc) = parsed.page_cache_pages { cfg.page_cache_pages = pc; }
    if let Some(ovf) = parsed.ovf_threshold_bytes { cfg.ovf_threshold_bytes = Some(ovf); }

    Ok(cfg)
}

/// Открыть writer с конфигом (JSON поверх env). NULL при ошибке.
#[no_mangle]
pub extern "C" fn qdb_open_with_config(path: *const c_char, cfg_json: *const c_char) -> *mut Handle {
    let res = (|| -> Result<Handle> {
        let root = cstr_to_path(path)?;
        let cfg = cfg_from_json(cfg_json)?;
        let db = Db::open_with_config(&root, cfg)?;
        Ok(Handle { db, readonly: false })
    })();

    match res {
        Ok(h) => Box::into_raw(Box::new(h)),
        Err(e) => { set_last_error(e); ptr::null_mut() }
    }
}

/// Открыть reader с конфигом (JSON поверх env). NULL при ошибке.
#[no_mangle]
pub extern "C" fn qdb_open_ro_with_config(path: *const c_char, cfg_json: *const c_char) -> *mut Handle {
    let res = (|| -> Result<Handle> {
        let root = cstr_to_path(path)?;
        let cfg = cfg_from_json(cfg_json)?;
        let db = Db::open_ro_with_config(&root, cfg)?;
        Ok(Handle { db, readonly: true })
    })();

    match res {
        Ok(h) => Box::into_raw(Box::new(h)),
        Err(e) => { set_last_error(e); ptr::null_mut() }
    }
}

/// Закрыть handle.
#[no_mangle]
pub extern "C" fn qdb_close(h: *mut Handle) {
    if !h.is_null() {
        unsafe { drop(Box::from_raw(h)); }
    }
}

// -------- put/get/del --------

struct Handle {
    db: Db,
    readonly: bool,
}

/// Вставка/обновление (writer). 0 = OK, -1 = ошибка, 1 = readonly.
#[no_mangle]
pub extern "C" fn qdb_put(
    h: *mut Handle,
    key_ptr: *const c_uchar,
    key_len: usize,
    val_ptr: *const c_uchar,
    val_len: usize,
) -> c_int {
    if h.is_null() { return -1; }
    let handle = unsafe { &mut *h };
    if handle.readonly { return 1; }
    let res = (|| -> Result<()> {
        if key_ptr.is_null() { anyhow::bail!("null key"); }
        if val_ptr.is_null() { anyhow::bail!("null value"); }
        let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
        let val = unsafe { std::slice::from_raw_parts(val_ptr, val_len) };
        handle.db.put(key, val)?;
        Ok(())
    })();
    match res { Ok(()) => 0, Err(e) => { set_last_error(e); -1 } }
}

/// Получение. 0 => found (out_ptr/out_len заполнены), 1 => not found, -1 => ошибка.
/// Буфер нужно освобождать qdb_free_buf().
#[no_mangle]
pub extern "C" fn qdb_get(
    h: *mut Handle,
    key_ptr: *const c_uchar,
    key_len: usize,
    out_ptr: *mut *mut c_uchar,
    out_len: *mut usize,
) -> c_int {
    if h.is_null() || key_ptr.is_null() || out_ptr.is_null() || out_len.is_null() {
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
    match res { Ok(code) => code, Err(e) => { set_last_error(e); -1 } }
}

/// Удаление. 0 => deleted, 1 => not found (или readonly), -1 => ошибка.
#[no_mangle]
pub extern "C" fn qdb_del(
    h: *mut Handle,
    key_ptr: *const c_uchar,
    key_len: usize,
) -> c_int {
    if h.is_null() || key_ptr.is_null() { return -1; }
    let handle = unsafe { &mut *h };
    if handle.readonly { return 1; }
    let res = (|| -> Result<c_int> {
        let key = unsafe { std::slice::from_raw_parts(key_ptr, key_len) };
        let ok = handle.db.del(key)?;
        Ok(if ok { 0 } else { 1 })
    })();
    match res { Ok(code) => code, Err(e) => { set_last_error(e); -1 } }
}

/// Освобождение буфера, возвращённого qdb_get().
#[no_mangle]
pub extern "C" fn qdb_free_buf(ptr_: *mut c_uchar, len: usize) {
    if ptr_.is_null() || len == 0 { return; }
    unsafe { let _ = Vec::from_raw_parts(ptr_, len, len); }
}

// -------- NEW: streaming scans via callback --------

pub type qdb_scan_cb = extern "C" fn(
    key_ptr: *const c_uchar,
    key_len: usize,
    val_ptr: *const c_uchar,
    val_len: usize,
    user: *mut c_void,
);

/// Полный скан. cb вызывается для каждой пары; данные валидны только во время вызова cb.
#[no_mangle]
pub extern "C" fn qdb_scan_all(
    h: *mut Handle,
    cb: Option<qdb_scan_cb>,
    user: *mut c_void,
) -> c_int {
    if h.is_null() { return -1; }
    let cb = match cb { Some(f) => f, None => return -1 };
    let handle = unsafe { &mut *h };
    let res = (|| -> Result<()> {
        let pairs = handle.db.scan_all()?;
        for (k, v) in pairs {
            cb(k.as_ptr(), k.len(), v.as_ptr(), v.len(), user);
        }
        Ok(())
    })();
    match res { Ok(()) => 0, Err(e) => { set_last_error(e); -1 } }
}

/// Скан по префиксу. cb вызывается для каждой пары; данные валидны только во время cb.
#[no_mangle]
pub extern "C" fn qdb_scan_prefix(
    h: *mut Handle,
    prefix_ptr: *const c_uchar,
    prefix_len: usize,
    cb: Option<qdb_scan_cb>,
    user: *mut c_void,
) -> c_int {
    if h.is_null() || prefix_ptr.is_null() { return -1; }
    let cb = match cb { Some(f) => f, None => return -1 };
    let handle = unsafe { &mut *h };
    let res = (|| -> Result<()> {
        let pref = unsafe { std::slice::from_raw_parts(prefix_ptr, prefix_len) };
        let pairs = handle.db.scan_prefix(pref)?;
        for (k, v) in pairs {
            cb(k.as_ptr(), k.len(), v.as_ptr(), v.len(), user);
        }
        Ok(())
    })();
    match res { Ok(()) => 0, Err(e) => { set_last_error(e); -1 } }
}

// -------- NEW: версия --------

/// Вернуть строку версии crate (освобождать qdb_free_string).
#[no_mangle]
pub extern "C" fn qdb_version_dup() -> *mut c_char {
    CString::new(env!("CARGO_PKG_VERSION")).unwrap().into_raw()
}