//! In-process subscriptions (live events) for Db.
//!
//! Scope (v1.1):
//! - Local (in-process) pub/sub: writer publishes events after successful commits.
//! - Subscribe by key prefix: tail-wins semantics at page level remain unchanged,
//!   but each put/del emits an Event.
//! - Drop of SubscriptionHandle unsubscribes.
//!
//! Notes:
//! - Callbacks are executed synchronously in the writer thread right after commit.
//!   Keep callbacks fast and non-blocking; if you need async work, spawn a thread/task.
//! - The registry is intended to be owned by Db (one per writer instance).
//! - This module does NOT depend on disk formats and can be reused.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Weak};

/// A single change event emitted by writer:
/// - key: affected key
/// - value: Some(value) for put/update; None for delete
/// - lsn: WAL LSN assigned to the commit (monotonic)
#[derive(Clone, Debug)]
pub struct Event {
    pub key: Vec<u8>,
    pub value: Option<Vec<u8>>,
    pub lsn: u64,
}

type Callback = Arc<dyn Fn(&Event) + Send + Sync + 'static>;

#[derive(Default)]
struct SubInner {
    next_id: u64,
    subs: HashMap<u64, (Vec<u8>, Callback)>, // id -> (prefix, cb)
}

/// Subscription registry (to be held inside Db).
pub struct SubRegistry {
    inner: Mutex<SubInner>,
}

impl SubRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(SubInner::default()),
        })
    }

    /// Subscribe for events whose key starts with `prefix`.
    /// Returns a handle; dropping it unsubscribes.
    pub fn subscribe(self: &Arc<Self>, prefix: Vec<u8>, cb: Callback) -> SubscriptionHandle {
        let mut g = self.inner.lock().unwrap();
        let id = g.next_id;
        g.next_id = g.next_id.wrapping_add(1);
        g.subs.insert(id, (prefix, cb));
        drop(g);
        SubscriptionHandle {
            id,
            reg: Arc::downgrade(self),
        }
    }

    /// Publish an event to all subscribers whose prefix matches the event key.
    pub fn publish(&self, ev: &Event) {
        let callbacks: Vec<Callback> = {
            let g = self.inner.lock().unwrap();
            g.subs
                .values()
                .filter_map(|(pref, cb)| {
                    if ev.key.starts_with(pref) {
                        Some(cb.clone())
                    } else {
                        None
                    }
                })
                .collect()
        };
        // Execute outside the lock
        for cb in callbacks {
            cb(ev);
        }
    }

    /// Unsubscribe by id (best-effort).
    fn unsubscribe(&self, id: u64) {
        let mut g = self.inner.lock().unwrap();
        g.subs.remove(&id);
    }
}

/// RAII handle: unsubscribes on drop.
pub struct SubscriptionHandle {
    id: u64,
    reg: Weak<SubRegistry>,
}

impl Drop for SubscriptionHandle {
    fn drop(&mut self) {
        if let Some(reg) = self.reg.upgrade() {
            reg.unsubscribe(self.id);
        }
    }
}

/// Public helpers for building callbacks.
pub fn callback<F>(f: F) -> Callback
where
    F: Fn(&Event) + Send + Sync + 'static,
{
    Arc::new(f)
}