//! O(1) LRU cache for pages.
//!
//! Design:
//! - HashMap<page_id, Entry> stores page content and doubly-linked pointers (prev/next by page_id).
//! - head = MRU, tail = LRU.
//! - get_mut() moves the node to head (if found and size matches) and copies data into `out`.
//! - put() updates existing (move to head) or inserts a new head, evicting tail if over capacity.
//!
//! Notes:
//! - API is intentionally the same as before: new/get_mut/put.
//! - No metrics here (caller may record hits/misses around `get_mut`).
//! - Capacity=0 disables the cache (no-op).

use std::collections::HashMap;

pub(crate) struct PageCache {
    cap: usize,
    page_size: usize,
    map: HashMap<u64, Entry>,
    head: Option<u64>, // Most-recently used
    tail: Option<u64>, // Least-recently used
}

struct Entry {
    data: Vec<u8>,
    prev: Option<u64>,
    next: Option<u64>,
}

impl PageCache {
    /// Create a cache for N pages of a fixed size.
    pub(crate) fn new(cap: usize, page_size: usize) -> Self {
        Self {
            cap,
            page_size,
            map: HashMap::with_capacity(cap.max(1)),
            head: None,
            tail: None,
        }
    }

    /// Get a page into `out` if present; moves entry to MRU (head).
    /// Returns true on hit (and copies bytes), false otherwise.
    pub(crate) fn get_mut(&mut self, page_id: u64, out: &mut [u8]) -> bool {
        if self.cap == 0 {
            return false;
        }
        if out.len() != self.page_size {
            return false;
        }
        if !self.map.contains_key(&page_id) {
            return false;
        }

        // Move to head and copy bytes.
        self.detach(page_id);
        self.attach_front(page_id);
        if let Some(e) = self.map.get(&page_id) {
            if e.data.len() == out.len() {
                out.copy_from_slice(&e.data);
                return true;
            }
        }
        false
    }

    /// Put page bytes into the cache (copy), updating/inserting and moving to MRU.
    /// If capacity is exceeded, evicts the LRU (tail).
    pub(crate) fn put(&mut self, page_id: u64, data: &[u8]) {
        if self.cap == 0 {
            return;
        }
        if data.len() != self.page_size {
            return;
        }

        if self.map.contains_key(&page_id) {
            // Update existing, move to head.
            if let Some(e) = self.map.get_mut(&page_id) {
                // Replace content; keep allocation stable if sizes match.
                if e.data.len() == data.len() {
                    e.data.copy_from_slice(data);
                } else {
                    e.data = data.to_vec();
                }
            }
            self.detach(page_id);
            self.attach_front(page_id);
            return;
        }

        // Need to insert a new entry; evict LRU if full.
        if self.map.len() >= self.cap {
            if let Some(victim) = self.tail {
                // Detach then remove from map.
                self.detach(victim);
                self.map.remove(&victim);
            }
        }

        // Insert new node as head (MRU).
        let entry = Entry {
            data: data.to_vec(),
            prev: None,
            next: None,
        };
        self.map.insert(page_id, entry);
        self.attach_front(page_id);
    }

    // ---------------- internal helpers ----------------

    fn detach(&mut self, page_id: u64) {
        // If not present, nothing to do.
        let (prev, next) = match self.map.get(&page_id) {
            Some(e) => (e.prev, e.next),
            None => return,
        };

        // Update head/tail if needed.
        if self.head == Some(page_id) {
            self.head = next;
        }
        if self.tail == Some(page_id) {
            self.tail = prev;
        }

        // Bridge neighbors.
        if let Some(p) = prev {
            if let Some(pe) = self.map.get_mut(&p) {
                pe.next = next;
            }
        }
        if let Some(n) = next {
            if let Some(ne) = self.map.get_mut(&n) {
                ne.prev = prev;
            }
        }

        // Clear pointers on this node.
        if let Some(e) = self.map.get_mut(&page_id) {
            e.prev = None;
            e.next = None;
        }
    }

    fn attach_front(&mut self, page_id: u64) {
        // Attach to head (MRU).
        if self.head == Some(page_id) {
            return;
        }

        // point new head to old head
        if let Some(e) = self.map.get_mut(&page_id) {
            e.prev = None;
            e.next = self.head;
        }

        // fix old head's prev
        if let Some(old_head) = self.head {
            if let Some(he) = self.map.get_mut(&old_head) {
                he.prev = Some(page_id);
            }
        }

        // set head
        self.head = Some(page_id);

        // if list was empty, tail also points to this node
        if self.tail.is_none() {
            self.tail = Some(page_id);
        }
    }
}