//! Events due at future ticks, delivered in due order and, within a tick,
//! in the order they were scheduled.

use std::collections::BTreeMap;

pub struct Queue<E> {
    events: BTreeMap<(u64, u64), E>,
    scheduled: u64,
}

impl<E> Default for Queue<E> {
    fn default() -> Queue<E> {
        Queue {
            events: BTreeMap::new(),
            scheduled: 0,
        }
    }
}

impl<E> Queue<E> {
    pub fn push(&mut self, due: u64, event: E) {
        self.events.insert((due, self.scheduled), event);
        self.scheduled += 1;
    }

    /// The next event due by `now`.
    pub fn pop_due(&mut self, now: u64) -> Option<E> {
        let entry = self.events.first_entry()?;
        if entry.key().0 > now {
            return None;
        }
        Some(entry.remove())
    }

    pub fn is_empty(&self) -> bool {
        self.events.is_empty()
    }

    /// Whether every event waiting passes `test`.
    pub fn all(&self, test: impl Fn(&E) -> bool) -> bool {
        self.events.values().all(test)
    }
}
