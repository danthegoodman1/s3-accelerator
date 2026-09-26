//! The simulated network: delivers each message after a seeded delay.

use crate::prng::Prng;
use std::collections::BTreeMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Address {
    Client(usize),
    Gateway(usize),
    Node(usize),
    Origin,
}

pub struct Network<M> {
    /// Keyed by due tick, then send order.
    in_flight: BTreeMap<(u64, u64), (Address, M)>,
    sent: u64,
    delay_min: u64,
    delay_max: u64,
}

impl<M> Network<M> {
    /// A network whose one-way delays are uniform in `delay_min..=delay_max`
    /// ticks. Messages on different paths, and on the same path, reorder.
    pub fn new(delay_min: u64, delay_max: u64) -> Network<M> {
        Network {
            in_flight: BTreeMap::new(),
            sent: 0,
            delay_min,
            delay_max,
        }
    }

    pub fn send(&mut self, prng: &mut Prng, now: u64, to: Address, message: M) {
        let due = now + prng.range(self.delay_min..=self.delay_max);
        self.in_flight.insert((due, self.sent), (to, message));
        self.sent += 1;
    }

    /// The next message due by `now`, in due order.
    pub fn deliver(&mut self, now: u64) -> Option<(Address, M)> {
        let entry = self.in_flight.first_entry()?;
        if entry.key().0 > now {
            return None;
        }
        Some(entry.remove())
    }

    pub fn is_empty(&self) -> bool {
        self.in_flight.is_empty()
    }
}
