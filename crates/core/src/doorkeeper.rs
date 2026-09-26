//! A Bloom filter that remembers which blocks were read recently, so a
//! block reaches disk only on its second read within a window.

/// Two Bloom filters: new entries go into the current one, and when it has
/// taken a window's worth, it becomes the previous one and a fresh filter
/// replaces it. An entry is remembered for one to two windows.
pub struct Doorkeeper {
    current: Bloom,
    previous: Bloom,
    inserted: u64,
    window: u64,
}

impl Doorkeeper {
    /// A doorkeeper that remembers at least `window` insertions.
    pub fn new(window: u64) -> Doorkeeper {
        let window = window.max(1);
        Doorkeeper {
            current: Bloom::new(window),
            previous: Bloom::new(window),
            inserted: 0,
            window,
        }
    }

    pub fn contains(&self, hash: u64) -> bool {
        self.current.contains(hash) || self.previous.contains(hash)
    }

    pub fn insert(&mut self, hash: u64) {
        if self.inserted == self.window {
            self.previous = std::mem::replace(&mut self.current, Bloom::new(self.window));
            self.inserted = 0;
        }
        self.current.insert(hash);
        self.inserted += 1;
    }
}

/// About 1% false positives at its capacity: ten bits and seven probes per
/// entry.
struct Bloom {
    words: Vec<u64>,
}

const BITS_PER_ENTRY: u64 = 10;
const PROBES: u64 = 7;

impl Bloom {
    fn new(capacity: u64) -> Bloom {
        let bits = (capacity * BITS_PER_ENTRY).max(64);
        Bloom {
            words: vec![0; bits.div_ceil(64) as usize],
        }
    }

    fn contains(&self, hash: u64) -> bool {
        self.probes(hash)
            .all(|bit| self.words[(bit / 64) as usize] & (1 << (bit % 64)) != 0)
    }

    fn insert(&mut self, hash: u64) {
        for bit in self.probes(hash) {
            self.words[(bit / 64) as usize] |= 1 << (bit % 64);
        }
    }

    /// Double hashing: probe `i` is `h1 + i * h2`.
    fn probes(&self, hash: u64) -> impl Iterator<Item = u64> + use<> {
        let bits = self.words.len() as u64 * 64;
        let second = hash.rotate_left(32).wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        (0..PROBES).map(move |i| hash.wrapping_add(i.wrapping_mul(second)) % bits)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hash(n: u64) -> u64 {
        xxhash_rust::xxh3::xxh3_64(&n.to_le_bytes())
    }

    #[test]
    fn remembers_for_a_window() {
        let mut doorkeeper = Doorkeeper::new(100);
        doorkeeper.insert(hash(0));
        for n in 1..150 {
            doorkeeper.insert(hash(n));
        }
        assert!(doorkeeper.contains(hash(0)));
        for n in 150..300 {
            doorkeeper.insert(hash(n));
        }
        assert!(!doorkeeper.contains(hash(0)));
    }

    #[test]
    fn false_positives_stay_near_one_percent() {
        let mut doorkeeper = Doorkeeper::new(10_000);
        for n in 0..10_000 {
            doorkeeper.insert(hash(n));
        }
        let false_positives = (10_000..110_000)
            .filter(|&n| doorkeeper.contains(hash(n)))
            .count();
        assert!(false_positives < 2_000, "{false_positives} in 100,000");
    }
}
