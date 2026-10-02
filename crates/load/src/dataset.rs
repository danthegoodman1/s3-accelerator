//! Every object's key, size and bytes follow from the plan's dataset, so
//! the hosts that seed it and the hosts that read it agree without asking
//! S3, and a reader checks each body byte against what it should hold.

use crate::plan::{Dataset, Set, SetFormat, SizeSpec};
use s3_accelerator_core::formats::Format;
use s3_accelerator_core::formats::fixtures::frame;
use xxhash_rust::xxh3::xxh3_64_with_seed;

/// One object of the dataset.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Object {
    pub key: String,
    pub size: u64,
    seed: u64,
    /// A format's framing at the object's start and end, over its bytes.
    head: Vec<u8>,
    tail: Vec<u8>,
}

impl Dataset {
    pub fn object(&self, set: &Set, index: u64) -> Object {
        let named = format!("{}/{index}", set.name);
        let seed = xxh3_64_with_seed(named.as_bytes(), self.seed);
        // A hash ahead of the index spreads keys across S3's partitions.
        let spread = mix(seed) >> 48;
        let extension = match set.format {
            Some(SetFormat::Parquet) => ".parquet",
            None => "",
        };
        let key = format!(
            "{}/{}/{spread:04x}/{index:09}{extension}",
            self.prefix, set.name
        );
        let unit = (mix(seed ^ 0x5eed) >> 11) as f64 / (1u64 << 53) as f64;
        let mut size = match set.size {
            SizeSpec::Fixed(size) => size,
            SizeSpec::Uniform(min, max) => min + ((max - min) as f64 * unit) as u64,
            SizeSpec::LogUniform(min, max) => {
                let (low, high) = ((min as f64).ln(), (max as f64).ln());
                ((low + (high - low) * unit).exp() as u64).clamp(min, max)
            }
        };
        let (head, tail) = match set.format {
            Some(SetFormat::Parquet) => {
                size = size.max(64);
                let footer = set.footer.max(1).min(size / 2);
                frame(Format::Parquet, size, footer).expect("room for the footer")
            }
            None => (Vec::new(), Vec::new()),
        };
        Object {
            key,
            size,
            seed,
            head,
            tail,
        }
    }
}

impl Object {
    /// An object a step writes to `key`, outside the dataset.
    pub fn written(key: String, size: u64) -> Object {
        let seed = xxh3_64_with_seed(key.as_bytes(), 0);
        Object {
            key,
            size,
            seed,
            head: Vec::new(),
            tail: Vec::new(),
        }
    }

    /// Fills `buffer` with the object's bytes from `offset`.
    pub fn fill(&self, offset: u64, buffer: &mut [u8]) {
        let end = offset + buffer.len() as u64;
        // The rest of the word `offset` falls in, then whole words, then
        // the start of the last word.
        let lead = (((8 - offset % 8) % 8) as usize).min(buffer.len());
        if lead > 0 {
            let from = (offset % 8) as usize;
            let bytes = word(self.seed, offset / 8).to_le_bytes();
            buffer[..lead].copy_from_slice(&bytes[from..from + lead]);
        }
        let aligned = offset + lead as u64;
        let rest = &mut buffer[lead..];
        let whole = rest.len() / 8 * 8;
        let (words, _) = rest[..whole].as_chunks_mut::<8>();
        for (slot, index) in words.iter_mut().zip(aligned / 8..) {
            *slot = word(self.seed, index).to_le_bytes();
        }
        let last = rest.len() - whole;
        if last > 0 {
            let bytes = word(self.seed, (aligned + whole as u64) / 8).to_le_bytes();
            rest[whole..].copy_from_slice(&bytes[..last]);
        }
        let head = self.head.len() as u64;
        if offset < head {
            let span = offset as usize..end.min(head) as usize;
            let len = span.len();
            buffer[..len].copy_from_slice(&self.head[span]);
        }
        let tail_start = self.size - self.tail.len() as u64;
        if end > tail_start {
            let from = offset.max(tail_start);
            let span = (from - tail_start) as usize..(end - tail_start) as usize;
            let at = (from - offset) as usize;
            buffer[at..at + span.len()].copy_from_slice(&self.tail[span]);
        }
    }

    /// Whether `bytes`, read from `offset`, are the object's, using
    /// `scratch` to hold what they should be.
    pub fn matches(&self, offset: u64, bytes: &[u8], scratch: &mut Vec<u8>) -> bool {
        if offset + bytes.len() as u64 > self.size {
            return false;
        }
        scratch.resize(bytes.len(), 0);
        self.fill(offset, scratch);
        scratch.as_slice() == bytes
    }
}

fn word(seed: u64, index: u64) -> u64 {
    mix(seed ^ index.wrapping_mul(0x9e37_79b9_7f4a_7c15))
}

/// SplitMix64's finalizer.
pub fn mix(value: u64) -> u64 {
    let mut z = value;
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::plan::Plan;

    fn dataset() -> Dataset {
        let plan = Plan::parse(
            r#"
            [dataset]
            prefix = "load"
            seed = 3
            [[dataset.sets]]
            name = "logs"
            count = 1000
            size = { log_uniform = ["4KiB", "1MiB"] }
            [[dataset.sets]]
            name = "tables"
            count = 4
            size = 100000
            format = "parquet"
            footer = "4KiB"
            "#,
        )
        .unwrap();
        plan.dataset
    }

    #[test]
    fn keys_and_sizes_follow_from_the_seed() {
        let dataset = dataset();
        let set = &dataset.sets[0];
        let object = dataset.object(set, 17);
        assert_eq!(object, dataset.object(set, 17));
        assert!(object.key.starts_with("load/logs/"));
        assert!(object.key.ends_with("/000000017"));
        let sizes: Vec<u64> = (0..1000)
            .map(|index| dataset.object(set, index).size)
            .collect();
        assert!(sizes.iter().all(|size| (4 << 10..=1 << 20).contains(size)));
        // Log-uniform: as many below the geometric mean as above.
        let below = sizes.iter().filter(|&&size| size < 64 << 10).count();
        assert!((400..600).contains(&below), "{below}");
        let table = dataset.object(&dataset.sets[1], 0);
        assert!(table.key.ends_with(".parquet"));
        assert_eq!(table.size, 100_000);
    }

    #[test]
    fn any_span_of_an_object_is_the_same_bytes() {
        let dataset = dataset();
        for object in [
            dataset.object(&dataset.sets[0], 5),
            dataset.object(&dataset.sets[1], 2),
        ] {
            let mut whole = vec![0; object.size as usize];
            object.fill(0, &mut whole);
            let mut scratch = Vec::new();
            let size = object.size;
            for (offset, len) in [
                (0, 1),
                (3, 13),
                (7, size / 2),
                (size - 11, 11),
                (size / 3, size / 3),
            ] {
                let span = &whole[offset as usize..(offset + len) as usize];
                let mut part = vec![0; len as usize];
                object.fill(offset, &mut part);
                assert_eq!(part, span, "{} at {offset}", object.key);
                assert!(object.matches(offset, span, &mut scratch));
            }
            let mut wrong = whole[100..200].to_vec();
            wrong[50] ^= 1;
            assert!(!object.matches(100, &wrong, &mut scratch));
            assert!(!object.matches(101, &whole[100..200], &mut scratch));
        }
    }

    #[test]
    fn a_parquet_object_ends_with_its_footers_length() {
        let dataset = dataset();
        let object = dataset.object(&dataset.sets[1], 1);
        let mut whole = vec![0; object.size as usize];
        object.fill(0, &mut whole);
        assert_eq!(&whole[..4], b"PAR1");
        assert_eq!(&whole[whole.len() - 4..], b"PAR1");
        let length =
            u32::from_le_bytes(whole[whole.len() - 8..whole.len() - 4].try_into().unwrap());
        assert_eq!(length, 4 << 10);
    }

    #[test]
    fn objects_differ() {
        let dataset = dataset();
        let (one, two) = (
            dataset.object(&dataset.sets[0], 1),
            dataset.object(&dataset.sets[0], 2),
        );
        let (mut left, mut right) = (vec![0; 64], vec![0; 64]);
        one.fill(0, &mut left);
        two.fill(0, &mut right);
        assert_ne!(left, right);
    }
}
