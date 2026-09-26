//! Where an object's bytes live: blocks, chunks and the home's region.

use crate::placement::Placement;
use crate::s3::ObjectKey;
use std::ops::{Range, RangeInclusive};

/// Block and chunk sizes. A block is the unit of fill, storage and eviction;
/// a chunk, a whole number of blocks, is the unit of placement.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Layout {
    block_size: u64,
    chunk_blocks: u64,
}

impl Layout {
    pub fn new(block_size: u64, chunk_blocks: u64) -> Layout {
        assert!(block_size > 0 && chunk_blocks > 0, "empty blocks or chunks");
        Layout {
            block_size,
            chunk_blocks,
        }
    }

    pub fn block_size(self) -> u64 {
        self.block_size
    }

    pub fn chunk_size(self) -> u64 {
        self.block_size * self.chunk_blocks
    }

    pub fn block_count(self, size: u64) -> u64 {
        size.div_ceil(self.block_size)
    }

    /// The bytes block `index` holds of an object of `size` bytes.
    pub fn block_span(self, size: u64, index: u64) -> Range<u64> {
        let first = index * self.block_size;
        first..(first + self.block_size).min(size)
    }

    /// The blocks that hold bytes `first..=last`.
    pub fn blocks_covering(self, first: u64, last: u64) -> RangeInclusive<u64> {
        first / self.block_size..=last / self.block_size
    }

    /// Where block `index` of an object of `size` bytes is placed. The home
    /// holds chunk 0 and every block overlapping the final chunk-sized
    /// region, so objects up to two chunks live entirely on their home.
    pub fn placement(self, key: &ObjectKey, size: u64, index: u64) -> Placement<'_> {
        let tail_start = size.saturating_sub(self.chunk_size());
        if index < self.chunk_blocks || (index + 1) * self.block_size > tail_start {
            Placement::Home(key)
        } else {
            Placement::Chunk(key, index / self.chunk_blocks)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key() -> ObjectKey {
        ObjectKey {
            bucket: "b".into(),
            key: "k".into(),
        }
    }

    fn chunk_of(layout: Layout, size: u64, index: u64) -> Option<u64> {
        match layout.placement(&key(), size, index) {
            Placement::Home(_) => None,
            Placement::Chunk(_, chunk) => Some(chunk),
        }
    }

    #[test]
    fn spans_cover_the_object() {
        let layout = Layout::new(10, 4);
        assert_eq!(layout.block_count(0), 0);
        assert_eq!(layout.block_count(25), 3);
        assert_eq!(layout.block_span(25, 0), 0..10);
        assert_eq!(layout.block_span(25, 2), 20..25);
        assert_eq!(layout.blocks_covering(9, 10), 0..=1);
        assert_eq!(layout.blocks_covering(20, 24), 2..=2);
    }

    #[test]
    fn objects_up_to_two_chunks_live_on_their_home() {
        let layout = Layout::new(10, 4);
        for size in [1, 40, 41, 79, 80] {
            for index in 0..layout.block_count(size) {
                assert_eq!(
                    chunk_of(layout, size, index),
                    None,
                    "size {size} block {index}"
                );
            }
        }
    }

    #[test]
    fn home_holds_chunk_0_and_the_final_chunk_sized_region() {
        // 13 blocks of 10 bytes, the last one 5 bytes: 125 bytes, chunks of 40.
        let layout = Layout::new(10, 4);
        let size = 125;
        let chunks: Vec<Option<u64>> = (0..13).map(|index| chunk_of(layout, size, index)).collect();
        // The final 40 bytes are 85..125: blocks 8 (80..90) through 12.
        let expected = [
            None,
            None,
            None,
            None,
            Some(1),
            Some(1),
            Some(1),
            Some(1),
            None,
            None,
            None,
            None,
            None,
        ];
        assert_eq!(chunks, expected);
    }
}
