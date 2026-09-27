//! What the store's index costs in memory per block, which the spec's
//! Index bullet states. A counting allocator measures the heap the store
//! holds once full and evicting, with its ghost queue in use.

use s3_accelerator_core::placement::PlacementHash;
use s3_accelerator_core::store::{BlockKey, Store, StoreConfig, VersionId};
use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};

struct Counting;

static HELD: AtomicUsize = AtomicUsize::new(0);

// SAFETY: every call goes to the system allocator unchanged; the counter
// only tallies the sizes it hands out and takes back.
unsafe impl GlobalAlloc for Counting {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        HELD.fetch_add(layout.size(), Ordering::Relaxed);
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        HELD.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        HELD.fetch_add(new_size, Ordering::Relaxed);
        HELD.fetch_sub(layout.size(), Ordering::Relaxed);
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: Counting = Counting;

/// Heap bytes per block of a store with `extents` extents of `extent_size`
/// bytes, full of `block`-byte blocks after admitting twice its capacity.
fn bytes_per_block(extent_size: u64, extents: u32, block: u64) -> f64 {
    let before = HELD.load(Ordering::Relaxed);
    let mut store = Store::new(StoreConfig {
        extent_size,
        extents,
        min_slot: 4 << 10,
        max_slot: 1 << 20,
    });
    let capacity = extent_size / block * u64::from(extents);
    for index in 0..2 * capacity {
        let key = BlockKey {
            version: VersionId {
                key: index,
                version: u128::from(index),
            },
            index: 0,
        };
        store
            .reserve(key, block, index, PlacementHash(index))
            .expect("a slot");
        store.filled(key);
        store.drain_evicted();
    }
    let held = HELD.load(Ordering::Relaxed) - before;
    assert_eq!(store.blocks().count() as u64, capacity);
    drop(store);
    held as f64 / capacity as f64
}

#[test]
fn the_index_costs_what_the_spec_says() {
    // 1 MiB blocks in one-block extents: the default.
    let large = bytes_per_block(1 << 20, 100_000, 1 << 20);
    // 4 KiB blocks, 256 to an extent.
    let small = bytes_per_block(1 << 20, 400, 4 << 10);
    println!("bytes per block: {large:.0} for 1 MiB blocks, {small:.0} for 4 KiB blocks");
    assert!(large < 420.0, "{large:.0} bytes per 1 MiB block");
    assert!(small < 340.0, "{small:.0} bytes per 4 KiB block");
}
