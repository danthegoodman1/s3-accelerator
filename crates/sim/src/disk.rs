//! A storage node's disk: extents of bytes that start as filler, so a read
//! of a slot nobody wrote returns bytes no object holds.

use s3_accelerator_core::store::Location;

pub struct Disk {
    extents: Vec<Vec<u8>>,
}

const FILLER: u8 = 0xa5;

impl Disk {
    pub fn new(extents: u32, extent_size: u64) -> Disk {
        Disk {
            extents: (0..extents)
                .map(|_| vec![FILLER; extent_size as usize])
                .collect(),
        }
    }

    pub fn read(&self, location: Location, offset: u64, len: u64) -> &[u8] {
        let start = (location.offset + offset) as usize;
        &self.extents[location.extent as usize][start..start + len as usize]
    }

    pub fn write(&mut self, location: Location, bytes: &[u8]) {
        let start = location.offset as usize;
        self.extents[location.extent as usize][start..start + bytes.len()].copy_from_slice(bytes);
    }
}
