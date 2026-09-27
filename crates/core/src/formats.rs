//! File formats that state where their metadata lies at a fixed spot:
//! Parquet and ORC in their trailers, safetensors in its first 8 bytes. A
//! home that reads the spot learns the metadata's span, and fills it before
//! the reader asks.

use crate::s3::ObjectKey;
use std::ops::Range;

/// The longest metadata the home prefetches.
const MAX_METADATA: u64 = 64 << 20;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Format {
    Parquet,
    Orc,
    Safetensors,
}

impl Format {
    /// The format a key's name says it holds.
    pub fn of(key: &ObjectKey) -> Option<Format> {
        let name = key.key.to_ascii_lowercase();
        if name.ends_with(".parquet") {
            Some(Format::Parquet)
        } else if name.ends_with(".orc") {
            Some(Format::Orc)
        } else if name.ends_with(".safetensors") {
            Some(Format::Safetensors)
        } else {
            None
        }
    }

    /// The bytes that state the metadata's span in an object of `size`
    /// bytes: Parquet's 8-byte trailer, the last 256 bytes of an ORC file,
    /// which hold its postscript and the postscript's length, and
    /// safetensors' 8-byte header length.
    pub fn spot(self, size: u64) -> Option<Range<u64>> {
        let spot = match self {
            Format::Parquet if size >= 12 => size - 8..size,
            Format::Orc if size >= 4 => size.saturating_sub(256)..size,
            Format::Safetensors if size >= 8 => 0..8,
            _ => return None,
        };
        Some(spot)
    }

    /// The span of an object's metadata, from the bytes at its spot, or
    /// `None` if they do not hold a sensible one.
    pub fn metadata(self, size: u64, spot: &[u8]) -> Option<Range<u64>> {
        let span = match self {
            Format::Parquet => {
                if spot.len() != 8 || &spot[4..] != b"PAR1" {
                    return None;
                }
                let len = u64::from(u32::from_le_bytes(spot[..4].try_into().ok()?));
                // The file starts with its 4-byte magic too.
                let start = (size - 8).checked_sub(len).filter(|&start| start >= 4)?;
                start..size - 8
            }
            Format::Orc => {
                let (&postscript_len, rest) = spot.split_last()?;
                let postscript_len = usize::from(postscript_len);
                let postscript = rest.get(rest.len().checked_sub(postscript_len)?..)?;
                let (footer, metadata) = orc_lengths(postscript)?;
                let tail = 1 + postscript_len as u64;
                let start = size
                    .checked_sub(tail)?
                    .checked_sub(footer)?
                    .checked_sub(metadata)?;
                // The file starts with its 3-byte magic.
                (start >= 3).then_some(start..size - tail)?
            }
            Format::Safetensors => {
                let len = u64::from_le_bytes(spot.try_into().ok()?);
                let end = 8u64.checked_add(len).filter(|&end| end <= size)?;
                8..end
            }
        };
        (!span.is_empty() && span.end - span.start <= MAX_METADATA).then_some(span)
    }
}

/// An ORC postscript's footer and metadata lengths: protobuf fields 1 and
/// 5. It must also name ORC's magic, field 8000.
fn orc_lengths(mut postscript: &[u8]) -> Option<(u64, u64)> {
    let (mut footer, mut metadata, mut magic) = (None, 0, false);
    while !postscript.is_empty() {
        let tag = varint(&mut postscript)?;
        let (field, wire) = (tag >> 3, tag & 7);
        match wire {
            0 => {
                let value = varint(&mut postscript)?;
                match field {
                    1 => footer = Some(value),
                    5 => metadata = value,
                    _ => {}
                }
            }
            2 => {
                let len = usize::try_from(varint(&mut postscript)?).ok()?;
                let value = postscript.get(..len)?;
                magic |= field == 8000 && value == b"ORC";
                postscript = &postscript[len..];
            }
            _ => return None,
        }
    }
    magic.then_some((footer?, metadata))
}

fn varint(bytes: &mut &[u8]) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let (&byte, rest) = bytes.split_first()?;
        *bytes = rest;
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Some(value);
        }
    }
    None
}

/// Test fixtures, and the simulator's model of objects in these formats.
pub mod fixtures {
    use super::Format;

    /// The bytes a `size`-byte object of `format` holds at its start and
    /// end, around metadata of `metadata` bytes, which the object ends
    /// with (safetensors: starts with). `None` if the object is too small.
    pub fn frame(format: Format, size: u64, metadata: u64) -> Option<(Vec<u8>, Vec<u8>)> {
        match format {
            Format::Parquet => {
                (size >= 12 + metadata).then_some(())?;
                let mut tail = u32::try_from(metadata).ok()?.to_le_bytes().to_vec();
                tail.extend_from_slice(b"PAR1");
                Some((b"PAR1".to_vec(), tail))
            }
            Format::Orc => {
                let mut postscript = vec![0x08];
                push_varint(&mut postscript, metadata);
                postscript.extend_from_slice(&[0x28, 0x00]);
                // Field 8000, length-delimited: tag 64002.
                postscript.extend_from_slice(&[0x82, 0xf4, 0x03, 0x03]);
                postscript.extend_from_slice(b"ORC");
                let mut tail = postscript;
                tail.push(u8::try_from(tail.len()).ok()?);
                (size >= 3 + metadata + tail.len() as u64).then_some(())?;
                Some((b"ORC".to_vec(), tail))
            }
            Format::Safetensors => {
                (size >= 8 + metadata).then_some(())?;
                Some((metadata.to_le_bytes().to_vec(), Vec::new()))
            }
        }
    }

    fn push_varint(bytes: &mut Vec<u8>, mut value: u64) {
        while value >= 0x80 {
            bytes.push((value as u8 & 0x7f) | 0x80);
            value >>= 7;
        }
        bytes.push(value as u8);
    }
}

#[cfg(test)]
mod tests {
    use super::fixtures::frame;
    use super::*;

    /// A `size`-byte object of `format` whose metadata is `metadata` bytes.
    fn object(format: Format, size: u64, metadata: u64) -> Vec<u8> {
        let (head, tail) = frame(format, size, metadata).unwrap();
        let mut bytes = vec![0xaa; size as usize];
        bytes[..head.len()].copy_from_slice(&head);
        let end = bytes.len() - tail.len();
        bytes[end..].copy_from_slice(&tail);
        bytes
    }

    fn metadata_of(format: Format, bytes: &[u8]) -> Option<Range<u64>> {
        let size = bytes.len() as u64;
        let spot = format.spot(size)?;
        format.metadata(size, &bytes[spot.start as usize..spot.end as usize])
    }

    #[test]
    fn finds_parquet_footers() {
        let bytes = object(Format::Parquet, 1_000, 300);
        assert_eq!(metadata_of(Format::Parquet, &bytes), Some(692..992));
        let mut broken = bytes.clone();
        broken[999] = b'X';
        assert_eq!(metadata_of(Format::Parquet, &broken), None);
        // A footer that would start before the file's magic.
        let mut long = object(Format::Parquet, 100, 80);
        long[92..96].copy_from_slice(&90u32.to_le_bytes());
        assert_eq!(metadata_of(Format::Parquet, &long), None);
    }

    #[test]
    fn finds_orc_tails() {
        let bytes = object(Format::Orc, 5_000, 1_234);
        let tail = frame(Format::Orc, 5_000, 1_234).unwrap().1.len() as u64;
        assert_eq!(
            metadata_of(Format::Orc, &bytes),
            Some(5_000 - tail - 1_234..5_000 - tail)
        );
        let mut broken = bytes.clone();
        let magic = bytes.len() - 4;
        broken[magic] = b'X';
        assert_eq!(metadata_of(Format::Orc, &broken), None);
    }

    #[test]
    fn finds_safetensors_headers() {
        let bytes = object(Format::Safetensors, 2_000, 700);
        assert_eq!(metadata_of(Format::Safetensors, &bytes), Some(8..708));
        let mut long = bytes.clone();
        long[..8].copy_from_slice(&5_000u64.to_le_bytes());
        assert_eq!(metadata_of(Format::Safetensors, &long), None);
    }

    #[test]
    fn names_say_the_format() {
        let key = |name: &str| ObjectKey {
            bucket: "b".into(),
            key: name.into(),
        };
        assert_eq!(Format::of(&key("t/part-0.PARQUET")), Some(Format::Parquet));
        assert_eq!(Format::of(&key("x.orc")), Some(Format::Orc));
        assert_eq!(Format::of(&key("m.safetensors")), Some(Format::Safetensors));
        assert_eq!(Format::of(&key("notes.txt")), None);
    }
}
