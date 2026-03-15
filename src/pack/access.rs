use std::fs;
use std::path::PathBuf;

use anyhow::{Result, anyhow, bail};
use flate2::{Decompress, FlushDecompress, Status};

use crate::data::object::{ObjectStore, ObjectType};

use super::types::{IndexedObject, PackEntry, PackEntryKind, PackObjectLocation};

pub(crate) struct PackIndex {
    pack_path: PathBuf,
    entries: Vec<IndexedObject>,
}

impl PackIndex {
    pub(crate) fn read(idx_path: PathBuf) -> Result<Self> {
        let data = fs::read(&idx_path)?;
        if data.len() < 8 || &data[..4] != b"\xfftOc" {
            bail!("unsupported pack index format");
        }

        let version = u32::from_be_bytes(data[4..8].try_into().unwrap());
        if version != 2 {
            bail!("unsupported pack index version: {version}");
        }

        let mut cursor = 8usize;
        let mut fanout = [0u32; 256];
        for slot in &mut fanout {
            *slot = u32::from_be_bytes(data[cursor..cursor + 4].try_into().unwrap());
            cursor += 4;
        }

        let object_count = fanout[255] as usize;
        let hashes_start = cursor;
        let hashes_end = hashes_start + object_count * 20;
        let crc_start = hashes_end;
        let crc_end = crc_start + object_count * 4;
        let offsets_start = crc_end;
        let offsets_end = offsets_start + object_count * 4;
        if data.len() < offsets_end + 40 {
            bail!("truncated pack index");
        }

        let small_offsets = (0..object_count)
            .map(|idx| {
                let start = offsets_start + idx * 4;
                u32::from_be_bytes(data[start..start + 4].try_into().unwrap())
            })
            .collect::<Vec<_>>();
        let large_count = small_offsets.iter().filter(|offset| **offset & 0x8000_0000 != 0).count();
        let large_start = offsets_end;
        let large_end = large_start + large_count * 8;
        if data.len() < large_end + 40 {
            bail!("truncated pack index large-offset table");
        }

        let mut entries = Vec::with_capacity(object_count);
        let mut large_index = 0usize;
        for (idx, small_offset) in small_offsets.iter().enumerate().take(object_count) {
            let hash_start = hashes_start + idx * 20;
            let crc_offset = crc_start + idx * 4;
            let hash = data[hash_start..hash_start + 20].try_into().unwrap();
            let crc32 = u32::from_be_bytes(data[crc_offset..crc_offset + 4].try_into().unwrap());
            let offset = if *small_offset & 0x8000_0000 == 0 {
                *small_offset as u64
            } else {
                let start = large_start + large_index * 8;
                large_index += 1;
                u64::from_be_bytes(data[start..start + 8].try_into().unwrap())
            };
            entries.push(IndexedObject {
                hash,
                offset,
                crc32,
            });
        }

        Ok(Self {
            pack_path: idx_path.with_extension("pack"),
            entries,
        })
    }

    pub(crate) fn find(&self, hash: &[u8; 20]) -> Option<PackObjectLocation> {
        let idx = self
            .entries
            .binary_search_by(|entry| entry.hash.cmp(hash))
            .ok()?;
        Some(PackObjectLocation {
            pack_path: self.pack_path.clone(),
            offset: self.entries[idx].offset,
        })
    }
}

pub(crate) fn read_packed_object_at(data: &[u8], offset: usize) -> Result<PackEntry> {
    let (type_id, header_len) = parse_object_header(&data[offset..])?;
    let mut cursor = offset + header_len;
    match type_id {
        1..=3 => {
            let object_type = match type_id {
                1 => ObjectType::Commit,
                2 => ObjectType::Tree,
                _ => ObjectType::Blob,
            };
            let (body, compressed_len) = inflate_object(data, cursor)?;
            Ok(PackEntry {
                offset,
                end_offset: cursor + compressed_len,
                kind: PackEntryKind::Base { object_type, body },
            })
        }
        6 => {
            let (distance, used) = parse_ofs_delta_base(&data[cursor..])?;
            cursor += used;
            let (delta, compressed_len) = inflate_object(data, cursor)?;
            Ok(PackEntry {
                offset,
                end_offset: cursor + compressed_len,
                kind: PackEntryKind::OfsDelta {
                    base_offset: offset
                        .checked_sub(distance)
                        .ok_or_else(|| anyhow!("invalid ofs-delta base offset"))?,
                    delta,
                },
            })
        }
        7 => {
            let base_hash = ObjectStore::hash_bytes_to_hex(
                data.get(cursor..cursor + 20)
                    .ok_or_else(|| anyhow!("truncated ref-delta base hash"))?,
            );
            cursor += 20;
            let (delta, compressed_len) = inflate_object(data, cursor)?;
            Ok(PackEntry {
                offset,
                end_offset: cursor + compressed_len,
                kind: PackEntryKind::RefDelta { base_hash, delta },
            })
        }
        other => bail!("unsupported pack object type: {}", other),
    }
}

fn parse_object_header(input: &[u8]) -> Result<(u8, usize)> {
    let mut consumed = 0usize;
    let first = *input
        .get(consumed)
        .ok_or_else(|| anyhow!("truncated pack object header"))?;
    let object_type = (first >> 4) & 0x07;
    let mut byte = first;
    consumed += 1;

    while byte & 0x80 != 0 {
        byte = *input
            .get(consumed)
            .ok_or_else(|| anyhow!("truncated pack object header"))?;
        consumed += 1;
    }

    Ok((object_type, consumed))
}

fn parse_ofs_delta_base(input: &[u8]) -> Result<(usize, usize)> {
    let mut consumed = 0usize;
    let first = *input
        .get(consumed)
        .ok_or_else(|| anyhow!("truncated ofs-delta base"))?;
    let mut byte = first;
    consumed += 1;
    let mut offset = (byte & 0x7f) as usize;

    while byte & 0x80 != 0 {
        byte = *input
            .get(consumed)
            .ok_or_else(|| anyhow!("truncated ofs-delta base"))?;
        consumed += 1;
        offset = ((offset + 1) << 7) | (byte & 0x7f) as usize;
    }

    Ok((offset, consumed))
}

fn inflate_object(data: &[u8], start: usize) -> Result<(Vec<u8>, usize)> {
    let input = data
        .get(start..)
        .ok_or_else(|| anyhow!("pack offset out of bounds"))?;
    let mut decompressor = Decompress::new(true);
    let mut output = Vec::new();
    let mut input_offset = 0usize;
    let mut buffer = [0u8; 8192];

    loop {
        let before_in = decompressor.total_in();
        let before_out = decompressor.total_out();
        let status = decompressor.decompress(
            &input[input_offset..],
            &mut buffer,
            FlushDecompress::None,
        )?;
        let consumed = (decompressor.total_in() - before_in) as usize;
        let produced = (decompressor.total_out() - before_out) as usize;
        input_offset += consumed;
        output.extend_from_slice(&buffer[..produced]);

        match status {
            Status::StreamEnd => break,
            Status::Ok | Status::BufError => {
                if consumed == 0 && produced == 0 {
                    bail!("stalled while inflating pack object");
                }
            }
        }
    }

    Ok((output, input_offset))
}
