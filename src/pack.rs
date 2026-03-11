use std::{collections::HashMap, io::Cursor};

use anyhow::{Result, anyhow, bail};
use bytes::Buf;
use flate2::{Decompress, FlushDecompress, Status};

use crate::object::{ObjectStore, ObjectType};

enum PackEntryKind {
    Base {
        object_type: ObjectType,
        body: Vec<u8>,
    },
    OfsDelta {
        base_offset: usize,
        delta: Vec<u8>,
    },
    RefDelta {
        base_hash: String,
        delta: Vec<u8>,
    },
}

struct PackEntry {
    offset: usize,
    kind: PackEntryKind,
}

#[derive(Clone)]
struct ResolvedObject {
    object_type: ObjectType,
    body: Vec<u8>,
}

struct Packfile<'a> {
    data: &'a [u8],
    cursor: Cursor<&'a [u8]>,
}

impl<'a> Packfile<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            cursor: Cursor::new(data),
        }
    }

    fn parse(mut self) -> Result<Vec<PackEntry>> {
        let count = self.parse_header()?;
        let mut entries = Vec::with_capacity(count as usize);
        for _ in 0..count {
            entries.push(self.parse_entry()?);
        }
        Ok(entries)
    }

    fn parse_header(&mut self) -> Result<u32> {
        if self.cursor.remaining() < 12 || self.cursor.copy_to_bytes(4).as_ref() != b"PACK" {
            bail!("invalid pack header");
        }

        let version = self.cursor.get_u32();
        if version != 2 && version != 3 {
            bail!("unsupported pack version: {version}");
        }

        Ok(self.cursor.get_u32())
    }

    fn parse_entry(&mut self) -> Result<PackEntry> {
        let offset = self.offset();
        let (type_id, header_len) = self.read_object_header()?;
        self.cursor.advance(header_len);

        let kind = match type_id {
            1 => PackEntryKind::Base {
                object_type: ObjectType::Commit,
                body: self.read_inflated_object()?,
            },
            2 => PackEntryKind::Base {
                object_type: ObjectType::Tree,
                body: self.read_inflated_object()?,
            },
            3 => PackEntryKind::Base {
                object_type: ObjectType::Blob,
                body: self.read_inflated_object()?,
            },
            6 => {
                let (distance, used) = self.read_ofs_delta_base()?;
                self.cursor.advance(used);
                let delta = self.read_inflated_object()?;
                PackEntryKind::OfsDelta {
                    base_offset: offset
                        .checked_sub(distance)
                        .ok_or_else(|| anyhow!("invalid ofs-delta base offset"))?,
                    delta,
                }
            }
            7 => {
                let base_hash = self.read_base_hash()?;
                let delta = self.read_inflated_object()?;
                PackEntryKind::RefDelta { base_hash, delta }
            }
            other => bail!("unsupported pack object type: {}", other),
        };

        Ok(PackEntry { offset, kind })
    }

    fn read_object_header(&self) -> Result<(u8, usize)> {
        let input = self.remaining_data();
        let mut size_shift = 4;
        let mut consumed = 0;
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
            let _ = ((byte & 0x7f) as usize) << size_shift;
            size_shift += 7;
            consumed += 1;
        }

        Ok((object_type, consumed))
    }

    fn read_ofs_delta_base(&self) -> Result<(usize, usize)> {
        let input = self.remaining_data();
        let mut consumed = 0;
        let mut byte = *input
            .get(consumed)
            .ok_or_else(|| anyhow!("truncated ofs-delta base"))?;
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

    fn inflate_object(&self, start: usize) -> Result<(Vec<u8>, usize)> {
        let input = self
            .data
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

    fn read_inflated_object(&mut self) -> Result<Vec<u8>> {
        let start = self.offset();
        let (body, used) = self.inflate_object(start)?;
        self.cursor.advance(used);
        Ok(body)
    }

    fn read_base_hash(&mut self) -> Result<String> {
        if self.cursor.remaining() < 20 {
            bail!("truncated ref-delta base hash");
        }

        let offset = self.offset();
        let hash = ObjectStore::hash_bytes_to_hex(&self.data[offset..offset + 20]);
        self.cursor.advance(20);
        Ok(hash)
    }

    fn offset(&self) -> usize {
        self.cursor.position() as usize
    }

    fn remaining_data(&self) -> &'a [u8] {
        &self.data[self.offset()..]
    }
}

pub fn unpack_into(store: &ObjectStore, pack: &[u8]) -> Result<()> {
    let entries = Packfile::new(pack).parse()?;
    let offset_to_index = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.offset, idx))
        .collect::<HashMap<_, _>>();

    let mut resolved = HashMap::<usize, ResolvedObject>::new();
    let mut resolved_hashes = HashMap::<String, usize>::new();

    for (idx, entry) in entries.iter().enumerate() {
        if let PackEntryKind::Base { object_type, body } = &entry.kind {
            let hash = ObjectStore::object_hash(*object_type, body);
            resolved_hashes.insert(hash, idx);
        }
    }

    for idx in 0..entries.len() {
        resolve_entry(
            idx,
            &entries,
            &offset_to_index,
            &mut resolved,
            &resolved_hashes,
            store,
        )?;
    }

    Ok(())
}

fn resolve_entry(
    index: usize,
    entries: &[PackEntry],
    offset_to_index: &HashMap<usize, usize>,
    resolved: &mut HashMap<usize, ResolvedObject>,
    resolved_hashes: &HashMap<String, usize>,
    store: &ObjectStore,
) -> Result<ResolvedObject> {
    if let Some(object) = resolved.get(&index) {
        return Ok(object.clone());
    }

    let object = match &entries[index].kind {
        PackEntryKind::Base { object_type, body } => {
            let _hash = store.write_object(*object_type, body)?;
            ResolvedObject {
                object_type: *object_type,
                body: body.clone(),
            }
        }
        PackEntryKind::OfsDelta { base_offset, delta } => {
            let base_index = *offset_to_index
                .get(base_offset)
                .ok_or_else(|| anyhow!("missing ofs-delta base object"))?;
            let base = resolve_entry(
                base_index,
                entries,
                offset_to_index,
                resolved,
                resolved_hashes,
                store,
            )?;
            let body = apply_delta(&base.body, delta)?;
            let _hash = store.write_object(base.object_type, &body)?;
            ResolvedObject {
                object_type: base.object_type,
                body,
            }
        }
        PackEntryKind::RefDelta { base_hash, delta } => {
            let base = if let Some(base_index) = resolved_hashes.get(base_hash) {
                resolve_entry(
                    *base_index,
                    entries,
                    offset_to_index,
                    resolved,
                    resolved_hashes,
                    store,
                )?
            } else {
                let (object_type, body) = store.read_object_body(base_hash)?;
                ResolvedObject { object_type, body }
            };

            let body = apply_delta(&base.body, delta)?;
            let _hash = store.write_object(base.object_type, &body)?;
            ResolvedObject {
                object_type: base.object_type,
                body,
            }
        }
    };

    resolved.insert(index, object.clone());
    Ok(object)
}

fn apply_delta(base: &[u8], delta: &[u8]) -> Result<Vec<u8>> {
    let mut delta = Cursor::new(delta);
    let base_size = read_varint(&mut delta)?;
    if base_size != base.len() {
        bail!(
            "delta base size mismatch, expected {}, got {}",
            base_size,
            base.len()
        );
    }
    let result_size = read_varint(&mut delta)?;
    let mut result = Vec::with_capacity(result_size);

    while delta.has_remaining() {
        let instruction = delta.get_u8();

        if instruction & 0x80 != 0 {
            let mut copy_offset = 0usize;
            let mut copy_size = 0usize;

            if instruction & 0x01 != 0 {
                copy_offset |= next_delta_byte(&mut delta)? as usize;
            }
            if instruction & 0x02 != 0 {
                copy_offset |= (next_delta_byte(&mut delta)? as usize) << 8;
            }
            if instruction & 0x04 != 0 {
                copy_offset |= (next_delta_byte(&mut delta)? as usize) << 16;
            }
            if instruction & 0x08 != 0 {
                copy_offset |= (next_delta_byte(&mut delta)? as usize) << 24;
            }
            if instruction & 0x10 != 0 {
                copy_size |= next_delta_byte(&mut delta)? as usize;
            }
            if instruction & 0x20 != 0 {
                copy_size |= (next_delta_byte(&mut delta)? as usize) << 8;
            }
            if instruction & 0x40 != 0 {
                copy_size |= (next_delta_byte(&mut delta)? as usize) << 16;
            }
            if copy_size == 0 {
                copy_size = 0x10000;
            }

            let end = copy_offset + copy_size;
            result.extend_from_slice(
                base.get(copy_offset..end)
                    .ok_or_else(|| anyhow!("delta copy exceeds base object"))?,
            );
            continue;
        }

        if instruction == 0 {
            bail!("invalid delta opcode 0");
        }

        let start = delta.position() as usize;
        let end = start + instruction as usize;
        result.extend_from_slice(
            delta
                .get_ref()
                .get(start..end)
                .ok_or_else(|| anyhow!("delta insert exceeds delta data"))?,
        );
        delta.advance(instruction as usize);
    }

    if result.len() != result_size {
        bail!(
            "delta result size mismatch, expected {}, got {}",
            result_size,
            result.len()
        );
    }

    Ok(result)
}

fn read_varint(input: &mut Cursor<&[u8]>) -> Result<usize> {
    let mut result = 0usize;
    let mut shift = 0usize;

    loop {
        let byte = next_delta_byte(input)?;
        result |= ((byte & 0x7f) as usize) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    Ok(result)
}

fn next_delta_byte(input: &mut Cursor<&[u8]>) -> Result<u8> {
    if !input.has_remaining() {
        bail!("truncated delta instruction");
    }
    Ok(input.get_u8())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_delta_insert() {
        let delta = [0x03, 0x06, 0x90, 0x03, 0x03, b'd', b'e', b'f'];
        let result = apply_delta(b"abc", &delta).unwrap();
        assert_eq!(result, b"abcdef");
    }

    #[test]
    fn test_read_ofs_delta_base() {
        let pack = Packfile::new(&[0x7f]);
        let (offset, consumed) = pack.read_ofs_delta_base().unwrap();
        assert_eq!(offset, 0x7f);
        assert_eq!(consumed, 1);
    }
}
