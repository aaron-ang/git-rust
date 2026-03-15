use std::io::Cursor;

use anyhow::{Result, anyhow, bail};
use bytes::Buf;
use flate2::{Decompress, FlushDecompress, Status};

use crate::data::object::{ObjectStore, ObjectType};

use super::types::{PackEntry, PackEntryKind};

pub(crate) struct Packfile<'a> {
    data: &'a [u8],
    cursor: Cursor<&'a [u8]>,
}

impl<'a> Packfile<'a> {
    pub(crate) fn new(data: &'a [u8]) -> Self {
        Self {
            data,
            cursor: Cursor::new(data),
        }
    }

    pub(crate) fn parse(mut self) -> Result<Vec<PackEntry>> {
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

        Ok(PackEntry {
            offset,
            end_offset: self.offset(),
            kind,
        })
    }

    fn read_object_header(&self) -> Result<(u8, usize)> {
        parse_object_header_complete(self.remaining_data())
    }

    pub(crate) fn read_ofs_delta_base(&self) -> Result<(usize, usize)> {
        parse_ofs_delta_base_complete(self.remaining_data())
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

pub fn pack_object_count(data: &[u8]) -> Option<usize> {
    if data.len() < 12 || &data[..4] != b"PACK" {
        return None;
    }

    Some(u32::from_be_bytes(data[8..12].try_into().ok()?) as usize)
}

pub(crate) fn parse_pack_header(data: &[u8]) -> Result<Option<usize>> {
    if data.is_empty() {
        return Ok(None);
    }
    if data.len() < 4 {
        return Ok(None);
    }
    if &data[..4] != b"PACK" {
        bail!("invalid pack header");
    }
    if data.len() < 12 {
        return Ok(None);
    }

    let version = u32::from_be_bytes(data[4..8].try_into().unwrap());
    if version != 2 && version != 3 {
        bail!("unsupported pack version: {version}");
    }

    Ok(Some(
        u32::from_be_bytes(data[8..12].try_into().unwrap()) as usize
    ))
}

pub(crate) fn parse_ofs_delta_base_partial(input: &[u8]) -> Result<Option<(usize, usize)>> {
    let mut consumed = 0;
    let Some(&first) = input.get(consumed) else {
        return Ok(None);
    };
    let mut byte = first;
    consumed += 1;
    let mut offset = (byte & 0x7f) as usize;

    while byte & 0x80 != 0 {
        let Some(&next) = input.get(consumed) else {
            return Ok(None);
        };
        byte = next;
        consumed += 1;
        offset = ((offset + 1) << 7) | (byte & 0x7f) as usize;
    }

    Ok(Some((offset, consumed)))
}

pub(crate) fn parse_object_header_partial(input: &[u8]) -> Result<Option<(u8, usize)>> {
    let mut size_shift = 4;
    let mut consumed = 0;
    let Some(&first) = input.get(consumed) else {
        return Ok(None);
    };
    let object_type = (first >> 4) & 0x07;
    let mut byte = first;
    consumed += 1;

    while byte & 0x80 != 0 {
        let Some(&next) = input.get(consumed) else {
            return Ok(None);
        };
        byte = next;
        let _ = ((byte & 0x7f) as usize) << size_shift;
        size_shift += 7;
        consumed += 1;
    }

    Ok(Some((object_type, consumed)))
}

fn parse_object_header_complete(input: &[u8]) -> Result<(u8, usize)> {
    parse_object_header_partial(input)?.ok_or_else(|| anyhow!("truncated pack object header"))
}

fn parse_ofs_delta_base_complete(input: &[u8]) -> Result<(usize, usize)> {
    parse_ofs_delta_base_partial(input)?.ok_or_else(|| anyhow!("truncated ofs-delta base"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_ofs_delta_base() {
        let pack = Packfile::new(&[0x7f]);
        let (offset, consumed) = pack.read_ofs_delta_base().unwrap();
        assert_eq!(offset, 0x7f);
        assert_eq!(consumed, 1);
    }
}
