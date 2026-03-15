use anyhow::{Result, anyhow, bail};
use flate2::{Decompress, FlushDecompress, Status};

use crate::object::{ObjectStore, ObjectType};

use super::parse::{parse_object_header_partial, parse_ofs_delta_base_partial, parse_pack_header};
use super::types::{PackEntry, PackEntryKind, PackTransferProgress, ParsedPack};

#[derive(Default)]
pub struct PackStream {
    data: Vec<u8>,
    next_offset: usize,
    total_objects: Option<usize>,
    entries: Vec<PackEntry>,
    current_entry: Option<StreamingEntry>,
}

struct StreamingEntry {
    offset: usize,
    kind: StreamingEntryKind,
    data_offset: usize,
    input_offset: usize,
    body: Vec<u8>,
    decompressor: Decompress,
}

enum StreamingEntryKind {
    Base(ObjectType),
    OfsDelta { base_offset: usize },
    RefDelta { base_hash: String },
}

impl PackStream {
    pub fn append(&mut self, chunk: &[u8]) -> Result<PackTransferProgress> {
        self.data.extend_from_slice(chunk);

        if self.total_objects.is_none() {
            self.total_objects = parse_pack_header(&self.data)?;
            if let Some(total_objects) = self.total_objects {
                self.next_offset = 12;
                self.entries.reserve(total_objects);
            }
        }

        while self.entries.len() < self.total_objects.unwrap_or(0) {
            if self.current_entry.is_none() {
                let Some(entry) = self.try_start_entry()? else {
                    break;
                };
                self.current_entry = Some(entry);
            }

            let Some(entry) = self.advance_current_entry()? else {
                break;
            };
            self.next_offset = entry.offset_after();
            self.entries.push(entry.finish());
            self.current_entry = None;
        }

        Ok(PackTransferProgress {
            total_objects: self.total_objects,
            received_objects: self.entries.len(),
        })
    }

    pub fn finish(self) -> Result<ParsedPack> {
        let total_objects = self
            .total_objects
            .ok_or_else(|| anyhow!("missing pack header"))?;
        if self.entries.len() != total_objects {
            bail!(
                "incomplete pack stream, expected {} objects, got {}",
                total_objects,
                self.entries.len()
            );
        }
        if self.current_entry.is_some() {
            bail!("incomplete pack stream, object truncated");
        }

        Ok(ParsedPack {
            entries: self.entries,
            pack_bytes: self.data.len(),
        })
    }

    pub fn pack_bytes(&self) -> usize {
        self.data.len()
    }

    fn try_start_entry(&self) -> Result<Option<StreamingEntry>> {
        let offset = self.next_offset;
        if offset >= self.data.len() {
            return Ok(None);
        }

        let Some((type_id, header_len)) = parse_object_header_partial(&self.data[offset..])? else {
            return Ok(None);
        };
        let mut cursor = offset + header_len;
        let kind = match type_id {
            1 => StreamingEntryKind::Base(ObjectType::Commit),
            2 => StreamingEntryKind::Base(ObjectType::Tree),
            3 => StreamingEntryKind::Base(ObjectType::Blob),
            6 => {
                let Some((distance, used)) = parse_ofs_delta_base_partial(&self.data[cursor..])?
                else {
                    return Ok(None);
                };
                cursor += used;
                StreamingEntryKind::OfsDelta {
                    base_offset: offset
                        .checked_sub(distance)
                        .ok_or_else(|| anyhow!("invalid ofs-delta base offset"))?,
                }
            }
            7 => {
                let Some(base_hash_bytes) = self.data.get(cursor..cursor + 20) else {
                    return Ok(None);
                };
                cursor += 20;
                StreamingEntryKind::RefDelta {
                    base_hash: ObjectStore::hash_bytes_to_hex(base_hash_bytes),
                }
            }
            other => bail!("unsupported pack object type: {}", other),
        };

        Ok(Some(StreamingEntry {
            offset,
            kind,
            data_offset: cursor,
            input_offset: 0,
            body: Vec::new(),
            decompressor: Decompress::new(true),
        }))
    }

    fn advance_current_entry(&mut self) -> Result<Option<StreamingEntry>> {
        let entry = self
            .current_entry
            .as_mut()
            .ok_or_else(|| anyhow!("missing streaming pack entry"))?;
        let mut output = [0u8; 8192];

        loop {
            let input_start = entry.data_offset + entry.input_offset;
            let Some(input) = self.data.get(input_start..) else {
                bail!("pack stream offset out of bounds");
            };
            if input.is_empty() {
                return Ok(None);
            }

            let before_in = entry.decompressor.total_in();
            let before_out = entry.decompressor.total_out();
            let status =
                entry
                    .decompressor
                    .decompress(input, &mut output, FlushDecompress::None)?;
            let consumed = (entry.decompressor.total_in() - before_in) as usize;
            let produced = (entry.decompressor.total_out() - before_out) as usize;
            entry.input_offset += consumed;
            entry.body.extend_from_slice(&output[..produced]);

            match status {
                Status::StreamEnd => return Ok(self.current_entry.take()),
                Status::Ok => {
                    if consumed == 0 && produced == 0 {
                        return Ok(None);
                    }
                }
                Status::BufError => {
                    if input_start + consumed == self.data.len() {
                        return Ok(None);
                    }
                    if consumed == 0 && produced == 0 {
                        bail!("stalled while streaming pack object");
                    }
                }
            }
        }
    }
}

impl StreamingEntry {
    fn offset_after(&self) -> usize {
        self.data_offset + self.input_offset
    }

    fn finish(self) -> PackEntry {
        let kind = match self.kind {
            StreamingEntryKind::Base(object_type) => PackEntryKind::Base {
                object_type,
                body: self.body,
            },
            StreamingEntryKind::OfsDelta { base_offset } => PackEntryKind::OfsDelta {
                base_offset,
                delta: self.body,
            },
            StreamingEntryKind::RefDelta { base_hash } => PackEntryKind::RefDelta {
                base_hash,
                delta: self.body,
            },
        };

        PackEntry {
            offset: self.offset,
            kind,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pack_stream_finish_requires_complete_objects() {
        let mut stream = PackStream::default();
        stream
            .append(b"PACK\x00\x00\x00\x02\x00\x00\x00\x01")
            .unwrap();
        assert!(stream.finish().is_err());
    }
}
