use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Result, anyhow, bail};
use flate2::{Decompress, FlushDecompress, Status};

use crate::data::object::{ObjectStore, ObjectType};

use super::parse::{parse_object_header_partial, parse_ofs_delta_base_partial, parse_pack_header};
use super::types::{PackEntryInfo, PackEntryInfoKind, PackTransferProgress, ParsedPack};

pub struct PackStream {
    file: File,
    temp_path: PathBuf,
    buffer: Vec<u8>,
    buffer_start: usize,
    next_offset: usize,
    total_objects: Option<usize>,
    entries: Vec<PackEntryInfo>,
    current_entry: Option<StreamingEntry>,
    pack_bytes: usize,
}

struct StreamingEntry {
    offset: usize,
    kind: StreamingEntryKind,
    data_offset: usize,
    input_offset: usize,
    decompressor: Decompress,
}

enum StreamingEntryKind {
    Base(ObjectType),
    OfsDelta { base_offset: usize },
    RefDelta { base_hash: String },
}

impl PackStream {
    pub fn new(pack_dir: &Path) -> Result<Self> {
        fs::create_dir_all(pack_dir)?;
        let temp_path = temp_pack_path(pack_dir);
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)?;
        Ok(Self {
            file,
            temp_path,
            buffer: Vec::new(),
            buffer_start: 0,
            next_offset: 0,
            total_objects: None,
            entries: Vec::new(),
            current_entry: None,
            pack_bytes: 0,
        })
    }

    pub fn append(&mut self, chunk: &[u8]) -> Result<PackTransferProgress> {
        self.file.write_all(chunk)?;
        self.pack_bytes += chunk.len();
        self.buffer.extend_from_slice(chunk);

        if self.total_objects.is_none() {
            self.total_objects = parse_pack_header(&self.buffer)?;
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
            self.prune_buffer();
        }

        Ok(PackTransferProgress {
            total_objects: self.total_objects,
            received_objects: self.entries.len(),
        })
    }

    pub fn finish(mut self) -> Result<ParsedPack> {
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

        let checksum = self.pack_checksum()?;
        self.file.flush()?;
        drop(self.file);

        let pack_hash_hex = ObjectStore::hash_bytes_to_hex(&checksum);
        let pack_path = self
            .temp_path
            .with_file_name(format!("pack-{pack_hash_hex}.pack"));
        if pack_path.exists() {
            fs::remove_file(&self.temp_path)?;
        } else {
            fs::rename(&self.temp_path, &pack_path)?;
        }

        Ok(ParsedPack {
            pack_path,
            pack_checksum: checksum,
            entries: self.entries,
            pack_bytes: self.pack_bytes,
        })
    }

    pub fn pack_bytes(&self) -> usize {
        self.pack_bytes
    }

    fn try_start_entry(&self) -> Result<Option<StreamingEntry>> {
        let offset = self.next_offset;
        if offset >= self.pack_bytes {
            return Ok(None);
        }

        let Some((type_id, header_len)) =
            parse_object_header_partial(self.slice_from_absolute(offset)?)?
        else {
            return Ok(None);
        };
        let mut cursor = offset + header_len;
        let kind = match type_id {
            1 => StreamingEntryKind::Base(ObjectType::Commit),
            2 => StreamingEntryKind::Base(ObjectType::Tree),
            3 => StreamingEntryKind::Base(ObjectType::Blob),
            6 => {
                let Some((distance, used)) =
                    parse_ofs_delta_base_partial(self.slice_from_absolute(cursor)?)?
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
                let base_hash_bytes = self
                    .bytes_at(cursor, 20)
                    .ok_or_else(|| anyhow!("missing ref-delta base hash"))?;
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
            decompressor: Decompress::new(true),
        }))
    }

    fn advance_current_entry(&mut self) -> Result<Option<StreamingEntry>> {
        let mut entry = self
            .current_entry
            .take()
            .ok_or_else(|| anyhow!("missing streaming pack entry"))?;
        let mut output = [0u8; 8192];

        loop {
            let input_start = entry.data_offset + entry.input_offset;
            let input = match self.slice_from_absolute(input_start) {
                Ok(input) if !input.is_empty() => input,
                Ok(_) => {
                    self.current_entry = Some(entry);
                    return Ok(None);
                }
                Err(_) => {
                    self.current_entry = Some(entry);
                    return Ok(None);
                }
            };

            let before_in = entry.decompressor.total_in();
            let before_out = entry.decompressor.total_out();
            let status =
                entry
                    .decompressor
                    .decompress(input, &mut output, FlushDecompress::None)?;
            let consumed = (entry.decompressor.total_in() - before_in) as usize;
            let produced = (entry.decompressor.total_out() - before_out) as usize;
            entry.input_offset += consumed;

            match status {
                Status::StreamEnd => return Ok(Some(entry)),
                Status::Ok => {
                    if consumed == 0 && produced == 0 {
                        self.current_entry = Some(entry);
                        return Ok(None);
                    }
                }
                Status::BufError => {
                    if input_start + consumed == self.pack_bytes {
                        self.current_entry = Some(entry);
                        return Ok(None);
                    }
                    if consumed == 0 && produced == 0 {
                        bail!("stalled while streaming pack object");
                    }
                }
            }
        }
    }

    fn prune_buffer(&mut self) {
        let keep_from = self.next_offset.saturating_sub(self.buffer_start);
        if keep_from == 0 {
            return;
        }
        self.buffer.drain(..keep_from);
        self.buffer_start = self.next_offset;
    }

    fn pack_checksum(&self) -> Result<[u8; 20]> {
        let trailer = self
            .bytes_at(self.next_offset, 20)
            .ok_or_else(|| anyhow!("missing pack checksum"))?;
        Ok(trailer.try_into().unwrap())
    }

    fn slice_from_absolute(&self, offset: usize) -> Result<&[u8]> {
        let local = offset
            .checked_sub(self.buffer_start)
            .ok_or_else(|| anyhow!("pack buffer underflow"))?;
        self.buffer
            .get(local..)
            .ok_or_else(|| anyhow!("pack buffer offset out of bounds"))
    }

    fn bytes_at(&self, offset: usize, len: usize) -> Option<&[u8]> {
        let local = offset.checked_sub(self.buffer_start)?;
        self.buffer.get(local..local + len)
    }
}

impl StreamingEntry {
    fn offset_after(&self) -> usize {
        self.data_offset + self.input_offset
    }

    fn finish(self) -> PackEntryInfo {
        let end_offset = self.offset_after();
        let kind = match self.kind {
            StreamingEntryKind::Base(object_type) => PackEntryInfoKind::Base { object_type },
            StreamingEntryKind::OfsDelta { base_offset } => {
                PackEntryInfoKind::OfsDelta { base_offset }
            }
            StreamingEntryKind::RefDelta { base_hash } => PackEntryInfoKind::RefDelta { base_hash },
        };

        PackEntryInfo {
            offset: self.offset,
            end_offset,
            kind,
        }
    }
}

fn temp_pack_path(pack_dir: &Path) -> PathBuf {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    pack_dir.join(format!(".tmp-pack-{}-{}.pack", process::id(), nanos))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::object::GIT_PACK_DIR;
    use tempfile::tempdir;

    #[test]
    fn test_pack_stream_finish_requires_complete_objects() {
        let temp = tempdir().unwrap();
        let pack_dir = temp.path().join(GIT_PACK_DIR);
        let mut stream = PackStream::new(&pack_dir).unwrap();
        stream
            .append(b"PACK\x00\x00\x00\x02\x00\x00\x00\x01")
            .unwrap();
        assert!(stream.finish().is_err());
    }
}
