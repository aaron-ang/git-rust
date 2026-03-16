use std::collections::HashMap;
use std::fs;
use std::io::Read;
use std::path::Path;
use std::path::PathBuf;

use anyhow::{Result, anyhow, bail};
use flate2::read::ZlibDecoder;
use flate2::{Decompress, FlushDecompress, Status};

use crate::data::object::{GIT_OBJECTS_DIR, GIT_PACK_DIR, ObjectStore, ObjectType};

use super::types::{IndexedObject, PackEntry, PackEntryKind, PackObjectLocation};

type OffsetCache = HashMap<(PathBuf, u64), PackedObject>;

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
        let large_count = small_offsets
            .iter()
            .filter(|offset| **offset & 0x8000_0000 != 0)
            .count();
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

#[derive(Clone)]
struct PackedObject {
    hash: String,
    object_type: ObjectType,
    body: Vec<u8>,
}

pub(crate) struct PackObjectReader {
    git_dir: PathBuf,
    pack_indices: Option<Vec<PackIndex>>,
    pack_data: HashMap<PathBuf, Vec<u8>>,
    object_cache: HashMap<String, (ObjectType, Vec<u8>)>,
    offset_cache: OffsetCache,
}

impl PackObjectReader {
    pub(crate) fn new(git_dir: PathBuf) -> Self {
        Self {
            git_dir,
            pack_indices: None,
            pack_data: HashMap::new(),
            object_cache: HashMap::new(),
            offset_cache: HashMap::new(),
        }
    }

    pub(crate) fn read_object_body(&mut self, hash: &str) -> Result<(ObjectType, Vec<u8>)> {
        if let Some(cached) = self.object_cache.get(hash).cloned() {
            return Ok(cached);
        }

        let hash_bytes = ObjectStore::hash_hex_to_bytes(hash)?;
        let hash_bytes: [u8; 20] = hash_bytes.try_into().unwrap();
        if let Some(location) = self.find_packed_object(&hash_bytes)? {
            let object = self.resolve_packed_object_at(&location.pack_path, location.offset)?;
            self.object_cache
                .insert(hash.to_string(), (object.object_type, object.body.clone()));
            return Ok((object.object_type, object.body));
        }

        if let Some(object) = self.read_loose_object_body(hash)? {
            self.object_cache.insert(hash.to_string(), object.clone());
            return Ok(object);
        }

        bail!("object not found: {hash}")
    }

    fn pack_dir(&self) -> PathBuf {
        self.git_dir.join(GIT_OBJECTS_DIR).join(GIT_PACK_DIR)
    }

    fn read_loose_object_body(&self, hash: &str) -> Result<Option<(ObjectType, Vec<u8>)>> {
        let path = self.object_path(hash)?;
        if !path.exists() {
            return Ok(None);
        }

        let compressed = fs::read(path)?;
        let mut decoder = ZlibDecoder::new(compressed.as_slice());
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(Some(ObjectStore::parse_object_body(&decompressed)?))
    }

    fn object_path(&self, hash: &str) -> Result<PathBuf> {
        if hash.len() != 40 {
            bail!("object hash must be 40 characters, got {}", hash.len());
        }
        let prefix = &hash[..2];
        let suffix = &hash[2..];
        Ok(self.git_dir.join(GIT_OBJECTS_DIR).join(prefix).join(suffix))
    }

    fn find_packed_object(&mut self, hash: &[u8; 20]) -> Result<Option<PackObjectLocation>> {
        self.ensure_pack_indices_loaded()?;
        let indices = self.pack_indices.as_ref().unwrap();
        for index in indices {
            if let Some(location) = index.find(hash) {
                return Ok(Some(location));
            }
        }
        Ok(None)
    }

    fn ensure_pack_indices_loaded(&mut self) -> Result<()> {
        if self.pack_indices.is_some() {
            return Ok(());
        }

        let pack_dir = self.pack_dir();
        let mut indices = Vec::new();
        if pack_dir.is_dir() {
            for entry in fs::read_dir(pack_dir)? {
                let entry = entry?;
                let path = entry.path();
                if path.extension().and_then(|ext| ext.to_str()) == Some("idx") {
                    indices.push(PackIndex::read(path)?);
                }
            }
        }
        self.pack_indices = Some(indices);
        Ok(())
    }

    fn resolve_packed_object_at(&mut self, pack_path: &Path, offset: u64) -> Result<PackedObject> {
        let cache_key = (pack_path.to_path_buf(), offset);
        if let Some(object) = self.offset_cache.get(&cache_key).cloned() {
            return Ok(object);
        }

        let data = self.read_pack_data(pack_path)?;
        let entry = PackEntry::from_packed_object_at(&data, offset as usize)?;
        let object = match entry.kind {
            PackEntryKind::Base { object_type, body } => PackedObject {
                hash: ObjectStore::object_hash(object_type, &body),
                object_type,
                body,
            },
            PackEntryKind::OfsDelta { base_offset, delta } => {
                let PackedObject {
                    object_type,
                    body: base_body,
                    ..
                } = self.resolve_packed_object_at(pack_path, base_offset as u64)?;
                let body = super::delta::apply_delta_with_interrupt(&base_body, &delta, || Ok(()))?;
                PackedObject {
                    hash: ObjectStore::object_hash(object_type, &body),
                    object_type,
                    body,
                }
            }
            PackEntryKind::RefDelta { base_hash, delta } => {
                let (object_type, base_body) = self.read_object_body(&base_hash)?;
                let body = super::delta::apply_delta_with_interrupt(&base_body, &delta, || Ok(()))?;
                PackedObject {
                    hash: ObjectStore::object_hash(object_type, &body),
                    object_type,
                    body,
                }
            }
        };

        self.offset_cache.insert(cache_key, object.clone());
        self.object_cache.insert(
            object.hash.clone(),
            (object.object_type, object.body.clone()),
        );
        Ok(object)
    }

    fn read_pack_data(&mut self, pack_path: &Path) -> Result<Vec<u8>> {
        if let Some(data) = self.pack_data.get(pack_path).cloned() {
            return Ok(data);
        }

        let data = fs::read(pack_path)?;
        self.pack_data.insert(pack_path.to_path_buf(), data.clone());
        Ok(data)
    }
}

impl PackEntry {
    pub(crate) fn from_packed_object_at(data: &[u8], offset: usize) -> Result<PackEntry> {
        let (type_id, header_len) = Self::parse_object_header(&data[offset..])?;
        let mut cursor = offset + header_len;
        match type_id {
            1..=3 => {
                let object_type = match type_id {
                    1 => ObjectType::Commit,
                    2 => ObjectType::Tree,
                    _ => ObjectType::Blob,
                };
                let body = Self::inflate_object(data, cursor)?;
                Ok(PackEntry {
                    kind: PackEntryKind::Base { object_type, body },
                })
            }
            6 => {
                let (distance, used) = Self::parse_ofs_delta_base(&data[cursor..])?;
                cursor += used;
                let delta = Self::inflate_object(data, cursor)?;
                Ok(PackEntry {
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
                let delta = Self::inflate_object(data, cursor)?;
                Ok(PackEntry {
                    kind: PackEntryKind::RefDelta { base_hash, delta },
                })
            }
            other => bail!("unsupported pack object type: {}", other),
        }
    }

    pub(crate) fn into_body(self) -> Result<Vec<u8>> {
        match self.kind {
            PackEntryKind::Base { body, .. } => Ok(body),
            PackEntryKind::OfsDelta { .. } | PackEntryKind::RefDelta { .. } => {
                bail!("expected base pack object")
            }
        }
    }

    pub(crate) fn into_delta(self) -> Result<Vec<u8>> {
        match self.kind {
            PackEntryKind::OfsDelta { delta, .. } | PackEntryKind::RefDelta { delta, .. } => {
                Ok(delta)
            }
            PackEntryKind::Base { .. } => bail!("expected delta pack object"),
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

    fn inflate_object(data: &[u8], start: usize) -> Result<Vec<u8>> {
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

        Ok(output)
    }
}
