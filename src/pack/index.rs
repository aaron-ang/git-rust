use std::collections::HashMap;
use std::fs;

use anyhow::{Result, anyhow, bail};
use crc32fast::hash as crc32;
use sha1::{Digest, Sha1};

use crate::data::object::{ObjectStore, ObjectType};

use super::types::{
    IndexedObject, PackEntry, PackEntryInfoKind, ParsedPack, ResolvedObject, UnpackProgress,
    UnpackStats,
};

pub(crate) trait PackIndexObserver {
    fn check_interrupt(&self) -> Result<()>;
    fn on_progress(&self, progress: UnpackProgress) -> Result<()>;
}

pub(crate) fn index_pack<O>(
    _store: &ObjectStore,
    pack: &ParsedPack,
    observer: &O,
) -> Result<UnpackStats>
where
    O: PackIndexObserver,
{
    PackIndexer::new(pack, observer)?.index()
}

struct PackIndexer<'a, O>
where
    O: PackIndexObserver,
{
    pack: &'a ParsedPack,
    pack_data: Vec<u8>,
    offset_to_index: HashMap<usize, usize>,
    base_hashes: HashMap<String, usize>,
    children_remaining: Vec<usize>,
    resolved: HashMap<usize, CachedResolvedObject>,
    observer: &'a O,
}

impl<'a, O> PackIndexer<'a, O>
where
    O: PackIndexObserver,
{
    fn new(pack: &'a ParsedPack, observer: &'a O) -> Result<Self> {
        let pack_data = fs::read(&pack.pack_path)?;
        if pack_data.len() < 20 {
            bail!("pack file too short");
        }

        let offset_to_index = pack
            .entries
            .iter()
            .enumerate()
            .map(|(idx, entry)| (entry.offset, idx))
            .collect();

        let mut base_hashes = HashMap::new();
        for (idx, entry) in pack.entries.iter().enumerate() {
            if let PackEntryInfoKind::Base { object_type } = entry.kind {
                let body =
                    PackEntry::from_packed_object_at(&pack_data, entry.offset)?.into_body()?;
                let hash = ObjectStore::object_hash(object_type, &body);
                base_hashes.insert(hash, idx);
            }
        }

        let mut children_remaining = vec![0; pack.entries.len()];
        for entry in &pack.entries {
            if let Some(parent_index) = Self::parent_index(entry, &offset_to_index, &base_hashes)? {
                children_remaining[parent_index] += 1;
            }
        }

        Ok(Self {
            pack,
            pack_data,
            offset_to_index,
            base_hashes,
            children_remaining,
            resolved: HashMap::new(),
            observer,
        })
    }

    fn index(mut self) -> Result<UnpackStats> {
        let indexed = self.resolve_indexed_objects()?;
        self.observer.check_interrupt()?;
        fs::write(
            self.pack.pack_path.with_extension("idx"),
            Self::build_pack_index(&indexed, &self.pack.pack_checksum)?,
        )?;

        Ok(UnpackStats {
            objects: self.pack.entries.len(),
            deltas: self.total_deltas(),
            pack_bytes: self.pack.pack_bytes,
        })
    }

    fn resolve_indexed_objects(&mut self) -> Result<Vec<IndexedObject>> {
        let mut indexed = Vec::with_capacity(self.pack.entries.len());
        let mut resolved_deltas = 0usize;
        let total_deltas = self.total_deltas();
        let total_objects = self.pack.entries.len();

        for (entry_index, entry) in self.pack.entries.iter().enumerate() {
            self.observer.check_interrupt()?;
            let object = self.resolve_entry(entry_index)?;
            if matches!(
                entry.kind,
                PackEntryInfoKind::OfsDelta { .. } | PackEntryInfoKind::RefDelta { .. }
            ) {
                resolved_deltas += 1;
            }
            self.observer.on_progress(UnpackProgress {
                received_objects: entry_index + 1,
                total_objects,
                resolved_deltas,
                total_deltas,
            })?;
            let hash = ObjectStore::hash_hex_to_bytes(&object.hash)?;
            indexed.push(IndexedObject {
                hash: hash.try_into().unwrap(),
                offset: entry.offset as u64,
                crc32: crc32(&self.pack_data[entry.offset..entry.end_offset]),
            });
            self.drop_body_if_unused(entry_index);
        }

        indexed.sort_by_key(|object| object.hash);
        Ok(indexed)
    }

    fn resolve_entry(&mut self, index: usize) -> Result<ResolvedObject> {
        self.observer.check_interrupt()?;
        if let Some(object) = self.resolved.get(&index) {
            return object.materialize();
        }

        let packed =
            PackEntry::from_packed_object_at(&self.pack_data, self.pack.entries[index].offset)?;
        let object = match &self.pack.entries[index].kind {
            PackEntryInfoKind::Base { object_type } => {
                let body = packed.into_body()?;
                ResolvedObject {
                    hash: ObjectStore::object_hash(*object_type, &body),
                    object_type: *object_type,
                    body,
                }
            }
            PackEntryInfoKind::OfsDelta { base_offset } => {
                let base_index = *self
                    .offset_to_index
                    .get(base_offset)
                    .ok_or_else(|| anyhow!("missing ofs-delta base object"))?;
                let base = self.resolve_entry(base_index)?;
                let delta = packed.into_delta()?;
                let body = super::delta::apply_delta_with_interrupt(
                    &base.body,
                    &delta,
                    || self.observer.check_interrupt(),
                )?;
                self.release_parent_body(base_index);
                ResolvedObject {
                    hash: ObjectStore::object_hash(base.object_type, &body),
                    object_type: base.object_type,
                    body,
                }
            }
            PackEntryInfoKind::RefDelta { base_hash } => {
                let base_index = *self
                    .base_hashes
                    .get(base_hash)
                    .ok_or_else(|| anyhow!("missing ref-delta base object"))?;
                let base = self.resolve_entry(base_index)?;
                let delta = packed.into_delta()?;
                let body = super::delta::apply_delta_with_interrupt(
                    &base.body,
                    &delta,
                    || self.observer.check_interrupt(),
                )?;
                self.release_parent_body(base_index);
                ResolvedObject {
                    hash: ObjectStore::object_hash(base.object_type, &body),
                    object_type: base.object_type,
                    body,
                }
            }
        };

        self.resolved
            .insert(index, CachedResolvedObject::from(&object));
        Ok(object)
    }

    fn total_deltas(&self) -> usize {
        self.pack
            .entries
            .iter()
            .filter(|entry| {
                matches!(
                    entry.kind,
                    PackEntryInfoKind::OfsDelta { .. } | PackEntryInfoKind::RefDelta { .. }
                )
            })
            .count()
    }

    fn parent_index(
        entry: &super::types::PackEntryInfo,
        offset_to_index: &HashMap<usize, usize>,
        base_hashes: &HashMap<String, usize>,
    ) -> Result<Option<usize>> {
        match &entry.kind {
            PackEntryInfoKind::Base { .. } => Ok(None),
            PackEntryInfoKind::OfsDelta { base_offset } => Ok(Some(
                *offset_to_index
                    .get(base_offset)
                    .ok_or_else(|| anyhow!("missing ofs-delta base object"))?,
            )),
            PackEntryInfoKind::RefDelta { base_hash } => Ok(Some(
                *base_hashes
                    .get(base_hash)
                    .ok_or_else(|| anyhow!("missing ref-delta base object"))?,
            )),
        }
    }

    fn release_parent_body(&mut self, parent_index: usize) {
        let remaining = &mut self.children_remaining[parent_index];
        if *remaining == 0 {
            return;
        }
        *remaining -= 1;
        self.drop_body_if_unused(parent_index);
    }

    fn drop_body_if_unused(&mut self, index: usize) {
        if self.children_remaining[index] != 0 {
            return;
        }
        if let Some(object) = self.resolved.get_mut(&index) {
            object.body = None;
        }
    }

    fn build_pack_index(objects: &[IndexedObject], pack_checksum: &[u8; 20]) -> Result<Vec<u8>> {
        let mut idx = Vec::new();
        idx.extend_from_slice(&[0xff, b't', b'O', b'c']);
        idx.extend_from_slice(&2u32.to_be_bytes());

        let mut fanout = [0u32; 256];
        for object in objects {
            fanout[object.hash[0] as usize] += 1;
        }
        for idx_pos in 1..fanout.len() {
            fanout[idx_pos] += fanout[idx_pos - 1];
        }
        for count in fanout {
            idx.extend_from_slice(&count.to_be_bytes());
        }

        for object in objects {
            idx.extend_from_slice(&object.hash);
        }
        for object in objects {
            idx.extend_from_slice(&object.crc32.to_be_bytes());
        }

        let mut large_offsets = Vec::new();
        for object in objects {
            if object.offset < 0x8000_0000 {
                idx.extend_from_slice(&(object.offset as u32).to_be_bytes());
            } else {
                let table_index = large_offsets.len() as u32;
                idx.extend_from_slice(&(0x8000_0000 | table_index).to_be_bytes());
                large_offsets.push(object.offset);
            }
        }

        for offset in large_offsets {
            idx.extend_from_slice(&offset.to_be_bytes());
        }

        idx.extend_from_slice(pack_checksum);
        let idx_checksum = Sha1::digest(&idx);
        idx.extend_from_slice(&idx_checksum);
        Ok(idx)
    }
}

#[derive(Clone)]
struct CachedResolvedObject {
    hash: String,
    object_type: ObjectType,
    body: Option<Vec<u8>>,
}

impl CachedResolvedObject {
    fn materialize(&self) -> Result<ResolvedObject> {
        let body = self
            .body
            .clone()
            .ok_or_else(|| anyhow!("resolved pack body evicted too early"))?;
        Ok(ResolvedObject {
            hash: self.hash.clone(),
            object_type: self.object_type,
            body,
        })
    }
}

impl From<&ResolvedObject> for CachedResolvedObject {
    fn from(value: &ResolvedObject) -> Self {
        Self {
            hash: value.hash.clone(),
            object_type: value.object_type,
            body: Some(value.body.clone()),
        }
    }
}
