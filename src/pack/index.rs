use std::collections::HashMap;
use std::fs;

use anyhow::{Result, anyhow, bail};
use crc32fast::hash as crc32;
use sha1::{Digest, Sha1};

use crate::data::object::ObjectStore;
use super::types::{
    IndexedObject, PackEntry, PackEntryInfo, PackEntryInfoKind, ParsedPack, ResolvedObject,
    UnpackProgress, UnpackStats,
};

pub(crate) fn index_pack<F, C>(
    _store: &ObjectStore,
    pack: &ParsedPack,
    mut on_progress: F,
    mut check_interrupt: C,
) -> Result<UnpackStats>
where
    F: FnMut(UnpackProgress) -> Result<()>,
    C: FnMut() -> Result<()>,
{
    let pack_data = fs::read(&pack.pack_path)?;
    if pack_data.len() < 20 {
        bail!("pack file too short");
    }

    let indexed =
        resolve_indexed_objects(&pack_data, pack, &mut on_progress, &mut check_interrupt)?;
    fs::write(
        pack.pack_path.with_extension("idx"),
        build_pack_index(&indexed, &pack.pack_checksum)?,
    )?;

    let total_deltas = pack
        .entries
        .iter()
        .filter(|entry| {
            matches!(
                entry.kind,
                PackEntryInfoKind::OfsDelta { .. } | PackEntryInfoKind::RefDelta { .. }
            )
        })
        .count();
    Ok(UnpackStats {
        objects: pack.entries.len(),
        deltas: total_deltas,
        pack_bytes: pack.pack_bytes,
    })
}

fn resolve_indexed_objects<F, C>(
    pack_data: &[u8],
    pack: &ParsedPack,
    on_progress: &mut F,
    check_interrupt: &mut C,
) -> Result<Vec<IndexedObject>>
where
    F: FnMut(UnpackProgress) -> Result<()>,
    C: FnMut() -> Result<()>,
{
    let entries = &pack.entries;
    let total_deltas = entries
        .iter()
        .filter(|entry| {
            matches!(
                entry.kind,
                PackEntryInfoKind::OfsDelta { .. } | PackEntryInfoKind::RefDelta { .. }
            )
        })
        .count();

    let offset_to_index: HashMap<usize, usize> = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.offset, idx))
        .collect();

    let mut base_hashes = HashMap::new();
    for (idx, entry) in entries.iter().enumerate() {
        if let PackEntryInfoKind::Base { object_type } = entry.kind {
            let body = PackEntry::from_packed_object_at(pack_data, entry.offset)?.into_body()?;
            let hash = ObjectStore::object_hash(object_type, &body);
            base_hashes.insert(hash, idx);
        }
    }

    let mut resolved = HashMap::new();
    let mut indexed = Vec::with_capacity(entries.len());
    let mut resolved_deltas = 0usize;
    for (entry_index, entry) in entries.iter().enumerate() {
        check_interrupt()?;
        let object = resolve_entry(
            pack_data,
            entry_index,
            entries,
            &offset_to_index,
            &base_hashes,
            &mut resolved,
            check_interrupt,
        )?;
        if matches!(
            entry.kind,
            PackEntryInfoKind::OfsDelta { .. } | PackEntryInfoKind::RefDelta { .. }
        ) {
            resolved_deltas += 1;
        }
        on_progress(UnpackProgress {
            received_objects: entry_index + 1,
            total_objects: entries.len(),
            resolved_deltas,
            total_deltas,
        })?;
        let hash = ObjectStore::hash_hex_to_bytes(&object.hash)?;
        indexed.push(IndexedObject {
            hash: hash.try_into().unwrap(),
            offset: entry.offset as u64,
            crc32: crc32(&pack_data[entry.offset..entry.end_offset]),
        });
    }

    indexed.sort_by_key(|object| object.hash);
    Ok(indexed)
}

fn resolve_entry<C>(
    pack_data: &[u8],
    index: usize,
    entries: &[PackEntryInfo],
    offset_to_index: &HashMap<usize, usize>,
    base_hashes: &HashMap<String, usize>,
    resolved: &mut HashMap<usize, ResolvedObject>,
    check_interrupt: &mut C,
) -> Result<ResolvedObject>
where
    C: FnMut() -> Result<()>,
{
    check_interrupt()?;
    if let Some(object) = resolved.get(&index) {
        return Ok(object.clone());
    }

    let packed = PackEntry::from_packed_object_at(pack_data, entries[index].offset)?;
    let object = match &entries[index].kind {
        PackEntryInfoKind::Base { object_type } => {
            let body = packed.into_body()?;
            ResolvedObject {
                hash: ObjectStore::object_hash(*object_type, &body),
                object_type: *object_type,
                body,
            }
        }
        PackEntryInfoKind::OfsDelta { base_offset } => {
            let base_index = *offset_to_index
                .get(base_offset)
                .ok_or_else(|| anyhow!("missing ofs-delta base object"))?;
            let base = resolve_entry(
                pack_data,
                base_index,
                entries,
                offset_to_index,
                base_hashes,
                resolved,
                check_interrupt,
            )?;
            let delta = packed.into_delta()?;
            let body = super::delta::apply_delta_with_interrupt(
                &base.body,
                &delta,
                &mut *check_interrupt,
            )?;
            ResolvedObject {
                hash: ObjectStore::object_hash(base.object_type, &body),
                object_type: base.object_type,
                body,
            }
        }
        PackEntryInfoKind::RefDelta { base_hash } => {
            let base_index = *base_hashes
                .get(base_hash)
                .ok_or_else(|| anyhow!("missing ref-delta base object"))?;
            let base = resolve_entry(
                pack_data,
                base_index,
                entries,
                offset_to_index,
                base_hashes,
                resolved,
                check_interrupt,
            )?;
            let delta = packed.into_delta()?;
            let body = super::delta::apply_delta_with_interrupt(
                &base.body,
                &delta,
                &mut *check_interrupt,
            )?;
            ResolvedObject {
                hash: ObjectStore::object_hash(base.object_type, &body),
                object_type: base.object_type,
                body,
            }
        }
    };

    resolved.insert(index, object.clone());
    Ok(object)
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
