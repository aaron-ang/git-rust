use std::fs;
use anyhow::{Result, bail};
use crc32fast::hash as crc32;
use sha1::{Digest, Sha1};

use crate::data::object::{GIT_OBJECTS_DIR, ObjectStore};

use super::types::{IndexedObject, PackEntry, PackEntryKind, ParsedPack, ResolvedObject, UnpackProgress, UnpackStats};

const PACK_DIR: &str = "pack";

pub(crate) fn index_pack<F, C>(
    store: &ObjectStore,
    pack: &ParsedPack,
    mut on_progress: F,
    mut check_interrupt: C,
) -> Result<UnpackStats>
where
    F: FnMut(UnpackProgress) -> Result<()>,
    C: FnMut() -> Result<()>,
{
    if pack.data.len() < 20 {
        bail!("pack file too short");
    }

    let indexed = resolve_indexed_objects(pack, &mut on_progress, &mut check_interrupt)?;
    let pack_checksum = &pack.data[pack.data.len() - 20..];
    let pack_hash_hex = ObjectStore::hash_bytes_to_hex(pack_checksum);
    let pack_dir = store.git_dir().join(GIT_OBJECTS_DIR).join(PACK_DIR);
    fs::create_dir_all(&pack_dir)?;

    let pack_path = pack_dir.join(format!("pack-{pack_hash_hex}.pack"));
    if !pack_path.exists() {
        fs::write(&pack_path, &pack.data)?;
    }

    let idx_path = pack_dir.join(format!("pack-{pack_hash_hex}.idx"));
    fs::write(idx_path, build_pack_index(&indexed, pack_checksum)?)?;

    let total_deltas = pack
        .entries
        .iter()
        .filter(|entry| matches!(entry.kind, PackEntryKind::OfsDelta { .. } | PackEntryKind::RefDelta { .. }))
        .count();
    Ok(UnpackStats {
        objects: pack.entries.len(),
        deltas: total_deltas,
        pack_bytes: pack.pack_bytes,
    })
}

fn resolve_indexed_objects<F, C>(
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
        .filter(|entry| matches!(entry.kind, PackEntryKind::OfsDelta { .. } | PackEntryKind::RefDelta { .. }))
        .count();

    let offset_to_index = entries
        .iter()
        .enumerate()
        .map(|(idx, entry)| (entry.offset, idx))
        .collect::<std::collections::HashMap<_, _>>();

    let mut base_hashes = std::collections::HashMap::<String, usize>::new();
    let mut resolved = std::collections::HashMap::<usize, ResolvedObject>::new();
    for (idx, entry) in entries.iter().enumerate() {
        if let PackEntryKind::Base { object_type, body } = &entry.kind {
            let hash = ObjectStore::object_hash(*object_type, body);
            base_hashes.insert(hash, idx);
        }
    }

    let mut indexed = Vec::with_capacity(entries.len());
    let mut resolved_deltas = 0usize;
    for (entry_index, entry) in entries.iter().enumerate() {
        check_interrupt()?;
        let object = resolve_entry(
            entry_index,
            entries,
            &offset_to_index,
            &base_hashes,
            &mut resolved,
            check_interrupt,
        )?;
        if matches!(entry.kind, PackEntryKind::OfsDelta { .. } | PackEntryKind::RefDelta { .. }) {
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
            crc32: crc32(&pack.data[entry.offset..entry.end_offset]),
        });
    }

    indexed.sort_by_key(|object| object.hash);
    Ok(indexed)
}

fn resolve_entry<C>(
    index: usize,
    entries: &[PackEntry],
    offset_to_index: &std::collections::HashMap<usize, usize>,
    base_hashes: &std::collections::HashMap<String, usize>,
    resolved: &mut std::collections::HashMap<usize, ResolvedObject>,
    check_interrupt: &mut C,
) -> Result<ResolvedObject>
where
    C: FnMut() -> Result<()>,
{
    check_interrupt()?;
    if let Some(object) = resolved.get(&index) {
        return Ok(object.clone());
    }

    let object = match &entries[index].kind {
        PackEntryKind::Base { object_type, body } => ResolvedObject {
            hash: ObjectStore::object_hash(*object_type, body),
            object_type: *object_type,
            body: body.clone(),
        },
        PackEntryKind::OfsDelta { base_offset, delta } => {
            let base_index = *offset_to_index
                .get(base_offset)
                .ok_or_else(|| anyhow::anyhow!("missing ofs-delta base object"))?;
            let base = resolve_entry(
                base_index,
                entries,
                offset_to_index,
                base_hashes,
                resolved,
                check_interrupt,
            )?;
            let body = super::delta::apply_delta_with_interrupt(&base.body, delta, &mut *check_interrupt)?;
            ResolvedObject {
                hash: ObjectStore::object_hash(base.object_type, &body),
                object_type: base.object_type,
                body,
            }
        }
        PackEntryKind::RefDelta { base_hash, delta } => {
            let base_index = *base_hashes
                .get(base_hash)
                .ok_or_else(|| anyhow::anyhow!("missing ref-delta base object"))?;
            let base = resolve_entry(
                base_index,
                entries,
                offset_to_index,
                base_hashes,
                resolved,
                check_interrupt,
            )?;
            let body = super::delta::apply_delta_with_interrupt(&base.body, delta, &mut *check_interrupt)?;
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

fn build_pack_index(objects: &[IndexedObject], pack_checksum: &[u8]) -> Result<Vec<u8>> {
    if pack_checksum.len() != 20 {
        bail!("invalid pack checksum length");
    }

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
