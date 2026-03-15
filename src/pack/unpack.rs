use std::collections::HashMap;

use anyhow::{Result, anyhow};

use crate::object::ObjectStore;

use super::delta::apply_delta_with_interrupt;
use super::parse::Packfile;
use super::types::{
    PackEntry, PackEntryKind, ParsedPack, ResolvedObject, UnpackProgress, UnpackStats,
};

pub fn unpack_into<F, C>(
    store: &ObjectStore,
    pack: &[u8],
    on_progress: F,
    check_interrupt: C,
) -> Result<UnpackStats>
where
    F: FnMut(UnpackProgress) -> Result<()>,
    C: FnMut() -> Result<()>,
{
    let entries = Packfile::new(pack).parse()?;
    unpack_entries(store, &entries, pack.len(), on_progress, check_interrupt)
}

impl ParsedPack {
    pub fn unpack_into<F, C>(
        &self,
        store: &ObjectStore,
        on_progress: F,
        check_interrupt: C,
    ) -> Result<UnpackStats>
    where
        F: FnMut(UnpackProgress) -> Result<()>,
        C: FnMut() -> Result<()>,
    {
        unpack_entries(
            store,
            &self.entries,
            self.pack_bytes,
            on_progress,
            check_interrupt,
        )
    }
}

fn unpack_entries<F, C>(
    store: &ObjectStore,
    entries: &[PackEntry],
    pack_bytes: usize,
    mut on_progress: F,
    mut check_interrupt: C,
) -> Result<UnpackStats>
where
    F: FnMut(UnpackProgress) -> Result<()>,
    C: FnMut() -> Result<()>,
{
    let total_deltas = entries
        .iter()
        .filter(|entry| {
            matches!(
                entry.kind,
                PackEntryKind::OfsDelta { .. } | PackEntryKind::RefDelta { .. }
            )
        })
        .count();
    let stats = UnpackStats {
        objects: entries.len(),
        deltas: total_deltas,
        pack_bytes,
    };
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

    let mut resolved_deltas = 0;
    let total_objects = entries.len();

    for (entry_index, entry) in entries.iter().enumerate() {
        check_interrupt()?;
        resolve_entry(
            entry_index,
            entries,
            &offset_to_index,
            &mut resolved,
            &resolved_hashes,
            store,
            &mut check_interrupt,
        )?;
        if matches!(
            entry.kind,
            PackEntryKind::OfsDelta { .. } | PackEntryKind::RefDelta { .. }
        ) {
            resolved_deltas += 1;
        }
        on_progress(UnpackProgress {
            received_objects: entry_index + 1,
            total_objects,
            resolved_deltas,
            total_deltas,
        })?;
    }

    Ok(stats)
}

fn resolve_entry(
    index: usize,
    entries: &[PackEntry],
    offset_to_index: &HashMap<usize, usize>,
    resolved: &mut HashMap<usize, ResolvedObject>,
    resolved_hashes: &HashMap<String, usize>,
    store: &ObjectStore,
    check_interrupt: &mut impl FnMut() -> Result<()>,
) -> Result<ResolvedObject> {
    check_interrupt()?;
    if let Some(object) = resolved.get(&index) {
        return Ok(object.clone());
    }

    let object = match &entries[index].kind {
        PackEntryKind::Base { object_type, body } => {
            check_interrupt()?;
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
                check_interrupt,
            )?;
            let body = apply_delta_with_interrupt(&base.body, delta, &mut *check_interrupt)?;
            check_interrupt()?;
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
                    check_interrupt,
                )?
            } else {
                check_interrupt()?;
                let (object_type, body) = store.read_object_body(base_hash)?;
                ResolvedObject { object_type, body }
            };

            let body = apply_delta_with_interrupt(&base.body, delta, &mut *check_interrupt)?;
            check_interrupt()?;
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
