use std::path::PathBuf;

use crate::data::object::ObjectType;

pub struct UnpackStats {
    pub objects: usize,
    pub deltas: usize,
    pub pack_bytes: usize,
}

pub struct UnpackProgress {
    pub received_objects: usize,
    pub total_objects: usize,
    pub resolved_deltas: usize,
    pub total_deltas: usize,
}

pub struct PackTransferProgress {
    pub total_objects: Option<usize>,
    pub received_objects: usize,
}

pub struct ParsedPack {
    pub(crate) pack_path: PathBuf,
    pub(crate) pack_checksum: [u8; 20],
    pub(crate) entries: Vec<PackEntryInfo>,
    pub(crate) pack_bytes: usize,
}

pub(crate) enum PackEntryInfoKind {
    Base { object_type: ObjectType },
    OfsDelta { base_offset: usize },
    RefDelta { base_hash: String },
}

pub(crate) struct PackEntryInfo {
    pub(crate) offset: usize,
    pub(crate) end_offset: usize,
    pub(crate) kind: PackEntryInfoKind,
}

pub(crate) enum PackEntryKind {
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

pub(crate) struct PackEntry {
    pub(crate) kind: PackEntryKind,
}

#[derive(Clone)]
pub(crate) struct ResolvedObject {
    pub(crate) hash: String,
    pub(crate) object_type: ObjectType,
    pub(crate) body: Vec<u8>,
}

pub(crate) struct IndexedObject {
    pub(crate) hash: [u8; 20],
    pub(crate) offset: u64,
    pub(crate) crc32: u32,
}

pub(crate) struct PackObjectLocation {
    pub(crate) pack_path: std::path::PathBuf,
    pub(crate) offset: u64,
}
