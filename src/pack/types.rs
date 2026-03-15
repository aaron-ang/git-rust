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
    pub(crate) entries: Vec<PackEntry>,
    pub(crate) pack_bytes: usize,
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
    pub(crate) offset: usize,
    pub(crate) kind: PackEntryKind,
}

#[derive(Clone)]
pub(crate) struct ResolvedObject {
    pub(crate) object_type: ObjectType,
    pub(crate) body: Vec<u8>,
}
