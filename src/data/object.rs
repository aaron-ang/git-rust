use std::cell::RefCell;
use std::collections::HashMap;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow, bail};
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
use sha1::{Digest, Sha1};
use strum::{Display, EnumString};

use crate::pack::{
    access::{PackIndex, read_packed_object_at},
    delta::apply_delta_with_interrupt,
    types::{PackEntryKind, PackObjectLocation},
};

type OffsetCache = HashMap<(PathBuf, u64), PackedObject>;

pub const GIT_DIR: &str = ".git";
pub const GIT_OBJECTS_DIR: &str = "objects";
pub const GIT_PACK_DIR: &str = "pack";
pub const GIT_REFS_DIR: &str = "refs";
pub const GIT_HEAD_FILE: &str = "HEAD";
pub const GIT_HEAD_CONTENT: &str = "ref: refs/heads/main\n";

#[derive(Debug, Clone, Copy, PartialEq, Display, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum ObjectType {
    Blob,
    Tree,
    Commit,
}

#[derive(Clone)]
struct PackedObject {
    hash: String,
    object_type: ObjectType,
    body: Vec<u8>,
}

pub struct ObjectStore {
    git_dir: PathBuf,
    pack_indices: RefCell<Option<Vec<PackIndex>>>,
    pack_data: RefCell<HashMap<PathBuf, Vec<u8>>>,
    object_cache: RefCell<HashMap<String, (ObjectType, Vec<u8>)>>,
    offset_cache: RefCell<OffsetCache>,
}

impl Default for ObjectStore {
    fn default() -> Self {
        Self::new(PathBuf::from(GIT_DIR))
    }
}

impl ObjectStore {
    pub fn new(git_dir: PathBuf) -> Self {
        Self {
            git_dir,
            pack_indices: RefCell::new(None),
            pack_data: RefCell::new(HashMap::new()),
            object_cache: RefCell::new(HashMap::new()),
            offset_cache: RefCell::new(HashMap::new()),
        }
    }

    pub(crate) fn git_dir(&self) -> &Path {
        &self.git_dir
    }

    pub(crate) fn pack_dir(&self) -> PathBuf {
        self.git_dir.join(GIT_OBJECTS_DIR).join(GIT_PACK_DIR)
    }

    pub fn object_path(&self, hash: &str) -> Result<PathBuf> {
        if hash.len() != 40 {
            bail!("object hash must be 40 characters, got {}", hash.len());
        }
        let prefix = &hash[..2];
        let suffix = &hash[2..];
        Ok(self.git_dir.join(GIT_OBJECTS_DIR).join(prefix).join(suffix))
    }

    pub fn write_object(&self, object_type: ObjectType, body: &[u8]) -> Result<String> {
        let payload = Self::serialize_object(object_type, body);
        let hash = Self::hash_payload(&payload);
        self.write_payload(&hash, &payload)?;
        Ok(hash)
    }

    pub fn read_object(&self, hash: &str) -> Result<Vec<u8>> {
        if let Some(payload) = self.read_loose_object(hash)? {
            return Ok(payload);
        }

        let (object_type, body) = self.read_packed_object_body(hash)?;
        Ok(Self::serialize_object(object_type, &body))
    }

    pub fn read_object_body(&self, hash: &str) -> Result<(ObjectType, Vec<u8>)> {
        if let Some(payload) = self.read_loose_object(hash)? {
            return Self::parse_object_body(&payload);
        }

        self.read_packed_object_body(hash)
    }

    pub(crate) fn hash_bytes_to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }

    pub(crate) fn hash_hex_to_bytes(hash: &str) -> Result<Vec<u8>> {
        if hash.len() != 40 {
            bail!("object hash must be 40 characters, got {}", hash.len());
        }
        (0..20)
            .map(|i| {
                u8::from_str_radix(&hash[i * 2..i * 2 + 2], 16)
                    .map_err(|_| anyhow!("object hash contains non-hex characters"))
            })
            .collect()
    }

    pub(crate) fn object_hash(object_type: ObjectType, body: &[u8]) -> String {
        let payload = Self::serialize_object(object_type, body);
        Self::hash_payload(&payload)
    }

    fn read_loose_object(&self, hash: &str) -> Result<Option<Vec<u8>>> {
        let path = self.object_path(hash)?;
        if !path.exists() {
            return Ok(None);
        }

        let compressed = fs::read(path)?;
        let mut decoder = ZlibDecoder::new(compressed.as_slice());
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(Some(decompressed))
    }

    fn read_packed_object_body(&self, hash: &str) -> Result<(ObjectType, Vec<u8>)> {
        if let Some(cached) = self.object_cache.borrow().get(hash).cloned() {
            return Ok(cached);
        }

        let hash_bytes = Self::hash_hex_to_bytes(hash)?;
        let hash_bytes: [u8; 20] = hash_bytes.try_into().unwrap();
        let location = self
            .find_packed_object(&hash_bytes)?
            .ok_or_else(|| anyhow!("object not found: {hash}"))?;
        let object = self.resolve_packed_object_at(&location.pack_path, location.offset)?;
        self.object_cache
            .borrow_mut()
            .insert(hash.to_string(), (object.object_type, object.body.clone()));
        Ok((object.object_type, object.body))
    }

    fn find_packed_object(&self, hash: &[u8; 20]) -> Result<Option<PackObjectLocation>> {
        self.ensure_pack_indices_loaded()?;
        let pack_indices = self.pack_indices.borrow();
        let indices = pack_indices.as_ref().unwrap();
        for index in indices {
            if let Some(location) = index.find(hash) {
                return Ok(Some(location));
            }
        }
        Ok(None)
    }

    fn ensure_pack_indices_loaded(&self) -> Result<()> {
        if self.pack_indices.borrow().is_some() {
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
        *self.pack_indices.borrow_mut() = Some(indices);
        Ok(())
    }

    fn resolve_packed_object_at(&self, pack_path: &Path, offset: u64) -> Result<PackedObject> {
        let cache_key = (pack_path.to_path_buf(), offset);
        if let Some(object) = self.offset_cache.borrow().get(&cache_key).cloned() {
            return Ok(object);
        }

        let data = self.read_pack_data(pack_path)?;
        let entry = read_packed_object_at(&data, offset as usize)?;
        let object = match entry.kind {
            PackEntryKind::Base { object_type, body } => PackedObject {
                hash: Self::object_hash(object_type, &body),
                object_type,
                body,
            },
            PackEntryKind::OfsDelta { base_offset, delta } => {
                let PackedObject {
                    object_type,
                    body: base_body,
                    ..
                } = self.resolve_packed_object_at(pack_path, base_offset as u64)?;
                let body = apply_delta_with_interrupt(&base_body, &delta, || Ok(()))?;
                PackedObject {
                    hash: Self::object_hash(object_type, &body),
                    object_type,
                    body,
                }
            }
            PackEntryKind::RefDelta { base_hash, delta } => {
                let (object_type, base_body) = self.read_object_body(&base_hash)?;
                let body = apply_delta_with_interrupt(&base_body, &delta, || Ok(()))?;
                PackedObject {
                    hash: Self::object_hash(object_type, &body),
                    object_type,
                    body,
                }
            }
        };

        self.offset_cache
            .borrow_mut()
            .insert(cache_key, object.clone());
        self.object_cache.borrow_mut().insert(
            object.hash.clone(),
            (object.object_type, object.body.clone()),
        );
        Ok(object)
    }

    fn read_pack_data(&self, pack_path: &Path) -> Result<Vec<u8>> {
        if let Some(data) = self.pack_data.borrow().get(pack_path).cloned() {
            return Ok(data);
        }

        let data = fs::read(pack_path)?;
        self.pack_data
            .borrow_mut()
            .insert(pack_path.to_path_buf(), data.clone());
        Ok(data)
    }

    fn write_payload(&self, hash: &str, payload: &[u8]) -> Result<()> {
        let path = self.object_path(hash)?;
        if path.exists() {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(payload)?;
        let compressed = encoder.finish()?;
        fs::write(path, compressed)?;
        Ok(())
    }

    fn parse_object_body(payload: &[u8]) -> Result<(ObjectType, Vec<u8>)> {
        let null_pos = payload
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| anyhow!("invalid object: missing null byte after header"))?;

        let header = std::str::from_utf8(&payload[..null_pos])
            .map_err(|_| anyhow!("invalid object: header is not UTF-8"))?;
        let (kind, size) = header
            .split_once(' ')
            .ok_or_else(|| anyhow!("invalid object: malformed header"))?;
        let object_type = kind
            .parse::<ObjectType>()
            .map_err(|_| anyhow!("invalid object: unsupported type: {kind}"))?;

        let body = payload[null_pos + 1..].to_vec();
        let expected_size: usize = size
            .parse()
            .map_err(|_| anyhow!("invalid object: size is not a number"))?;
        if body.len() != expected_size {
            bail!(
                "invalid object: size mismatch, header says {}, body is {}",
                expected_size,
                body.len()
            );
        }
        Ok((object_type, body))
    }

    fn serialize_object(object_type: ObjectType, body: &[u8]) -> Vec<u8> {
        let mut payload = format!("{} {}\0", object_type, body.len()).into_bytes();
        payload.extend_from_slice(body);
        payload
    }

    fn hash_payload(payload: &[u8]) -> String {
        let digest = Sha1::digest(payload);
        format!("{digest:x}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::blob::Blob;
    use crate::pack::index::index_pack;
    use crate::pack::stream::PackStream;
    use tempfile::tempdir;

    #[test]
    fn test_object_path() {
        let store = ObjectStore::default();
        let hash = "e88f7a929cd70b0274c4ea33b209c97fa845fdbc";
        let path = store.object_path(hash).unwrap();
        assert_eq!(
            path,
            PathBuf::from(".git/objects/e8/8f7a929cd70b0274c4ea33b209c97fa845fdbc")
        );
    }

    #[test]
    fn test_object_path_invalid_length() {
        let store = ObjectStore::default();
        assert!(store.object_path("short").is_err());
        assert!(store.object_path("a".repeat(39).as_str()).is_err());
        assert!(store.object_path("a".repeat(41).as_str()).is_err());
    }

    #[test]
    fn test_write_object_is_idempotent_and_stored_correctly() {
        let temp = tempdir().unwrap();
        let store = ObjectStore::new(temp.path().join(GIT_DIR));

        let hash = store
            .write_object(ObjectType::Blob, b"hello world\n")
            .unwrap();
        let payload = store.read_object(&hash).unwrap();
        let second_hash = store
            .write_object(ObjectType::Blob, b"hello world\n")
            .unwrap();

        assert_eq!(hash, second_hash);
        assert_eq!(payload, b"blob 12\0hello world\n");
    }

    #[test]
    fn test_read_object_body_from_pack() {
        let temp = tempdir().unwrap();
        let store = ObjectStore::new(temp.path().join(GIT_DIR));
        let hash = Blob::write_content_in(&store, b"hello world\n").unwrap();
        let loose = store.read_object(&hash).unwrap();
        let mut stream = PackStream::default();
        let pack = build_pack_from_payload(&loose);
        stream.append(&pack).unwrap();
        let parsed = stream.finish().unwrap();
        index_pack(&store, &parsed, |_| Ok(()), || Ok(())).unwrap();
        fs::remove_file(store.object_path(&hash).unwrap()).unwrap();

        let (object_type, body) = store.read_object_body(&hash).unwrap();
        assert_eq!(object_type, ObjectType::Blob);
        assert_eq!(body, b"hello world\n");
    }

    fn build_pack_from_payload(payload: &[u8]) -> Vec<u8> {
        let mut pack = Vec::from(b"PACK\x00\x00\x00\x02\x00\x00\x00\x01".as_slice());
        let body = payload.split(|byte| *byte == 0).nth(1).unwrap();
        let size = body.len() as u8;
        pack.push(0x30 | size);

        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(body).unwrap();
        pack.extend_from_slice(&encoder.finish().unwrap());
        let digest = Sha1::digest(&pack);
        pack.extend_from_slice(&digest);
        pack
    }
}
