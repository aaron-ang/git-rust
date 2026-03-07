use std::io::{Read, Write};
use std::path::PathBuf;

use anyhow::{Result, anyhow, bail};
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
use sha1::{Digest, Sha1};
use strum::{Display, EnumString};

pub const GIT_DIR: &str = ".git";
pub const GIT_OBJECTS_DIR: &str = "objects";
pub const GIT_REFS_DIR: &str = "refs";
pub const GIT_HEAD_FILE: &str = "HEAD";
pub const GIT_HEAD_CONTENT: &str = "ref: refs/heads/main\n";

#[derive(Clone, Copy, PartialEq, Display, EnumString)]
#[strum(serialize_all = "lowercase")]
pub enum ObjectType {
    Blob,
    Tree,
    Commit,
}

pub struct ObjectStore {
    git_dir: PathBuf,
}

impl Default for ObjectStore {
    fn default() -> Self {
        Self::new(PathBuf::from(GIT_DIR))
    }
}

impl ObjectStore {
    pub fn new(git_dir: PathBuf) -> Self {
        Self { git_dir }
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
        let path = self.object_path(hash)?;
        let compressed = std::fs::read(path)?;
        let mut decoder = ZlibDecoder::new(compressed.as_slice());
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Ok(decompressed)
    }

    pub fn read_object_body(&self, hash: &str) -> Result<(ObjectType, Vec<u8>)> {
        let payload = self.read_object(hash)?;
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

    fn write_payload(&self, hash: &str, payload: &[u8]) -> Result<()> {
        let path = self.object_path(hash)?;
        if path.exists() {
            return Ok(());
        }
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(payload)?;
        let compressed = encoder.finish()?;
        std::fs::write(path, compressed)?;
        Ok(())
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
}
