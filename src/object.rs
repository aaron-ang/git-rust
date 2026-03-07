use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{Result, anyhow};
use flate2::{Compression, read::ZlibDecoder, write::ZlibEncoder};
use sha1::{Digest, Sha1};

pub const GIT_DIR: &str = ".git";
pub const GIT_OBJECTS_DIR: &str = "objects";
pub const GIT_REFS_DIR: &str = "refs";
pub const GIT_HEAD_FILE: &str = "HEAD";
pub const GIT_HEAD_CONTENT: &str = "ref: refs/heads/main\n";

/// Git blob object: file content only (no name or permissions).
#[derive(Debug, Clone)]
pub struct Blob {
    content: Vec<u8>,
}

impl Blob {
    /// Reads a file from disk, stores it as a Git blob object and returns its hash.
    pub fn write_from_path(path: &Path) -> Result<String> {
        let content = std::fs::read(path)?;
        Self::write_content(&content)
    }

    /// Stores content as a Git blob object and returns its hash.
    pub fn write_content(content: &[u8]) -> Result<String> {
        let payload = Self::serialize(content);
        let hash = Self::hash_payload(&payload);
        Self::write_object_payload_in(Path::new(GIT_DIR), &hash, &payload)?;
        Ok(hash)
    }

    /// Reads a blob from the Git object store by hash and returns its content.
    pub fn read(hash: &str) -> Result<Vec<u8>> {
        let path = Self::object_path(hash)?;
        let compressed = std::fs::read(&path)?;
        let mut decoder = ZlibDecoder::new(compressed.as_slice());
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        let blob = Self::parse(&decompressed)?;
        Ok(blob.content)
    }

    /// Parses decompressed blob object bytes.
    /// Format: `blob <size>\0<content>`
    fn parse(data: &[u8]) -> Result<Self> {
        const BLOB_PREFIX: &[u8] = b"blob ";
        if !data.starts_with(BLOB_PREFIX) {
            return Err(anyhow!("invalid blob: expected 'blob' header"));
        }
        let after_prefix = &data[BLOB_PREFIX.len()..];
        let null_pos = after_prefix
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| anyhow!("invalid blob: missing null byte after size"))?;
        let size_str = std::str::from_utf8(&after_prefix[..null_pos])
            .map_err(|_| anyhow!("invalid blob: size is not UTF-8"))?;
        let _size: usize = size_str
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid blob: size is not a number"))?;
        let content = after_prefix[null_pos + 1..].to_vec();
        Ok(Blob { content })
    }

    /// Builds uncompressed blob payload bytes: `blob <size>\0<content>`.
    fn serialize(content: &[u8]) -> Vec<u8> {
        let mut payload = format!("blob {}\0", content.len()).into_bytes();
        payload.extend_from_slice(content);
        payload
    }

    fn hash_payload(payload: &[u8]) -> String {
        let digest = Sha1::digest(payload);
        format!("{digest:x}")
    }

    fn write_object_payload_in(git_dir: &Path, hash: &str, payload: &[u8]) -> Result<()> {
        let path = Self::object_path_in(git_dir, hash)?;
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

    /// Returns the path to a Git object file from its 40-character SHA-1 hash.
    /// Path format: `.git/objects/<first_2_chars>/<remaining_38_chars>`.
    fn object_path(hash: &str) -> Result<PathBuf> {
        Self::object_path_in(Path::new(GIT_DIR), hash)
    }

    fn object_path_in(git_dir: &Path, hash: &str) -> Result<PathBuf> {
        if hash.len() != 40 {
            return Err(anyhow!(
                "object hash must be 40 characters, got {}",
                hash.len()
            ));
        }
        let prefix = &hash[..2];
        let suffix = &hash[2..];
        Ok(git_dir.join(GIT_OBJECTS_DIR).join(prefix).join(suffix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn test_object_path() {
        let hash = "e88f7a929cd70b0274c4ea33b209c97fa845fdbc";
        let path = Blob::object_path(hash).unwrap();
        assert_eq!(
            path,
            PathBuf::from(".git/objects/e8/8f7a929cd70b0274c4ea33b209c97fa845fdbc")
        );
    }

    #[test]
    fn test_object_path_invalid_length() {
        assert!(Blob::object_path("short").is_err());
        assert!(Blob::object_path("a".repeat(39).as_str()).is_err());
        assert!(Blob::object_path("a".repeat(41).as_str()).is_err());
    }

    #[test]
    fn test_parse_blob_hello_world() {
        let data = b"blob 11\0hello world";
        let blob = Blob::parse(data).unwrap();
        assert_eq!(blob.content, b"hello world");
    }

    #[test]
    fn test_parse_blob_no_trailing_newline() {
        let data = b"blob 5\0abcde";
        let blob = Blob::parse(data).unwrap();
        assert_eq!(blob.content, b"abcde");
    }

    #[test]
    fn test_parse_blob_empty_content() {
        let data = b"blob 0\0";
        let blob = Blob::parse(data).unwrap();
        assert_eq!(blob.content, b"");
    }

    #[test]
    fn test_parse_blob_invalid_header() {
        assert!(Blob::parse(b"tree 0\0").is_err());
        assert!(Blob::parse(b"blob").is_err());
    }

    #[test]
    fn test_parse_blob_missing_null() {
        assert!(Blob::parse(b"blob 5 hello").is_err());
    }

    #[test]
    fn test_serialize_blob_payload() {
        let payload = Blob::serialize(b"hello world");
        assert_eq!(payload, b"blob 11\0hello world");
    }

    #[test]
    fn test_hash_payload_hello_world_no_newline() {
        let payload = Blob::serialize(b"hello world");
        let hash = Blob::hash_payload(&payload);
        assert_eq!(hash, "95d09f2b10159347eece71399a7e2e907ea3df4f");
    }

    #[test]
    fn test_hash_payload_hello_world_with_newline() {
        let payload = Blob::serialize(b"hello world\n");
        let hash = Blob::hash_payload(&payload);
        assert_eq!(hash, "3b18e512dba79e4c8300dd08aeb37f8e728b8dad");
    }

    #[test]
    fn test_write_object_payload_is_idempotent_and_stored_correctly() {
        let temp = tempdir().unwrap();
        let git_dir = temp.path().join(GIT_DIR);
        let payload = Blob::serialize(b"hello world\n");
        let hash = Blob::hash_payload(&payload);

        Blob::write_object_payload_in(&git_dir, &hash, &payload).unwrap();
        Blob::write_object_payload_in(&git_dir, &hash, &payload).unwrap();

        let object_path = Blob::object_path_in(&git_dir, &hash).unwrap();
        assert!(object_path.exists());

        let compressed = std::fs::read(object_path).unwrap();
        let mut decoder = ZlibDecoder::new(compressed.as_slice());
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed).unwrap();
        assert_eq!(decompressed, payload);
    }
}
