use std::path::Path;

use anyhow::{Result, anyhow, bail};
use derive_more::{AsRef, Deref};

use crate::object::{ObjectStore, ObjectType};

/// Git blob object: file content only (no name or permissions).
#[derive(Debug, Deref, AsRef)]
#[as_ref(forward)]
pub struct Blob(Vec<u8>);

impl Blob {
    pub fn read(hash: &str) -> Result<Self> {
        Self::read_from(&ObjectStore::default(), hash)
    }

    pub(crate) fn read_from(store: &ObjectStore, hash: &str) -> Result<Self> {
        let data = store.read_object(hash)?;
        Self::parse(&data)
    }

    pub fn write_from_path(path: &Path) -> Result<String> {
        Self::write_from_path_in(&ObjectStore::default(), path)
    }

    pub(crate) fn write_from_path_in(store: &ObjectStore, path: &Path) -> Result<String> {
        let content = std::fs::read(path)?;
        Self::write_content_in(store, &content)
    }

    pub fn write_content(content: &[u8]) -> Result<String> {
        Self::write_content_in(&ObjectStore::default(), content)
    }

    pub(crate) fn write_content_in(store: &ObjectStore, content: &[u8]) -> Result<String> {
        store.write_object(ObjectType::Blob, content)
    }

    /// Parses decompressed blob object bytes.
    /// Format: `blob <size>\0<content>`
    fn parse(data: &[u8]) -> Result<Self> {
        const BLOB_PREFIX: &[u8] = b"blob ";
        if !data.starts_with(BLOB_PREFIX) {
            bail!("invalid blob: expected 'blob' header");
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
        Ok(Self(after_prefix[null_pos + 1..].to_vec()))
    }
}

impl<const N: usize> PartialEq<[u8; N]> for Blob {
    fn eq(&self, other: &[u8; N]) -> bool {
        self.0 == other.as_slice()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::GIT_DIR;

    use tempfile::tempdir;

    const HELLO_WORLD_BLOB_HASH: &str = "95d09f2b10159347eece71399a7e2e907ea3df4f";
    const HELLO_WORLD_NEWLINE_BLOB_HASH: &str = "3b18e512dba79e4c8300dd08aeb37f8e728b8dad";

    fn blob_payload(content: &[u8]) -> Vec<u8> {
        let mut payload = format!("blob {}\0", content.len()).into_bytes();
        payload.extend_from_slice(content);
        payload
    }

    #[test]
    fn test_parse_blob_hello_world() {
        let blob = Blob::parse(b"blob 11\0hello world").unwrap();
        assert_eq!(&blob, b"hello world");
    }

    #[test]
    fn test_parse_blob_no_trailing_newline() {
        let blob = Blob::parse(b"blob 5\0abcde").unwrap();
        assert_eq!(&blob, b"abcde");
    }

    #[test]
    fn test_parse_blob_empty_content() {
        let blob = Blob::parse(b"blob 0\0").unwrap();
        assert_eq!(&blob, b"");
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
    fn test_write_content_known_hashes() {
        let temp = tempdir().unwrap();
        let store = ObjectStore::new(temp.path().join(GIT_DIR));

        let no_newline = Blob::write_content_in(&store, b"hello world").unwrap();
        let with_newline = Blob::write_content_in(&store, b"hello world\n").unwrap();

        assert_eq!(no_newline, HELLO_WORLD_BLOB_HASH);
        assert_eq!(with_newline, HELLO_WORLD_NEWLINE_BLOB_HASH);
    }

    #[test]
    fn test_write_content_stores_expected_payload() {
        let temp = tempdir().unwrap();
        let store = ObjectStore::new(temp.path().join(GIT_DIR));

        let hash = Blob::write_content_in(&store, b"hello world\n").unwrap();
        let payload = store.read_object(&hash).unwrap();

        assert_eq!(payload, blob_payload(b"hello world\n"));
    }
}
