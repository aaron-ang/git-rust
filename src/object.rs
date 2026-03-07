use std::io::Read;
use std::path::PathBuf;

use anyhow::{Result, anyhow};
use flate2::read::ZlibDecoder;

pub const GIT_DIR: &str = ".git";

/// Git blob object: file content only (no name or permissions).
#[derive(Debug, Clone)]
pub struct Blob {
    pub content: Vec<u8>,
}

impl Blob {
    /// Reads a blob from the Git object store by hash and returns its content.
    pub fn read(hash: &str) -> Result<Vec<u8>> {
        let path = Self::object_path(hash)?;
        let compressed =
            std::fs::read(&path).map_err(|e| anyhow!("reading object at {:?}: {e}", path))?;
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
            return Err(anyhow!("invalid blob: expected 'blob ' header"));
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

    /// Returns the path to a Git object file from its 40-character SHA-1 hash.
    /// Path format: `.git/objects/<first_2_chars>/<remaining_38_chars>`.
    fn object_path(hash: &str) -> Result<PathBuf> {
        if hash.len() != 40 {
            return Err(anyhow!(
                "object hash must be 40 characters, got {}",
                hash.len()
            ));
        }
        let prefix = &hash[..2];
        let suffix = &hash[2..];
        Ok(PathBuf::from(GIT_DIR)
            .join("objects")
            .join(prefix)
            .join(suffix))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
