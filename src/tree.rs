use anyhow::{Result, anyhow, bail};
use flate2::read::ZlibDecoder;
use std::{io::Read, ops::Deref};

use crate::object::object_path;

pub struct TreeEntry {
    pub mode: String,
    pub name: String,
    pub hash: String,
}

pub struct Tree(Vec<TreeEntry>);

impl Tree {
    pub fn read(hash: &str) -> Result<Self> {
        let path = object_path(hash)?;
        let compressed = std::fs::read(path)?;
        let mut decoder = ZlibDecoder::new(compressed.as_slice());
        let mut decompressed = Vec::new();
        decoder.read_to_end(&mut decompressed)?;
        Self::parse(&decompressed)
    }

    fn parse(data: &[u8]) -> Result<Self> {
        let (size, mut body) = Self::parse_body(data)?;
        if body.len() != size {
            bail!(
                "invalid tree: size mismatch, header says {size}, body is {}",
                body.len()
            );
        }

        let mut entries = Vec::new();
        while !body.is_empty() {
            let (entry, rest) = Self::parse_entry(body)?;
            entries.push(entry);
            body = rest;
        }

        Ok(Self(entries))
    }

    fn parse_body(data: &[u8]) -> Result<(usize, &[u8])> {
        const TREE_PREFIX: &[u8] = b"tree ";
        if !data.starts_with(TREE_PREFIX) {
            bail!("invalid tree: expected 'tree' header");
        }

        // Tree objects start with `tree <size>\0`, followed by packed entries.
        let after_prefix = &data[TREE_PREFIX.len()..];
        let null_pos = after_prefix
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| anyhow!("invalid tree: missing null byte after size"))?;
        let size_str = std::str::from_utf8(&after_prefix[..null_pos])
            .map_err(|_| anyhow!("invalid tree: size is not UTF-8"))?;
        let size = size_str
            .trim()
            .parse()
            .map_err(|_| anyhow!("invalid tree: size is not a number"))?;

        Ok((size, &after_prefix[null_pos + 1..]))
    }

    fn parse_entry(data: &[u8]) -> Result<(TreeEntry, &[u8])> {
        // Each entry begins with `<mode> <name>\0`.
        let mode_end = data
            .iter()
            .position(|&b| b == b' ')
            .ok_or_else(|| anyhow!("invalid tree entry: missing mode/name separator"))?;
        let mode = std::str::from_utf8(&data[..mode_end])
            .map_err(|_| anyhow!("invalid tree entry: mode is not UTF-8"))?
            .to_string();

        let after_mode = &data[mode_end + 1..];
        let name_end = after_mode
            .iter()
            .position(|&b| b == 0)
            .ok_or_else(|| anyhow!("invalid tree entry: missing null byte after name"))?;
        let name = std::str::from_utf8(&after_mode[..name_end])
            .map_err(|_| anyhow!("invalid tree entry: name is not UTF-8"))?
            .to_string();

        // The object id is stored as 20 raw bytes, not 40 hex characters.
        let after_name = &after_mode[name_end + 1..];
        if after_name.len() < 20 {
            bail!("invalid tree entry: truncated SHA-1 bytes");
        }

        let entry = TreeEntry {
            mode,
            name,
            hash: Self::hash_bytes_to_hex(&after_name[..20]),
        };
        Ok((entry, &after_name[20..]))
    }

    fn hash_bytes_to_hex(bytes: &[u8]) -> String {
        bytes.iter().map(|b| format!("{b:02x}")).collect()
    }
}

impl Deref for Tree {
    type Target = [TreeEntry];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl<'a> IntoIterator for &'a Tree {
    type Item = &'a TreeEntry;
    type IntoIter = std::slice::Iter<'a, TreeEntry>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hex_to_20_bytes(hex: &str) -> Vec<u8> {
        assert_eq!(hex.len(), 40);
        (0..20)
            .map(|i| u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).unwrap())
            .collect()
    }

    fn make_tree_data(entries: &[(&str, &str, &str)]) -> Vec<u8> {
        let mut body = Vec::new();
        for (mode, name, hex_hash) in entries {
            body.extend_from_slice(mode.as_bytes());
            body.push(b' ');
            body.extend_from_slice(name.as_bytes());
            body.push(0);
            body.extend_from_slice(&hex_to_20_bytes(hex_hash));
        }

        let mut data = format!("tree {}\0", body.len()).into_bytes();
        data.extend_from_slice(&body);
        data
    }

    #[test]
    fn test_parse_tree_multiple_entries() {
        let data = make_tree_data(&[
            ("40000", "dir1", "1111111111111111111111111111111111111111"),
            (
                "100644",
                "file1",
                "95d09f2b10159347eece71399a7e2e907ea3df4f",
            ),
        ]);

        let tree = Tree::parse(&data).unwrap();
        assert_eq!(tree.len(), 2);
        assert_eq!(tree[0].mode, "40000");
        assert_eq!(tree[0].name, "dir1");
        assert_eq!(tree[0].hash, "1111111111111111111111111111111111111111");
        assert_eq!(tree[1].mode, "100644");
        assert_eq!(tree[1].name, "file1");
        assert_eq!(tree[1].hash, "95d09f2b10159347eece71399a7e2e907ea3df4f");
    }

    #[test]
    fn test_parse_tree_invalid_header() {
        assert!(Tree::parse(b"blob 0\0").is_err());
    }

    #[test]
    fn test_parse_tree_missing_null_after_name() {
        let mut data = b"tree 31\0100644 file1".to_vec();
        data.extend_from_slice(&hex_to_20_bytes("95d09f2b10159347eece71399a7e2e907ea3df4f"));
        assert!(Tree::parse(&data).is_err());
    }

    #[test]
    fn test_parse_tree_truncated_sha() {
        let mut data = b"tree 16\0100644 file1\0".to_vec();
        data.extend_from_slice(&[0u8; 5]);
        assert!(Tree::parse(&data).is_err());
    }

    #[test]
    fn test_parse_tree_invalid_size_header() {
        assert!(Tree::parse(b"tree no-number\0abc").is_err());
        assert!(Tree::parse(b"tree 7\0abc").is_err());
        assert!(Tree::parse(b"tree 3abc").is_err());
    }
}
