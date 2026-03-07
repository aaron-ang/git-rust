use std::path::Path;

use anyhow::{Result, anyhow, bail};
use derive_more::{Deref, Display};
use strum::{AsRefStr, Display as StrumDisplay, EnumString};

use crate::{
    blob::Blob,
    object::{GIT_DIR, ObjectStore, ObjectType},
};

#[derive(Debug, StrumDisplay, EnumString, AsRefStr, PartialEq)]
enum TreeEntryMode {
    #[strum(serialize = "100644")]
    RegularFile,
    #[strum(serialize = "100755")]
    ExecutableFile,
    #[strum(serialize = "40000")]
    Directory,
}

#[derive(Display)]
#[display("{name}")]
pub struct TreeEntry {
    mode: TreeEntryMode,
    name: String,
    hash: String,
}

#[derive(Deref)]
pub struct Tree(Vec<TreeEntry>);

impl Tree {
    pub fn read(hash: &str) -> Result<Self> {
        Self::read_from(&ObjectStore::default(), hash)
    }

    pub fn write_current_dir() -> Result<String> {
        let cwd = std::env::current_dir()?;
        let store = ObjectStore::default();
        Self::write_dir(&store, &cwd)
    }

    pub fn checkout_in(store: &ObjectStore, tree_sha: &str, root: &Path) -> Result<()> {
        let tree = Self::read_from(store, tree_sha)?;
        for entry in tree.iter() {
            let path = root.join(&entry.name);
            if entry.mode == TreeEntryMode::Directory {
                std::fs::create_dir_all(&path)?;
                Self::checkout_in(store, &entry.hash, &path)?;
                continue;
            }

            let blob = Blob::read_from(store, &entry.hash)?;
            std::fs::write(&path, blob)?;

            #[cfg(unix)]
            if entry.mode == TreeEntryMode::ExecutableFile {
                use std::os::unix::fs::PermissionsExt;

                let mut permissions = std::fs::metadata(&path)?.permissions();
                permissions.set_mode(0o755);
                std::fs::set_permissions(&path, permissions)?;
            }
        }
        Ok(())
    }

    fn read_from(store: &ObjectStore, hash: &str) -> Result<Self> {
        let data = store.read_object(hash)?;
        Self::parse(&data)
    }

    fn write_dir(store: &ObjectStore, dir: &Path) -> Result<String> {
        let mut entries = Vec::new();

        for entry in std::fs::read_dir(dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            if file_name == GIT_DIR {
                continue;
            }

            let path = entry.path();
            let metadata = entry.metadata()?;
            let name = file_name
                .into_string()
                .map_err(|_| anyhow!("invalid UTF-8 path name"))?;

            if metadata.is_file() {
                let hash = Blob::write_from_path_in(store, &path)?;
                entries.push(TreeEntry {
                    mode: TreeEntryMode::RegularFile,
                    name,
                    hash,
                });
            } else if metadata.is_dir() {
                let hash = Self::write_dir(store, &path)?;
                entries.push(TreeEntry {
                    mode: TreeEntryMode::Directory,
                    name,
                    hash,
                });
            } else {
                bail!("unsupported directory entry type: {}", path.display());
            }
        }

        entries.sort_by(|a, b| a.name.cmp(&b.name));
        let tree = Self(entries);
        tree.write(store)
    }

    fn write(&self, store: &ObjectStore) -> Result<String> {
        let body = self.serialize()?;
        store.write_object(ObjectType::Tree, &body)
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

    fn serialize(&self) -> Result<Vec<u8>> {
        let mut body = Vec::new();
        for entry in self.iter() {
            body.extend_from_slice(entry.mode.as_ref().as_bytes());
            body.push(b' ');
            body.extend_from_slice(entry.name.as_bytes());
            body.push(0);
            body.extend_from_slice(&ObjectStore::hash_hex_to_bytes(&entry.hash)?);
        }
        Ok(body)
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
            .parse::<TreeEntryMode>()
            .map_err(|_| anyhow!("invalid tree entry: unsupported mode"))?;

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
            hash: ObjectStore::hash_bytes_to_hex(&after_name[..20]),
        };
        Ok((entry, &after_name[20..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    use crate::object::GIT_OBJECTS_DIR;

    const SAMPLE_TREE_HASH: &str = "1111111111111111111111111111111111111111";
    const HELLO_WORLD_BLOB_HASH: &str = "95d09f2b10159347eece71399a7e2e907ea3df4f";
    const EMPTY_TREE_HASH: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";

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
            ("40000", "dir1", SAMPLE_TREE_HASH),
            ("100644", "file1", HELLO_WORLD_BLOB_HASH),
        ]);

        let tree = Tree::parse(&data).unwrap();
        assert_eq!(tree.len(), 2);
        assert_eq!(tree[0].mode, TreeEntryMode::Directory);
        assert_eq!(tree[0].name, "dir1");
        assert_eq!(tree[0].hash, SAMPLE_TREE_HASH);
        assert_eq!(tree[1].mode, TreeEntryMode::RegularFile);
        assert_eq!(tree[1].name, "file1");
        assert_eq!(tree[1].hash, HELLO_WORLD_BLOB_HASH);
    }

    #[test]
    fn test_parse_tree_invalid_header() {
        assert!(Tree::parse(b"blob 0\0").is_err());
    }

    #[test]
    fn test_parse_tree_missing_null_after_name() {
        let mut data = b"tree 31\0100644 file1".to_vec();
        data.extend_from_slice(&hex_to_20_bytes(HELLO_WORLD_BLOB_HASH));
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

    #[test]
    fn test_serialize_tree_uses_raw_sha_bytes() {
        let tree = Tree(vec![TreeEntry {
            mode: TreeEntryMode::RegularFile,
            name: "file1".to_string(),
            hash: HELLO_WORLD_BLOB_HASH.to_string(),
        }]);

        let body = tree.serialize().unwrap();
        assert_eq!(&body[..13], b"100644 file1\0");
        assert_eq!(&body[13..], &hex_to_20_bytes(HELLO_WORLD_BLOB_HASH));
    }

    #[test]
    fn test_write_empty_directory_uses_canonical_empty_tree_hash() {
        let temp = tempdir().unwrap();
        let store = ObjectStore::new(temp.path().join(GIT_DIR));
        std::fs::create_dir_all(temp.path().join(GIT_DIR).join(GIT_OBJECTS_DIR)).unwrap();

        let hash = Tree::write_dir(&store, temp.path()).unwrap();
        assert_eq!(hash, EMPTY_TREE_HASH);
    }

    #[test]
    fn test_write_tree_sorts_entries_by_name() {
        let temp = tempdir().unwrap();
        let store = ObjectStore::new(temp.path().join(GIT_DIR));
        std::fs::create_dir_all(temp.path().join(GIT_DIR).join(GIT_OBJECTS_DIR)).unwrap();

        std::fs::write(temp.path().join("z.txt"), b"z").unwrap();
        std::fs::write(temp.path().join("a.txt"), b"a").unwrap();

        let hash = Tree::write_dir(&store, temp.path()).unwrap();
        let tree = Tree::read_from(&store, &hash).unwrap();
        assert_eq!(tree[0].name, "a.txt");
        assert_eq!(tree[1].name, "z.txt");
    }

    #[test]
    fn test_write_tree_recurses_and_excludes_git_directory() {
        let temp = tempdir().unwrap();
        let store = ObjectStore::new(temp.path().join(GIT_DIR));
        std::fs::create_dir_all(temp.path().join(GIT_DIR).join(GIT_OBJECTS_DIR)).unwrap();
        std::fs::create_dir_all(temp.path().join("dir1")).unwrap();
        std::fs::create_dir_all(temp.path().join(GIT_DIR).join("nested")).unwrap();

        std::fs::write(temp.path().join("root.txt"), b"root").unwrap();
        std::fs::write(temp.path().join("dir1").join("child.txt"), b"child").unwrap();
        std::fs::write(
            temp.path().join(GIT_DIR).join("nested").join("ignored.txt"),
            b"ignored",
        )
        .unwrap();

        let hash = Tree::write_dir(&store, temp.path()).unwrap();
        let root_tree = Tree::read_from(&store, &hash).unwrap();

        assert_eq!(root_tree.len(), 2);
        assert_eq!(root_tree[0].name, "dir1");
        assert_eq!(root_tree[1].name, "root.txt");

        let child_tree = Tree::read_from(&store, &root_tree[0].hash).unwrap();
        assert_eq!(child_tree.len(), 1);
        assert_eq!(child_tree[0].name, "child.txt");
    }
}
