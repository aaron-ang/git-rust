use anyhow::Result;

use crate::object::{ObjectStore, ObjectType};

const AUTHOR_NAME: &str = "John Doe";
const AUTHOR_EMAIL: &str = "john@example.com";
const AUTHOR_TIMESTAMP: &str = "1234567890 +0000";

pub struct Commit {
    tree: String,
    parent: String,
    message: String,
}

impl Commit {
    pub fn write(tree_sha: &str, parent_sha: &str, message: &str) -> Result<String> {
        Self::write_in(&ObjectStore::default(), tree_sha, parent_sha, message)
    }

    pub fn write_in(
        store: &ObjectStore,
        tree_sha: &str,
        parent_sha: &str,
        message: &str,
    ) -> Result<String> {
        let commit = Self {
            tree: tree_sha.to_string(),
            parent: parent_sha.to_string(),
            message: message.to_string(),
        };
        store.write_object(ObjectType::Commit, &commit.serialize())
    }

    fn serialize(&self) -> Vec<u8> {
        format!(
            "tree {}\nparent {}\nauthor {} <{}> {}\ncommitter {} <{}> {}\n\n{}\n",
            self.tree,
            self.parent,
            AUTHOR_NAME,
            AUTHOR_EMAIL,
            AUTHOR_TIMESTAMP,
            AUTHOR_NAME,
            AUTHOR_EMAIL,
            AUTHOR_TIMESTAMP,
            self.message
        )
        .into_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::object::GIT_DIR;
    use tempfile::tempdir;

    const TREE_SHA: &str = "4b825dc642cb6eb9a060e54bf8d69288fbee4904";
    const PARENT_SHA: &str = "3b18e512dba79e4c8300dd08aeb37f8e728b8dad";
    const MESSAGE: &str = "Initial commit";

    #[test]
    fn test_serialize_commit_body() {
        let commit = Commit {
            tree: TREE_SHA.to_string(),
            parent: PARENT_SHA.to_string(),
            message: MESSAGE.to_string(),
        };

        let body = String::from_utf8(commit.serialize()).unwrap();
        assert_eq!(
            body,
            format!(
                "tree {TREE_SHA}\n\
parent {PARENT_SHA}\n\
author {AUTHOR_NAME} <{AUTHOR_EMAIL}> {AUTHOR_TIMESTAMP}\n\
committer {AUTHOR_NAME} <{AUTHOR_EMAIL}> {AUTHOR_TIMESTAMP}\n\
\n\
{MESSAGE}\n"
            )
        );
    }

    #[test]
    fn test_write_commit_stores_expected_payload() {
        let temp = tempdir().unwrap();
        let store = ObjectStore::new(temp.path().join(GIT_DIR));

        let hash = Commit::write_in(&store, TREE_SHA, PARENT_SHA, MESSAGE).unwrap();
        let payload = String::from_utf8(store.read_object(&hash).unwrap()).unwrap();
        let body = format!(
            "tree {TREE_SHA}\n\
parent {PARENT_SHA}\n\
author {AUTHOR_NAME} <{AUTHOR_EMAIL}> {AUTHOR_TIMESTAMP}\n\
committer {AUTHOR_NAME} <{AUTHOR_EMAIL}> {AUTHOR_TIMESTAMP}\n\
\n\
{MESSAGE}\n"
        );
        assert_eq!(payload, format!("commit {}\0{}", body.len(), body));
    }
}
