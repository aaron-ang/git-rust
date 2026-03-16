use std::path::PathBuf;

use crate::{
    data::blob::Blob,
    error::{GitError, GitResult},
};

pub fn run(write: bool, path: PathBuf) -> GitResult<()> {
    if !write {
        return Err(GitError::RequiredFlag("-w"));
    }
    let hash = Blob::write_from_path(&path)?;
    println!("{hash}");
    Ok(())
}
