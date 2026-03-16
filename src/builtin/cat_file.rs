use std::io::{self, Write};

use crate::{
    data::blob::Blob,
    error::{GitError, GitResult},
};

pub fn run(pretty: bool, object: Option<String>) -> GitResult<()> {
    if !pretty {
        let arg_count = usize::from(object.is_some()) as u32;
        return Err(GitError::CatFileTypeObjectMode(arg_count));
    }
    let object = match object {
        Some(object) if !object.is_empty() => object,
        _ => return Err(GitError::CatFileObjectRequired),
    };
    let content = Blob::read(&object)?;
    let mut stdout = io::stdout();
    stdout.write_all(&content)?;
    stdout.flush()?;
    Ok(())
}
