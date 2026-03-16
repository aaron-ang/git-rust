use std::fs;
use std::path::PathBuf;

use crate::{
    data::object::{GIT_DIR, GIT_HEAD_CONTENT, GIT_HEAD_FILE, GIT_OBJECTS_DIR, GIT_REFS_DIR},
    error::GitResult,
};

pub fn run() -> GitResult<()> {
    let git_dir = PathBuf::from(GIT_DIR);
    let already_initialized = git_dir.is_dir();

    fs::create_dir_all(git_dir.join(GIT_OBJECTS_DIR))?;
    fs::create_dir_all(git_dir.join(GIT_REFS_DIR))?;

    let head_path = git_dir.join(GIT_HEAD_FILE);
    if !head_path.exists() {
        fs::write(head_path, GIT_HEAD_CONTENT)?;
    }

    let mut display_path = std::env::current_dir()?.join(GIT_DIR).display().to_string();
    if !display_path.ends_with('/') {
        display_path.push('/');
    }

    if already_initialized {
        println!("Reinitialized existing Git repository in {display_path}");
    } else {
        println!("Initialized empty Git repository in {display_path}");
    }

    Ok(())
}
