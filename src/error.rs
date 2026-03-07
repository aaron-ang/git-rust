use std::path::PathBuf;
use thiserror::Error;

pub type GitResult<T> = Result<T, GitError>;

#[derive(Debug, Error)]
pub enum GitError {
    #[error("fatal: <object> required with '-p'")]
    CatFileObjectRequired,

    #[error("fatal: only two arguments allowed in <type> <object> mode, not {0}")]
    CatFileTypeObjectMode(u32),

    #[error("fatal: '{0}' is required")]
    RequiredFlag(&'static str),

    #[error("fatal: destination path '{0}' already exists and is not an empty directory.")]
    CloneTargetNotEmpty(PathBuf),

    #[error("fatal: No directory name could be guessed.\nPlease specify a target directory on the command line")]
    CantGuessCloneTarget,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl GitError {
    pub const EXIT_CODE: u8 = 129;
}
