use thiserror::Error;

pub type GitResult<T> = Result<T, GitError>;

#[derive(Debug, Error)]
pub enum GitError {
    #[error("fatal: <object> required with '-p'")]
    CatFileObjectRequired,

    #[error("fatal: only two arguments allowed in <type> <object> mode, not {0}")]
    CatFileTypeObjectMode(u32),

    #[error("fatal: '-w' is required")]
    HashObjectWriteRequired,

    #[error(transparent)]
    Io(#[from] std::io::Error),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

impl GitError {
    pub const EXIT_CODE: u8 = 129;
}
