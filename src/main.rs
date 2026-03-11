use std::process::ExitCode;

use git_rust::{cli, error::GitError};

fn main() -> ExitCode {
    match cli::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(GitError::EXIT_CODE)
        }
    }
}
