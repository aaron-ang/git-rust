use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;
use std::process::ExitCode;

use clap::{Parser, Subcommand};

use git_rust::{Blob, GIT_DIR, GitError, Result};

#[derive(Parser)]
#[command(name = "git-rust", about = "A minimal Git implementation in Rust.")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Init,
    #[command(name = "cat-file")]
    CatFile {
        #[arg(short, long)]
        pretty: bool,
        /// Object hash (40-character SHA-1)
        object: Option<String>,
    },
}

fn run(cli: Cli) -> Result<()> {
    match cli.command {
        Commands::Init => run_init()?,
        Commands::CatFile { pretty, object } => run_cat_file(pretty, object)?,
    }
    Ok(())
}

fn run_init() -> Result<()> {
    let git_dir = PathBuf::from(GIT_DIR);
    let already_initialized = git_dir.is_dir();

    fs::create_dir_all(git_dir.join("objects"))?;
    fs::create_dir_all(git_dir.join("refs"))?;

    let head_path = git_dir.join("HEAD");
    if !head_path.exists() {
        fs::write(head_path, "ref: refs/heads/main\n")?;
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

fn run_cat_file(pretty: bool, object: Option<String>) -> Result<()> {
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

fn main() -> ExitCode {
    match run(Cli::parse()) {
        Ok(()) => ExitCode::SUCCESS,
        Err(err) => {
            eprintln!("{err}");
            ExitCode::from(GitError::EXIT_CODE)
        }
    }
}
