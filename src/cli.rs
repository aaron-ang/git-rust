use std::fs;
use std::io::{self, Write};
use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::{
    clone::run_clone,
    commit::Commit,
    data::blob::Blob,
    data::object::{GIT_DIR, GIT_HEAD_CONTENT, GIT_HEAD_FILE, GIT_OBJECTS_DIR, GIT_REFS_DIR},
    data::tree::Tree,
    error::{GitError, GitResult},
};

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
#[command(rename_all = "kebab-case")]
enum Commands {
    Init,
    CatFile {
        #[arg(short, long)]
        pretty: bool,
        /// Object hash (40-character SHA-1)
        object: Option<String>,
    },
    HashObject {
        #[arg(short, long)]
        write: bool,
        path: PathBuf,
    },
    LsTree {
        #[arg(long)]
        name_only: bool,
        tree_sha: String,
    },
    CommitTree {
        tree_sha: String,
        #[arg(short)]
        parent: String,
        #[arg(short)]
        message: String,
    },
    Clone {
        repo_url: String,
        target_dir: Option<PathBuf>,
    },
    WriteTree,
}

pub fn run() -> GitResult<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Init => run_init()?,
        Commands::CatFile { pretty, object } => run_cat_file(pretty, object)?,
        Commands::HashObject { write, path } => run_hash_object(write, path)?,
        Commands::LsTree {
            name_only,
            tree_sha,
        } => run_ls_tree(name_only, tree_sha)?,
        Commands::CommitTree {
            tree_sha,
            parent,
            message,
        } => run_commit_tree(tree_sha, parent, message)?,
        Commands::Clone {
            repo_url,
            target_dir,
        } => run_clone(&repo_url, target_dir)?,
        Commands::WriteTree => run_write_tree()?,
    }

    Ok(())
}

fn run_init() -> GitResult<()> {
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

fn run_cat_file(pretty: bool, object: Option<String>) -> GitResult<()> {
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

fn run_hash_object(write: bool, path: PathBuf) -> GitResult<()> {
    if !write {
        return Err(GitError::RequiredFlag("-w"));
    }
    let hash = Blob::write_from_path(&path)?;
    println!("{hash}");
    Ok(())
}

fn run_ls_tree(name_only: bool, tree_sha: String) -> GitResult<()> {
    if !name_only {
        return Err(GitError::RequiredFlag("--name-only"));
    }
    let tree = Tree::read(&tree_sha)?;
    for entry in tree.iter() {
        println!("{entry}");
    }
    Ok(())
}

fn run_write_tree() -> GitResult<()> {
    let hash = Tree::write_current_dir()?;
    println!("{hash}");
    Ok(())
}

fn run_commit_tree(tree_sha: String, parent: String, message: String) -> GitResult<()> {
    let hash = Commit::write(&tree_sha, &parent, &message)?;
    println!("{hash}");
    Ok(())
}
