use std::fs;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use clap::{Parser, Subcommand};

use crate::{
    blob::Blob,
    commit::Commit,
    error::{GitError, GitResult},
    object::{
        GIT_DIR, GIT_HEAD_CONTENT, GIT_HEAD_FILE, GIT_OBJECTS_DIR, GIT_REFS_DIR, ObjectStore,
    },
    pack,
    remote::RemoteClient,
    tree::Tree,
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

fn run_clone(repo_url: &str, target_dir: Option<PathBuf>) -> GitResult<()> {
    let target_dir = resolve_clone_target(repo_url, target_dir)?;
    ensure_empty_clone_target(&target_dir)?;

    fs::create_dir_all(&target_dir)?;
    let git_dir = init_repo_layout(&target_dir)?;
    let store = ObjectStore::new(git_dir.clone());

    let remote = RemoteClient::new(repo_url)?;
    let discovery = remote.discover()?;
    let packfile = remote.fetch_packfile(&discovery.head_hash, &discovery.capabilities)?;
    pack::unpack_into(&store, packfile.as_ref())?;

    write_clone_refs(&git_dir, &discovery.head_ref, &discovery.head_hash)?;

    let root_tree = Commit::root_tree_in(&store, &discovery.head_hash)?;
    Tree::checkout_in(&store, &root_tree, &target_dir)?;
    Ok(())
}

fn resolve_clone_target(repo_url: &str, target_dir: Option<PathBuf>) -> GitResult<PathBuf> {
    if let Some(target_dir) = target_dir {
        return Ok(target_dir);
    }

    let repo_name = repo_url
        .trim_end_matches('/')
        .rsplit('/')
        .next()
        .map(|segment| segment.strip_suffix(".git").unwrap_or(segment))
        .filter(|segment| !segment.is_empty())
        .ok_or(GitError::CantGuessCloneTarget)?;

    Ok(PathBuf::from(repo_name))
}

fn ensure_empty_clone_target(target_dir: &Path) -> GitResult<()> {
    if !target_dir.exists() {
        return Ok(());
    }

    let metadata = fs::metadata(target_dir)?;
    if !metadata.is_dir() {
        return Err(GitError::CloneTargetNotEmpty(target_dir.to_path_buf()));
    }

    if fs::read_dir(target_dir)?.next().is_some() {
        return Err(GitError::CloneTargetNotEmpty(target_dir.to_path_buf()));
    }

    Ok(())
}

fn init_repo_layout(target_dir: &Path) -> GitResult<PathBuf> {
    let git_dir = target_dir.join(GIT_DIR);
    fs::create_dir_all(git_dir.join(GIT_OBJECTS_DIR))?;
    fs::create_dir_all(git_dir.join(GIT_REFS_DIR))?;
    Ok(git_dir)
}

fn write_clone_refs(git_dir: &Path, head_ref: &str, head_hash: &str) -> GitResult<()> {
    fs::write(git_dir.join(GIT_HEAD_FILE), format!("ref: {head_ref}\n"))?;
    let ref_path = git_dir.join(head_ref);
    if let Some(parent) = ref_path.parent() {
        fs::create_dir_all(parent)?;
    }
    fs::write(ref_path, format!("{head_hash}\n"))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn empty_clone_target_rejects_non_empty_directory() {
        let temp = tempdir().unwrap();
        fs::write(temp.path().join("README.md"), b"hello").unwrap();

        let error = ensure_empty_clone_target(temp.path()).unwrap_err();
        assert!(matches!(error, GitError::CloneTargetNotEmpty(_)));
    }
}
