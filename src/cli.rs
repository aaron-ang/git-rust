use std::path::PathBuf;

use clap::{Parser, Subcommand};

use crate::{builtin, error::GitResult};

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
        Commands::Init => builtin::init::run()?,
        Commands::CatFile { pretty, object } => builtin::cat_file::run(pretty, object)?,
        Commands::HashObject { write, path } => builtin::hash_object::run(write, path)?,
        Commands::LsTree {
            name_only,
            tree_sha,
        } => builtin::ls_tree::run(name_only, tree_sha)?,
        Commands::CommitTree {
            tree_sha,
            parent,
            message,
        } => builtin::commit_tree::run(tree_sha, parent, message)?,
        Commands::Clone {
            repo_url,
            target_dir,
        } => builtin::clone::Clone::run(&repo_url, target_dir)?,
        Commands::WriteTree => builtin::write_tree::run()?,
    }

    Ok(())
}
