use crate::{commit::Commit, error::GitResult};

pub fn run(tree_sha: String, parent: String, message: String) -> GitResult<()> {
    let hash = Commit::write(&tree_sha, &parent, &message)?;
    println!("{hash}");
    Ok(())
}
