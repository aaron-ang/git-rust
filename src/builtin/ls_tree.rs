use crate::{
    data::tree::Tree,
    error::{GitError, GitResult},
};

pub fn run(name_only: bool, tree_sha: String) -> GitResult<()> {
    if !name_only {
        return Err(GitError::RequiredFlag("--name-only"));
    }
    let tree = Tree::read(&tree_sha)?;
    for entry in tree.iter() {
        println!("{entry}");
    }
    Ok(())
}
