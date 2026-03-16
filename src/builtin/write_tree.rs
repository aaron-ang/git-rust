use crate::{data::tree::Tree, error::GitResult};

pub fn run() -> GitResult<()> {
    let hash = Tree::write_current_dir()?;
    println!("{hash}");
    Ok(())
}
