mod discovery;
mod fetch_pack;
mod sideband;

use anyhow::Result;
use reqwest::Url;
use reqwest::blocking::Client;

use crate::pack::types::ParsedPack;

#[derive(Clone, Debug)]
pub struct RemoteRef {
    pub name: String,
    pub hash: String,
}

#[derive(Debug)]
pub struct RemoteDiscovery {
    pub head_ref: String,
    pub head_hash: String,
    pub refs: Vec<RemoteRef>,
    pub capabilities: Vec<String>,
}

pub struct RemoteClient {
    client: Client,
    repo_url: Url,
}

impl RemoteClient {
    pub fn new(repo_url: &str) -> Result<Self> {
        let client = Client::builder().build()?;
        let repo_url = Url::parse(repo_url)?;
        Ok(Self { client, repo_url })
    }

    pub fn discover(&self) -> Result<RemoteDiscovery> {
        discovery::discover(&self.client, &self.repo_url)
    }

    pub fn fetch_packfile<Pr, PB>(
        &self,
        want: &str,
        capabilities: &[String],
        on_progress: Pr,
        on_pack_bytes: PB,
    ) -> Result<ParsedPack>
    where
        Pr: FnMut(&str) -> Result<()>,
        PB: FnMut(usize, Option<usize>, usize) -> Result<()>,
    {
        fetch_pack::fetch_packfile(
            &self.client,
            &self.repo_url,
            want,
            capabilities,
            on_progress,
            on_pack_bytes,
        )
    }
}
