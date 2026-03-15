use anyhow::{Result, anyhow, bail};
use reqwest::Url;
use reqwest::blocking::Client;
use std::str;

use super::sideband::parse_pkt_lines;
use super::{RemoteDiscovery, RemoteRef};

pub(super) fn discover(client: &Client, repo_url: &Url) -> Result<RemoteDiscovery> {
    let mut url = repo_url.clone();
    url.path_segments_mut()
        .map_err(|_| anyhow!("invalid repository URL"))?
        .push("info")
        .push("refs");
    url.query_pairs_mut()
        .append_pair("service", "git-upload-pack");

    let bytes = client
        .get(url)
        .header("Accept", "application/x-git-upload-pack-advertisement")
        .send()?
        .error_for_status()?
        .bytes()?;

    let mut lines = parse_pkt_lines(bytes)?.into_iter();
    match lines.next() {
        Some(Some(line)) if line.as_ref() == b"# service=git-upload-pack\n" => {}
        _ => bail!("invalid upload-pack advertisement"),
    }
    if !matches!(lines.next(), Some(None)) {
        bail!("missing advertisement flush packet");
    }

    let mut refs = Vec::new();
    let mut capabilities = Vec::new();
    for (idx, line) in lines.flatten().enumerate() {
        let line = str::from_utf8(&line)?.trim_end_matches('\n');
        let (ref_line, caps) = if idx == 0 {
            match line.split_once('\0') {
                Some((ref_line, caps)) => (ref_line, Some(caps)),
                None => (line, None),
            }
        } else {
            (line, None)
        };
        if let Some(caps) = caps {
            capabilities = caps.split(' ').map(str::to_string).collect();
        }
        let (hash, name) = ref_line
            .split_once(' ')
            .ok_or_else(|| anyhow!("invalid advertised ref line"))?;
        refs.push(RemoteRef {
            name: name.to_string(),
            hash: hash.to_string(),
        });
    }

    let head_ref = capabilities
        .iter()
        .find_map(|cap| cap.strip_prefix("symref=HEAD:"))
        .map(str::to_string)
        .or_else(|| {
            refs.iter()
                .find(|remote_ref| remote_ref.name == "HEAD")
                .map(|_| "refs/heads/main".to_string())
        })
        .ok_or_else(|| anyhow!("remote HEAD not advertised"))?;

    let head_hash = refs
        .iter()
        .find(|remote_ref| remote_ref.name == head_ref)
        .map(|remote_ref| remote_ref.hash.clone())
        .or_else(|| {
            refs.iter()
                .find(|remote_ref| remote_ref.name == "HEAD")
                .map(|remote_ref| remote_ref.hash.clone())
        })
        .ok_or_else(|| anyhow!("failed to resolve remote HEAD hash"))?;

    Ok(RemoteDiscovery {
        head_ref,
        head_hash,
        refs,
        capabilities,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::remote::sideband::pkt_line;

    #[test]
    fn test_parse_advertised_refs_with_symref() {
        let mut payload = Vec::new();
        payload.extend_from_slice(&pkt_line(b"# service=git-upload-pack\n"));
        payload.extend_from_slice(b"0000");
        payload.extend_from_slice(&pkt_line(
            b"0123456789012345678901234567890123456789 HEAD\0ofs-delta symref=HEAD:refs/heads/main\n",
        ));
        payload.extend_from_slice(&pkt_line(
            b"0123456789012345678901234567890123456789 refs/heads/main\n",
        ));

        let lines = parse_pkt_lines(bytes::Bytes::from(payload)).unwrap();
        assert_eq!(
            lines[0].as_deref(),
            Some(b"# service=git-upload-pack\n".as_slice())
        );
        assert!(lines[1].is_none());
        let first_ref = str::from_utf8(lines[2].as_deref().unwrap()).unwrap();
        assert!(first_ref.contains("symref=HEAD:refs/heads/main"));
    }
}
