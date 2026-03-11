use bytes::{Buf, Bytes, BytesMut};
use std::str;

use anyhow::{Result, anyhow, bail};
use reqwest::Url;
use reqwest::blocking::Client;

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
        let mut url = self.repo_url.clone();
        url.path_segments_mut()
            .map_err(|_| anyhow!("invalid repository URL"))?
            .push("info")
            .push("refs");
        url.query_pairs_mut()
            .append_pair("service", "git-upload-pack");

        let bytes = self
            .client
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

    pub fn fetch_packfile(&self, want: &str, capabilities: &[String]) -> Result<Bytes> {
        let mut url = self.repo_url.clone();
        url.path_segments_mut()
            .map_err(|_| anyhow!("invalid repository URL"))?
            .push("git-upload-pack");

        let want_caps = capabilities
            .iter()
            .filter(|cap| cap.as_str() == "ofs-delta")
            .cloned()
            .collect::<Vec<_>>();
        let want_line = if want_caps.is_empty() {
            format!("want {}\n", want)
        } else {
            format!("want {} {}\n", want, want_caps.join(" "))
        };

        let mut body = BytesMut::new();
        body.extend_from_slice(&pkt_line(want_line.as_bytes()));
        body.extend_from_slice(b"0000");
        body.extend_from_slice(&pkt_line(b"done\n"));

        let bytes = self
            .client
            .post(url)
            .header("Content-Type", "application/x-git-upload-pack-request")
            .header("Accept", "application/x-git-upload-pack-result")
            .body(body.freeze())
            .send()?
            .error_for_status()?
            .bytes()?;

        extract_packfile_from_response(bytes)
    }
}

fn extract_packfile_from_response(mut bytes: Bytes) -> Result<Bytes> {
    if bytes.starts_with(b"PACK") {
        return Ok(bytes);
    }

    if bytes.remaining() < 8 {
        bail!("upload-pack response too short");
    }
    let len = pkt_len(&bytes[..4])?;
    if len < 4 || bytes.remaining() < len {
        bail!("invalid upload-pack response prefix");
    }
    let prefix = bytes.slice(4..len);
    if prefix.as_ref() != b"NAK\n" && prefix.as_ref() != b"ACK\n" {
        bail!("unsupported upload-pack response prefix");
    }

    bytes.advance(len);
    if !bytes.starts_with(b"PACK") {
        bail!("upload-pack response missing packfile payload");
    }
    Ok(bytes)
}

fn pkt_line(payload: &[u8]) -> Bytes {
    let len = payload.len() + 4;
    let mut line = BytesMut::with_capacity(len);
    line.extend_from_slice(format!("{len:04x}").as_bytes());
    line.extend_from_slice(payload);
    line.freeze()
}

fn pkt_len(header: &[u8]) -> Result<usize> {
    if header.len() != 4 {
        bail!("invalid pkt-line header length");
    }
    Ok(usize::from_str_radix(str::from_utf8(header)?, 16)?)
}

fn parse_pkt_lines(mut data: Bytes) -> Result<Vec<Option<Bytes>>> {
    let mut lines = Vec::new();
    while data.has_remaining() {
        if data.remaining() < 4 {
            bail!("truncated pkt-line header");
        }
        let len = pkt_len(&data[..4])?;
        data.advance(4);
        if len == 0 {
            lines.push(None);
            continue;
        }
        if len < 4 || data.remaining() < len - 4 {
            bail!("truncated pkt-line payload");
        }
        lines.push(Some(data.split_to(len - 4)));
    }
    Ok(lines)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_pkt_lines() {
        let lines = parse_pkt_lines(Bytes::from_static(b"0008NAK\n0000")).unwrap();
        assert_eq!(lines.len(), 2);
        assert_eq!(lines[0].as_deref(), Some(b"NAK\n".as_slice()));
        assert!(lines[1].is_none());
    }

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

        let lines = parse_pkt_lines(Bytes::from(payload)).unwrap();
        assert_eq!(
            lines[0].as_deref(),
            Some(b"# service=git-upload-pack\n".as_slice())
        );
        assert!(lines[1].is_none());
        let first_ref = str::from_utf8(lines[2].as_deref().unwrap()).unwrap();
        assert!(first_ref.contains("symref=HEAD:refs/heads/main"));
    }
}
