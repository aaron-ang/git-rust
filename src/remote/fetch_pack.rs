use std::path::Path;

use anyhow::{Result, anyhow};
use bytes::BytesMut;
use reqwest::Url;
use reqwest::blocking::Client;

use crate::pack::stream::PackStream;
use crate::pack::types::ParsedPack;

use super::sideband::{extract_packfile_from_response, pkt_line, stream_packfile_response};

pub(super) fn fetch_packfile<Pr, PB>(
    client: &Client,
    repo_url: &Url,
    pack_dir: &Path,
    want: &str,
    capabilities: &[String],
    mut on_progress: Pr,
    mut on_pack_bytes: PB,
) -> Result<ParsedPack>
where
    Pr: FnMut(&str) -> Result<()>,
    PB: FnMut(usize, Option<usize>, usize) -> Result<()>,
{
    let mut url = repo_url.clone();
    url.path_segments_mut()
        .map_err(|_| anyhow!("invalid repository URL"))?
        .push("git-upload-pack");

    let want_caps = capabilities
        .iter()
        .filter(|cap| matches!(cap.as_str(), "ofs-delta" | "side-band-64k" | "side-band"))
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

    let mut response = client
        .post(url)
        .header("Content-Type", "application/x-git-upload-pack-request")
        .header("Accept", "application/x-git-upload-pack-result")
        .body(body.freeze())
        .send()?
        .error_for_status()?;

    if want_caps
        .iter()
        .any(|cap| matches!(cap.as_str(), "side-band-64k" | "side-band"))
    {
        return stream_packfile_response(
            &mut response,
            pack_dir,
            &mut on_progress,
            &mut on_pack_bytes,
        );
    }

    let bytes = response.bytes()?;
    let bytes = extract_packfile_from_response(bytes)?;
    let mut pack = PackStream::new(pack_dir)?;
    let progress = pack.append(bytes.as_ref())?;
    on_pack_bytes(
        pack.pack_bytes(),
        progress.total_objects,
        progress.received_objects,
    )?;
    pack.finish()
}

#[cfg(test)]
mod tests {
    use crate::pack::parse::pack_object_count;

    #[test]
    fn test_pack_object_count_from_header() {
        let pack = b"PACK\x00\x00\x00\x02\x00\x00\x00\xf5rest";
        assert_eq!(pack_object_count(pack), Some(245));
        assert_eq!(pack_object_count(b"PACK\x00\x00\x00\x02"), None);
        assert_eq!(
            pack_object_count(b"NOPE\x00\x00\x00\x02\x00\x00\x00\xf5"),
            None
        );
    }
}
