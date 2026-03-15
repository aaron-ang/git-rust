use std::io::{self, Read};
use std::path::Path;
use std::str;

use anyhow::{Result, anyhow, bail};
use bytes::{Buf, Bytes, BytesMut};

use crate::pack::stream::PackStream;
use crate::pack::types::ParsedPack;

pub(super) enum PacketLine {
    Flush,
    Data(Vec<u8>),
}

pub(super) fn extract_packfile_from_response(mut bytes: Bytes) -> Result<Bytes> {
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

pub(super) fn stream_packfile_response<R, Pr, PB>(
    reader: &mut R,
    pack_dir: &Path,
    on_progress: &mut Pr,
    on_pack_bytes: &mut PB,
) -> Result<ParsedPack>
where
    R: Read,
    Pr: FnMut(&str) -> Result<()>,
    PB: FnMut(usize, Option<usize>, usize) -> Result<()>,
{
    match read_packet_line(reader)? {
        Some(PacketLine::Data(line)) if line == b"NAK\n" || line == b"ACK\n" => {}
        Some(PacketLine::Data(_)) => bail!("unsupported upload-pack response prefix"),
        Some(PacketLine::Flush) => bail!("upload-pack response missing ack/nak"),
        None => bail!("upload-pack response too short"),
    }

    let mut pack = PackStream::new(pack_dir)?;
    while let Some(packet) = read_packet_line(reader)? {
        let PacketLine::Data(payload) = packet else {
            continue;
        };
        let (&channel, data) = payload
            .split_first()
            .ok_or_else(|| anyhow!("empty side-band packet"))?;
        match channel {
            1 => {
                let progress = pack.append(data)?;
                on_pack_bytes(
                    pack.pack_bytes(),
                    progress.total_objects,
                    progress.received_objects,
                )?;
            }
            2 => on_progress(&String::from_utf8_lossy(data))?,
            3 => bail!("{}", String::from_utf8_lossy(data).trim_end()),
            other => bail!("unsupported side-band channel: {}", other),
        }
    }

    pack.finish()
}

pub(super) fn pkt_line(payload: &[u8]) -> Bytes {
    let len = payload.len() + 4;
    let mut line = BytesMut::with_capacity(len);
    line.extend_from_slice(format!("{len:04x}").as_bytes());
    line.extend_from_slice(payload);
    line.freeze()
}

pub(super) fn parse_pkt_lines(mut data: Bytes) -> Result<Vec<Option<Bytes>>> {
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

fn read_packet_line<R: Read>(reader: &mut R) -> Result<Option<PacketLine>> {
    let mut header = [0u8; 4];
    if !read_exact_or_eof(reader, &mut header)? {
        return Ok(None);
    }

    let len = pkt_len(&header)?;
    if len == 0 {
        return Ok(Some(PacketLine::Flush));
    }
    if len < 4 {
        bail!("invalid pkt-line header length");
    }

    let mut payload = vec![0; len - 4];
    reader.read_exact(&mut payload)?;
    Ok(Some(PacketLine::Data(payload)))
}

fn read_exact_or_eof<R: Read>(reader: &mut R, mut buffer: &mut [u8]) -> io::Result<bool> {
    let mut read_any = false;
    while !buffer.is_empty() {
        match reader.read(buffer) {
            Ok(0) if !read_any => return Ok(false),
            Ok(0) => {
                return Err(io::Error::new(
                    io::ErrorKind::UnexpectedEof,
                    "truncated input",
                ));
            }
            Ok(n) => {
                read_any = true;
                let (_, rest) = std::mem::take(&mut buffer).split_at_mut(n);
                buffer = rest;
            }
            Err(err) if err.kind() == io::ErrorKind::Interrupted => continue,
            Err(err) => return Err(err),
        }
    }
    Ok(true)
}

fn pkt_len(header: &[u8]) -> Result<usize> {
    if header.len() != 4 {
        bail!("invalid pkt-line header length");
    }
    Ok(usize::from_str_radix(str::from_utf8(header)?, 16)?)
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
}
