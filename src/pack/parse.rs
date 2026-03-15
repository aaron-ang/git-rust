use anyhow::{Result, bail};

pub fn pack_object_count(data: &[u8]) -> Option<usize> {
    if data.len() < 12 || &data[..4] != b"PACK" {
        return None;
    }

    Some(u32::from_be_bytes(data[8..12].try_into().ok()?) as usize)
}

pub(crate) fn parse_pack_header(data: &[u8]) -> Result<Option<usize>> {
    if data.is_empty() {
        return Ok(None);
    }
    if data.len() < 4 {
        return Ok(None);
    }
    if &data[..4] != b"PACK" {
        bail!("invalid pack header");
    }
    if data.len() < 12 {
        return Ok(None);
    }

    let version = u32::from_be_bytes(data[4..8].try_into().unwrap());
    if version != 2 && version != 3 {
        bail!("unsupported pack version: {version}");
    }

    Ok(Some(
        u32::from_be_bytes(data[8..12].try_into().unwrap()) as usize
    ))
}

pub(crate) fn parse_ofs_delta_base_partial(input: &[u8]) -> Result<Option<(usize, usize)>> {
    let mut consumed = 0;
    let Some(&first) = input.get(consumed) else {
        return Ok(None);
    };
    let mut byte = first;
    consumed += 1;
    let mut offset = (byte & 0x7f) as usize;

    while byte & 0x80 != 0 {
        let Some(&next) = input.get(consumed) else {
            return Ok(None);
        };
        byte = next;
        consumed += 1;
        offset = ((offset + 1) << 7) | (byte & 0x7f) as usize;
    }

    Ok(Some((offset, consumed)))
}

pub(crate) fn parse_object_header_partial(input: &[u8]) -> Result<Option<(u8, usize)>> {
    let mut size_shift = 4;
    let mut consumed = 0;
    let Some(&first) = input.get(consumed) else {
        return Ok(None);
    };
    let object_type = (first >> 4) & 0x07;
    let mut byte = first;
    consumed += 1;

    while byte & 0x80 != 0 {
        let Some(&next) = input.get(consumed) else {
            return Ok(None);
        };
        byte = next;
        let _ = ((byte & 0x7f) as usize) << size_shift;
        size_shift += 7;
        consumed += 1;
    }

    Ok(Some((object_type, consumed)))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_ofs_delta_base_partial() {
        let (offset, consumed) = parse_ofs_delta_base_partial(&[0x7f]).unwrap().unwrap();
        assert_eq!(offset, 0x7f);
        assert_eq!(consumed, 1);
    }
}
