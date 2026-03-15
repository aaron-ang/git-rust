use std::io::Cursor;

use anyhow::{Result, anyhow, bail};
use bytes::Buf;

pub(crate) fn apply_delta_with_interrupt<F>(
    base: &[u8],
    delta: &[u8],
    mut check_interrupt: F,
) -> Result<Vec<u8>>
where
    F: FnMut() -> Result<()>,
{
    let mut delta = Cursor::new(delta);
    let base_size = read_varint(&mut delta)?;
    if base_size != base.len() {
        bail!(
            "delta base size mismatch, expected {}, got {}",
            base_size,
            base.len()
        );
    }
    let result_size = read_varint(&mut delta)?;
    let mut result = Vec::with_capacity(result_size);

    while delta.has_remaining() {
        check_interrupt()?;
        let instruction = delta.get_u8();

        if instruction & 0x80 != 0 {
            let mut copy_offset = 0usize;
            let mut copy_size = 0usize;

            if instruction & 0x01 != 0 {
                copy_offset |= next_delta_byte(&mut delta)? as usize;
            }
            if instruction & 0x02 != 0 {
                copy_offset |= (next_delta_byte(&mut delta)? as usize) << 8;
            }
            if instruction & 0x04 != 0 {
                copy_offset |= (next_delta_byte(&mut delta)? as usize) << 16;
            }
            if instruction & 0x08 != 0 {
                copy_offset |= (next_delta_byte(&mut delta)? as usize) << 24;
            }
            if instruction & 0x10 != 0 {
                copy_size |= next_delta_byte(&mut delta)? as usize;
            }
            if instruction & 0x20 != 0 {
                copy_size |= (next_delta_byte(&mut delta)? as usize) << 8;
            }
            if instruction & 0x40 != 0 {
                copy_size |= (next_delta_byte(&mut delta)? as usize) << 16;
            }
            if copy_size == 0 {
                copy_size = 0x10000;
            }

            let end = copy_offset + copy_size;
            result.extend_from_slice(
                base.get(copy_offset..end)
                    .ok_or_else(|| anyhow!("delta copy exceeds base object"))?,
            );
            continue;
        }

        if instruction == 0 {
            bail!("invalid delta opcode 0");
        }

        let start = delta.position() as usize;
        let end = start + instruction as usize;
        result.extend_from_slice(
            delta
                .get_ref()
                .get(start..end)
                .ok_or_else(|| anyhow!("delta insert exceeds delta data"))?,
        );
        delta.advance(instruction as usize);
    }

    if result.len() != result_size {
        bail!(
            "delta result size mismatch, expected {}, got {}",
            result_size,
            result.len()
        );
    }

    Ok(result)
}

fn read_varint(input: &mut Cursor<&[u8]>) -> Result<usize> {
    let mut result = 0usize;
    let mut shift = 0usize;

    loop {
        let byte = next_delta_byte(input)?;
        result |= ((byte & 0x7f) as usize) << shift;
        if byte & 0x80 == 0 {
            break;
        }
        shift += 7;
    }

    Ok(result)
}

fn next_delta_byte(input: &mut Cursor<&[u8]>) -> Result<u8> {
    if !input.has_remaining() {
        bail!("truncated delta instruction");
    }
    Ok(input.get_u8())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_apply_delta_insert() {
        let delta = [0x03, 0x06, 0x90, 0x03, 0x03, b'd', b'e', b'f'];
        let result = apply_delta_with_interrupt(b"abc", &delta, || Ok(())).unwrap();
        assert_eq!(result, b"abcdef");
    }

    #[test]
    fn test_apply_delta_with_interrupt_aborts() {
        let delta = [0x03, 0x06, 0x90, 0x03, 0x03, b'd', b'e', b'f'];
        let error = apply_delta_with_interrupt(b"abc", &delta, || bail!("interrupted"))
            .unwrap_err()
            .to_string();
        assert!(error.contains("interrupted"));
    }
}
