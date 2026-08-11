use crate::core::MtopError;
use std::io::{Read, Write};

pub(crate) fn read_be_u8<R>(reader: &mut R) -> Result<u8, MtopError>
where
    R: Read,
{
    let mut buf = [0; 1];
    reader.read_exact(&mut buf)?;
    Ok(buf[0])
}

pub(crate) fn read_be_u16<R>(reader: &mut R) -> Result<u16, MtopError>
where
    R: Read,
{
    let mut buf = [0; 2];
    reader.read_exact(&mut buf)?;
    Ok(u16::from_be_bytes(buf[..2].try_into().unwrap()))
}

pub(crate) fn read_be_u32<R>(reader: &mut R) -> Result<u32, MtopError>
where
    R: Read,
{
    let mut buf = [0; 4];
    reader.read_exact(&mut buf)?;
    Ok(u32::from_be_bytes(buf[..4].try_into().unwrap()))
}

pub(crate) fn write_be_u8<W>(writer: &mut W, b: u8) -> Result<(), MtopError>
where
    W: Write,
{
    Ok(writer.write_all(&[b])?)
}

pub(crate) fn write_be_u16<W>(writer: &mut W, b: u16) -> Result<(), MtopError>
where
    W: Write,
{
    Ok(writer.write_all(&b.to_be_bytes())?)
}

pub(crate) fn write_be_u32<W>(writer: &mut W, b: u32) -> Result<(), MtopError>
where
    W: Write,
{
    Ok(writer.write_all(&b.to_be_bytes())?)
}
