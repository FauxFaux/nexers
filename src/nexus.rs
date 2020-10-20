use std::collections::HashSet;
use std::convert::TryFrom;
use std::io::BufRead;

use anyhow::anyhow;
use anyhow::bail;
use anyhow::ensure;
use anyhow::Context;
use anyhow::Result;
use bitflags::bitflags;
use hex;
use maplit::hashset;

use crate::java::DataInput;

pub type Checksum = [u8; 20];

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
pub struct UniqId {
    pub group: String,
    pub artifact: String,
    pub version: String,
    pub classifier: Option<String>,
    pub extension: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct FullInfo {
    pub packaging: String,
    pub last_modified: u64,
    pub size: Option<u64>,
    pub source_attached: AttachmentStatus,
    pub javadoc_attached: AttachmentStatus,
    pub signature_attached: AttachmentStatus,
    pub extension: String,
}

#[derive(Debug)]
pub struct Doc {
    pub id: UniqId,
    pub object_info: FullInfo,
    pub modified: u64,
    pub name: Option<String>,
    pub description: Option<String>,
    pub checksum: Option<Checksum>,
}

#[derive(Debug)]
pub enum Event {
    Doc(Doc),
    Delete(UniqId),
    Error {
        error: anyhow::Error,
        raw: Vec<(String, String)>,
    },
}

pub fn read<R: BufRead, F>(from: R, mut cb: F) -> Result<()>
where
    F: FnMut(Event) -> Result<()>,
{
    let mut from = DataInput::new(from);

    ensure!(1 == from.read_byte()?, "version byte");
    let _timestamp_ms = from.read_long()?;

    loop {
        let fields = read_fields(&mut from).with_context(|| anyhow!("reading fields"))?;

        let fields = match fields {
            Some(fields) => fields,
            None => break,
        };

        let names = fields
            .iter()
            .map(|(key, _value)| key.as_str())
            .collect::<HashSet<_>>();

        if names.contains("del") {
            cb(Event::Delete(read_uniq(
                fields
                    .iter()
                    .find_map(|(key, value)| if "del" == key { Some(value) } else { None })
                    .expect("just checked"),
            )?))?;
            continue;
        }

        if hashset!("DESCRIPTOR", "IDXINFO") == names {
            continue;
        }

        if hashset!("rootGroups", "rootGroupsList") == names {
            continue;
        }

        if hashset!("allGroups", "allGroupsList") == names {
            continue;
        }

        if !(names.contains("u") && names.contains("i") && names.contains("m")) {
            // TODO: move checker fails on 'fields' here
            cb(Event::Error {
                error: anyhow!("unrecognised doc type"),
                raw: fields.clone(),
            })?;
            continue;
        }

        cb(match read_doc(&fields) {
            Ok(doc) => Event::Doc(doc),
            Err(error) => Event::Error { error, raw: fields },
        })?;
    }

    Ok(())
}

fn read_doc(fields: &[(String, String)]) -> Result<Doc> {
    let mut you = None;
    let mut eye = None;
    let mut modified = None;
    let mut name = None;
    let mut description = None;
    let mut checksum = None;

    for (field_name, value) in fields {
        match field_name.as_str() {
            "u" => {
                you = Some(read_uniq(&value).with_context(|| anyhow!("reading 'u': {:?}", value))?)
            }
            "i" => {
                eye = Some(read_info(&value).with_context(|| anyhow!("reading 'i': {:?}", value))?)
            }
            "m" => {
                modified = Some(
                    value
                        .parse::<u64>()
                        .with_context(|| anyhow!("reading 'm': {:?}", value))?,
                )
            }
            "n" => name = Some(value.to_string()),
            "d" => description = Some(value.to_string()),
            "1" => checksum = read_checksum(&value).ok(),

            _ => (), // bail!("unrecognised field value: {:?}", field_name),
        }
    }

    Ok(Doc {
        id: you.ok_or_else(|| anyhow!("no 'u'"))?,
        object_info: eye.ok_or_else(|| anyhow!("no 'i'"))?,
        modified: modified.ok_or_else(|| anyhow!("no modified"))?,
        name,
        description,
        checksum,
    })
}

fn read_fields<R: BufRead>(f: &mut DataInput<R>) -> Result<Option<Vec<(String, String)>>> {
    if f.check_eof()? {
        return Ok(None);
    }

    let field_count = f
        .read_int()
        .with_context(|| anyhow!("reading field count (first field)"))?;

    let field_count = usize::try_from(field_count)?;
    let mut ret = Vec::with_capacity(field_count);

    for field in 0..field_count {
        ret.push(read_field(f).with_context(|| anyhow!("reading field {}", field))?);
    }

    Ok(Some(ret))
}

fn read_field<R: BufRead>(f: &mut DataInput<R>) -> Result<(String, String)> {
    let flags = u8::try_from(f.read_byte()?)?;
    let _flags = FieldFlag::from_bits(flags).ok_or_else(|| anyhow!("decoding field flags"))?;

    let name_len = f.read_unsigned_short()?;
    let name = f.read_utf8(usize::try_from(name_len).unwrap())?;

    // yup, they went out of their way to use signed data here
    let value_len = usize::try_from(f.read_int()?)?;
    let value = f.read_utf8(value_len)?;

    Ok((name, value))
}

#[inline]
fn read_checksum(value: &str) -> Result<[u8; 20]> {
    let decoded = hex::decode(value).with_context(|| anyhow!("decoding checksum"))?;
    ensure!(20 == decoded.len(), "checksum was wrong length");
    let mut arr = [0u8; 20];
    arr.copy_from_slice(&decoded);
    Ok(arr)
}

fn read_uniq(value: &str) -> Result<UniqId> {
    let mut parts = value.split('|');

    Ok(UniqId {
        group: parts
            .next()
            .ok_or_else(|| anyhow!("short uniq: group"))?
            .to_string(),
        artifact: parts
            .next()
            .ok_or_else(|| anyhow!("short uniq: artifact"))?
            .to_string(),
        version: parts
            .next()
            .ok_or_else(|| anyhow!("short uniq: version"))?
            .to_string(),
        classifier: not_na(
            parts
                .next()
                .ok_or_else(|| anyhow!("short uniq: classifier"))?,
        )
        .map(|v| v.to_string()),
        extension: parts.next().map(|v| v.to_string()),
    })
}

fn read_info(value: &str) -> Result<FullInfo> {
    let mut parts = value.split('|');
    Ok(FullInfo {
        packaging: parts
            .next()
            .ok_or_else(|| anyhow!("short info: packaging"))?
            .to_string(),
        last_modified: parts
            .next()
            .ok_or_else(|| anyhow!("short info: time"))?
            .parse::<u64>()
            .with_context(|| anyhow!("reading time"))?,
        size: read_size(parts.next().ok_or_else(|| anyhow!("short i: size"))?)?,
        source_attached: AttachmentStatus::read(
            parts
                .next()
                .ok_or_else(|| anyhow!("short info: sources flag"))?,
        )?,
        javadoc_attached: AttachmentStatus::read(
            parts.next().ok_or_else(|| anyhow!("short info: flag 2"))?,
        )?,
        signature_attached: AttachmentStatus::read(
            parts.next().ok_or_else(|| anyhow!("short info: flag 3"))?,
        )?,
        extension: parts
            .next()
            .ok_or_else(|| anyhow!("short info: extension"))?
            .to_string(),
    })
}

fn read_size(value: &str) -> Result<Option<u64>> {
    if "-1" == value {
        return Ok(None);
    }

    Ok(Some(
        value
            .parse::<u64>()
            .with_context(|| anyhow!("reading size"))?,
    ))
}

fn not_na(value: &str) -> Option<&str> {
    if "NA" == value {
        None
    } else {
        Some(value)
    }
}

#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum AttachmentStatus {
    Absent,
    Present,
    Unavailable,
}

impl AttachmentStatus {
    fn read(value: &str) -> Result<AttachmentStatus> {
        Ok(match value.parse::<u8>() {
            Ok(0) => AttachmentStatus::Absent,
            Ok(1) => AttachmentStatus::Present,
            Ok(2) => AttachmentStatus::Unavailable,
            other => bail!("invalid attachment value: {:?}: {:?}", value, other),
        })
    }
}

bitflags! {
    struct FieldFlag: u8 {
        const INDEXED    = 0b0000_0001;
        const TOKENIZED  = 0b0000_0010;
        const STORED     = 0b0000_0100;
        const COMPRESSED = 0b0000_1000;
    }
}
