use std::collections::HashSet;
use std::io;
use std::io::BufRead;

use bitflags::bitflags;
use byteorder::ReadBytesExt;
use byteorder::BE;
use cast::u8;
use cast::usize;
use cesu8;
use failure::bail;
use failure::ensure;
use failure::err_msg;
use failure::format_err;
use failure::Error;
use failure::ResultExt;
use hex;
use maplit::hashset;

bitflags! {
    struct FieldFlag: u8 {
        const INDEXED    = 0b0000_0001;
        const TOKENIZED  = 0b0000_0010;
        const STORED     = 0b0000_0100;
        const COMPRESSED = 0b0000_1000;
    }
}

#[derive(Clone, Debug, Hash, Eq, PartialEq)]
struct UniqId {
    group: String,
    artifact: String,
    version: String,
    classifier: Option<String>,
    extension: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct FullInfo {
    packaging: String,
    last_modified: u64,
    size: Option<u64>,
    source_attached: AttachmentStatus,
    javadoc_attached: AttachmentStatus,
    signature_attached: AttachmentStatus,
    extension: String,
}

pub struct Doc {
    id: UniqId,
    object_info: FullInfo,
    modified: u64,
    name: Option<String>,
    description: Option<String>,
    checksum: Option<[u8; 20]>,
}

pub fn read<R: BufRead>(f: R) -> Result<Vec<Doc>, Error> {
    let mut f = DataInput { inner: f };

    ensure!(1 == f.read_byte()?, "version byte");
    let _timestamp_ms = f.read_long()?;

    let mut docs = Vec::with_capacity(100_000);
    let mut deletions = HashSet::with_capacity(1_000);

    let mut errors = Vec::with_capacity(32);

    loop {
        let fields = read_fields(&mut f).with_context(|_| err_msg("reading fields"))?;

        let fields = match fields {
            Some(fields) => fields,
            None => break,
        };

        let names = fields
            .iter()
            .map(|(key, _value)| key.as_str())
            .collect::<HashSet<_>>();

        if names.contains("del") {
            deletions.insert(read_uniq(
                fields
                    .iter()
                    .find_map(|(key, value)| if "del" == key { Some(value) } else { None })
                    .expect("just checked"),
            )?);
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
            errors.push((err_msg("unrecognised doc type"), fields.clone()));
            continue;
        }

        match read_doc(&fields) {
            Ok(doc) => docs.push(doc),
            Err(e) => errors.push((e, fields)),
        }
    }

    for (e, fields) in &errors {
        println!("Error in doc:");
        for (name, value) in fields {
            println!(" * {:?}: {:?}", name, value);
        }
        println!("{:?}", e);
        println!();
    }

    println!(
        "{} errors, {} deletions, {} docs",
        errors.len(),
        deletions.len(),
        docs.len()
    );

    docs.retain(|v| !deletions.contains(&v.id));

    println!("{} live docs", docs.len());

    Ok(docs)
}

fn read_doc(fields: &[(String, String)]) -> Result<Doc, Error> {
    let mut you = None;
    let mut eye = None;
    let mut modified = None;
    let mut name = None;
    let mut description = None;
    let mut checksum = None;

    for (field_name, value) in fields {
        match field_name.as_str() {
            "u" => {
                you = Some(
                    read_uniq(&value).with_context(|_| format_err!("reading 'u': {:?}", value))?,
                )
            }
            "i" => {
                eye = Some(
                    read_info(&value).with_context(|_| format_err!("reading 'i': {:?}", value))?,
                )
            }
            "m" => {
                modified = Some(
                    value
                        .parse::<u64>()
                        .with_context(|_| format_err!("reading 'm': {:?}", value))?,
                )
            }
            "n" => name = Some(value.to_string()),
            "d" => description = Some(value.to_string()),
            "1" => checksum = read_checksum(&value).ok(),

            _ => (), // bail!("unrecognised field value: {:?}", field_name),
        }
    }

    Ok(Doc {
        id: you.ok_or_else(|| err_msg("no 'u'"))?,
        object_info: eye.ok_or_else(|| err_msg("no 'i'"))?,
        modified: modified.ok_or_else(|| err_msg("no modified"))?,
        name,
        description,
        checksum,
    })
}

fn read_fields<R: BufRead>(f: &mut DataInput<R>) -> Result<Option<Vec<(String, String)>>, Error> {
    if f.check_eof()? {
        return Ok(None);
    }

    let field_count = f
        .read_int()
        .with_context(|_| err_msg("reading field count (first field)"))?;

    let field_count = usize(field_count)?;
    let mut ret = Vec::with_capacity(field_count);

    for field in 0..field_count {
        ret.push(read_field(f).with_context(|_| format_err!("reading field {}", field))?);
    }

    Ok(Some(ret))
}

fn read_field<R: BufRead>(f: &mut DataInput<R>) -> Result<(String, String), Error> {
    let flags = u8(f.read_byte()?)?;
    let _flags = FieldFlag::from_bits(flags).ok_or_else(|| err_msg("decoding field flags"))?;

    let name_len = f.inner.read_u16::<BE>()?;
    let name = f.read_utf8(usize(name_len))?;

    // yup, they went out of their way to use signed data here
    let value_len = usize(f.read_int()?)?;
    let value = f.read_utf8(value_len)?;

    Ok((name, value))
}

#[inline]
fn read_checksum(value: &str) -> Result<[u8; 20], Error> {
    let decoded = hex::decode(value).with_context(|_| err_msg("decoding checksum"))?;
    ensure!(20 == decoded.len(), "checksum was wrong length");
    let mut arr = [0u8; 20];
    arr.copy_from_slice(&decoded);
    Ok(arr)
}

fn read_uniq(value: &str) -> Result<UniqId, Error> {
    let mut parts = value.split('|');

    Ok(UniqId {
        group: parts
            .next()
            .ok_or_else(|| err_msg("short uniq: group"))?
            .to_string(),
        artifact: parts
            .next()
            .ok_or_else(|| err_msg("short uniq: artifact"))?
            .to_string(),
        version: parts
            .next()
            .ok_or_else(|| err_msg("short uniq: version"))?
            .to_string(),
        classifier: not_na(
            parts
                .next()
                .ok_or_else(|| err_msg("short uniq: classifier"))?,
        )
        .map(|v| v.to_string()),
        extension: parts.next().map(|v| v.to_string()),
    })
}

fn read_info(value: &str) -> Result<FullInfo, Error> {
    let mut parts = value.split('|');
    Ok(FullInfo {
        packaging: parts
            .next()
            .ok_or_else(|| err_msg("short info: packaging"))?
            .to_string(),
        last_modified: parts
            .next()
            .ok_or_else(|| err_msg("short info: time"))?
            .parse::<u64>()
            .with_context(|_| err_msg("reading time"))?,
        size: read_size(parts.next().ok_or_else(|| err_msg("short i: size"))?)?,
        source_attached: AttachmentStatus::read(
            parts
                .next()
                .ok_or_else(|| err_msg("short info: sources flag"))?,
        )?,
        javadoc_attached: AttachmentStatus::read(
            parts.next().ok_or_else(|| err_msg("short info: flag 2"))?,
        )?,
        signature_attached: AttachmentStatus::read(
            parts.next().ok_or_else(|| err_msg("short info: flag 3"))?,
        )?,
        extension: parts
            .next()
            .ok_or_else(|| err_msg("short info: extension"))?
            .to_string(),
    })
}

fn read_size(value: &str) -> Result<Option<u64>, Error> {
    if "-1" == value {
        return Ok(None);
    }

    Ok(Some(
        value
            .parse::<u64>()
            .with_context(|_| err_msg("reading size"))?,
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
enum AttachmentStatus {
    Absent,
    Present,
    Unavailable,
}

impl AttachmentStatus {
    fn read(value: &str) -> Result<AttachmentStatus, Error> {
        Ok(match value.parse::<u8>() {
            Ok(0) => AttachmentStatus::Absent,
            Ok(1) => AttachmentStatus::Present,
            Ok(2) => AttachmentStatus::Unavailable,
            other => bail!("invalid attachment value: {:?}: {:?}", value, other),
        })
    }
}

struct DataInput<R: BufRead> {
    inner: R,
}

impl<R: BufRead> DataInput<R> {
    fn read_byte(&mut self) -> Result<i8, io::Error> {
        self.inner.read_i8()
    }

    fn read_int(&mut self) -> Result<i32, io::Error> {
        self.inner.read_i32::<BE>()
    }

    fn read_long(&mut self) -> Result<i64, io::Error> {
        self.inner.read_i64::<BE>()
    }

    fn read_utf8(&mut self, len: usize) -> Result<String, Error> {
        if 0 == len {
            return Ok(String::new());
        }
        let mut buf = vec![0u8; usize::from(len)];
        self.inner.read_exact(&mut buf)?;

        match cesu8::from_java_cesu8(&buf) {
            Ok(s) => Ok(s.to_string()),
            Err(e) => Err(format_err!(
                "invalid 'modified' utf-8: {:?}: {:?}",
                e,
                String::from_utf8_lossy(&buf)
            )),
        }
    }

    fn check_eof(&mut self) -> Result<bool, Error> {
        Ok(self.inner.fill_buf()?.is_empty())
    }
}

#[test]
fn sample() -> Result<(), Error> {
    use std::fs;
    let docs = read(io::BufReader::new(fs::File::open("sample-index").unwrap()))?;
    for d in docs {
        if d.id.group == "com.google.guava" && d.id.artifact == "guava" {
            println!("{:?} {:?}", d.id, d.object_info);
        }
    }
    Ok(())
}
