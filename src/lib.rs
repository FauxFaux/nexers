use std::io;
use std::io::Read;

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

bitflags! {
    struct FieldFlag: u8 {
        const INDEXED    = 0b0000_0001;
        const TOKENIZED  = 0b0000_0010;
        const STORED     = 0b0000_0100;
        const COMPRESSED = 0b0000_1000;
    }
}

struct You {
    group: String,
    artifact: String,
    version: String,
    trail: Option<Trail>,
}

struct Trail {
    sources: String,
    packaging: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct Eye {
    packaging_1: String,
    some_time: u64,
    size: Option<u64>,
    flag_1: u8,
    flag_2: u8,
    flag_3: u8,
    packaging_2: String,
}

struct Doc {
    you: You,
    eye: Eye,
    modified: u64,
    name: Option<String>,
    description: Option<String>,
    checksum: Option<[u8; 20]>,
}

pub fn read<R: Read>(f: R) -> Result<(), Error> {
    let mut f = DataInput { inner: f };

    ensure!(1 == f.read_byte()?, "version byte");
    let _timestamp_ms = f.read_long()?;

    let mut docs = Vec::with_capacity(1_000);

    let mut errors = Vec::with_capacity(32);

    loop {
        let fields = read_fields(&mut f).with_context(|_| err_msg("reading fields"))?;
        if fields.iter().find(|(key, _value)| "del" == key).is_some() {
            continue;
        }
        match read_doc(&fields) {
            Ok(doc) => docs.push(doc),
            Err(e) => {
                println!("Error in doc:");
                for (name, value) in &fields {
                    println!(" * {:?}: {:?}", name, value);
                }
                println!("{:?}", e);
                errors.push((e, fields))
            }
        }
    }
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
                you =
                    Some(read_u(&value).with_context(|_| format_err!("reading 'u': {:?}", value))?)
            }
            "i" => {
                eye =
                    Some(read_i(&value).with_context(|_| format_err!("reading 'i': {:?}", value))?)
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
            "1" => {
                checksum = Some(
                    read_checksum(&value)
                        .with_context(|_| format_err!("reading '1': {:?}", value))?,
                )
            }
            _ => (), // bail!("unrecognised field value: {:?}", field_name),
        }
    }

    Ok(Doc {
        you: you.ok_or_else(|| err_msg("no 'u'"))?,
        eye: eye.ok_or_else(|| err_msg("no 'i'"))?,
        modified: modified.ok_or_else(|| err_msg("no modified"))?,
        name,
        description,
        checksum,
    })
}

fn read_fields<R: Read>(f: &mut DataInput<R>) -> Result<Vec<(String, String)>, Error> {
    // TODO: err_msg here
    let field_count = f
        .read_int()
        .with_context(|_| err_msg("reading field count (first field: eof -> end?)"))?;

    let field_count = usize(field_count)?;
    let mut ret = Vec::with_capacity(field_count);

    for field in 0..field_count {
        ret.push(read_field(f).with_context(|_| format_err!("reading field {}", field))?);
    }

    Ok(ret)
}

fn read_field<R: Read>(f: &mut DataInput<R>) -> Result<(String, String), Error> {
    let flags = u8(f.read_byte()?)?;
    let _flags = FieldFlag::from_bits(flags).ok_or_else(|| err_msg("decoding field flags"))?;

    let name_len = f.inner.read_u16::<BE>()?;
    let name = f.read_utf8(usize(name_len))?;

    // yup, they went out of their way to use signed data here
    let value_len = usize(f.read_int()?)?;
    let value = f.read_utf8(value_len)?;

    Ok((name, value))
}

fn read_checksum(value: &str) -> Result<[u8; 20], Error> {
    let decoded = hex::decode(value).with_context(|_| err_msg("decoding checksum"))?;
    ensure!(20 == decoded.len(), "checksum was wrong length");
    let mut arr = [0u8; 20];
    arr.copy_from_slice(&decoded);
    Ok(arr)
}

fn read_u(value: &str) -> Result<You, Error> {
    let mut parts = value.split('|');
    Ok(You {
        group: parts
            .next()
            .ok_or_else(|| err_msg("short i: p1"))?
            .to_string(),
        artifact: parts
            .next()
            .ok_or_else(|| err_msg("short i: p1"))?
            .to_string(),
        version: parts
            .next()
            .ok_or_else(|| err_msg("short i: p1"))?
            .to_string(),
        // TODO
        trail: None,
    })
}

fn read_i(value: &str) -> Result<Eye, Error> {
    let mut parts = value.split('|');
    Ok(Eye {
        packaging_1: parts
            .next()
            .ok_or_else(|| err_msg("short i: p1"))?
            .to_string(),
        some_time: parts
            .next()
            .ok_or_else(|| err_msg("short i: time"))?
            .parse::<u64>()
            .with_context(|_| err_msg("reading time"))?,
        size: read_size(parts.next().ok_or_else(|| err_msg("short i: size"))?)?,
        flag_1: parts
            .next()
            .ok_or_else(|| err_msg("short i: flag 1"))?
            .parse::<u8>()
            .with_context(|_| err_msg("reading flag 1"))?,
        flag_2: parts
            .next()
            .ok_or_else(|| err_msg("short i: flag 2"))?
            .parse::<u8>()
            .with_context(|_| err_msg("reading flag 2"))?,
        flag_3: parts
            .next()
            .ok_or_else(|| err_msg("short i: flag 3"))?
            .parse::<u8>()
            .with_context(|_| err_msg("reading flag 3"))?,
        packaging_2: parts
            .next()
            .ok_or_else(|| err_msg("short i: p2"))?
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

struct DataInput<R: Read> {
    inner: R,
}

impl<R: Read> DataInput<R> {
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
}

#[test]
fn sample() -> Result<(), Error> {
    use std::fs;
    read(io::BufReader::new(fs::File::open("sample-index").unwrap()))
}
