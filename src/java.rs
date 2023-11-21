use std::io;
use std::io::BufRead;

use anyhow::anyhow;
use anyhow::Result;
use byteorder::ReadBytesExt;
use byteorder::BE;

pub struct DataInput<R: BufRead> {
    inner: R,
}

impl<R: BufRead> DataInput<R> {
    pub fn new(inner: R) -> Self {
        DataInput { inner }
    }

    pub fn read_byte(&mut self) -> Result<i8, io::Error> {
        self.inner.read_i8()
    }

    // `char`? Sigh.
    pub fn read_unsigned_short(&mut self) -> Result<u16, io::Error> {
        self.inner.read_u16::<BE>()
    }

    pub fn read_int(&mut self) -> Result<i32, io::Error> {
        self.inner.read_i32::<BE>()
    }

    pub fn read_long(&mut self) -> Result<i64, io::Error> {
        self.inner.read_i64::<BE>()
    }

    pub fn read_utf8(&mut self, len: usize) -> Result<String> {
        if 0 == len {
            return Ok(String::new());
        }
        let mut buf = vec![0u8; len];
        self.inner.read_exact(&mut buf)?;

        // cesu is a superset of utf-8, so try that first
        let buf = match String::from_utf8(buf) {
            Ok(s) => return Ok(s),
            Err(e) => e.into_bytes(),
        };

        match cesu8::from_java_cesu8(&buf) {
            Ok(s) => Ok(s.to_string()),
            Err(e) => Err(anyhow!(
                "invalid 'modified' utf-8: {:?}: {:?}",
                e,
                String::from_utf8_lossy(&buf)
            )),
        }
    }

    pub fn check_eof(&mut self) -> Result<bool> {
        Ok(self.inner.fill_buf()?.is_empty())
    }
}
