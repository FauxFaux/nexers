use std::fs;
use std::io;

use failure::Error;

use nexers::db;

#[cfg(feature = "jemallocator")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> Result<(), Error> {
    let from = io::BufReader::new(fs::File::open("sample-index")?);
    let sql = rusqlite::Connection::open("search.db")?;
    sql.execute_batch(include_str!("../schema.sql"))?;
    let errors = db::ingest(from, sql)?;
    println!("..and {} errors", errors.len());

    Ok(())
}
