use std::io;

use failure::Error;

use nexers::db;

#[cfg(feature = "jemallocator")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> Result<(), Error> {
    let conn = rusqlite::Connection::open("maven.db")?;
    conn.execute_batch(include_str!("../schema.sql"))?;
    db::ingest(io::stdin().lock(), conn)?;
    Ok(())
}
