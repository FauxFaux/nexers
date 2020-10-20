use std::io;

use anyhow::Result;

use nexers::db;

#[cfg(feature = "jemallocator")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> Result<()> {
    let conn = rusqlite::Connection::open("maven.db")?;
    conn.execute_batch(db::SCHEMA)?;
    db::ingest(io::stdin().lock(), conn)?;
    Ok(())
}
