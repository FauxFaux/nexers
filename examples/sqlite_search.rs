use std::fs;
use std::io;

use anyhow::Result;

use nexers::db;

#[cfg(feature = "jemallocator")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> Result<()> {
    let from = io::BufReader::new(fs::File::open("sample-index")?);
    let conn = rusqlite::Connection::open("search.db")?;
    conn.execute_batch(db::SCHEMA)?;
    let conn = db::ingest(from, conn)?;

    println!(
        "{:?}",
        db::find_versions(&conn, "com.google.guava", "guava")?
    );

    Ok(())
}
