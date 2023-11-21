use std::io;

use anyhow::Result;

use nexers::db;

fn main() -> Result<()> {
    let conn = rusqlite::Connection::open("maven.db")?;
    conn.execute_batch(db::SCHEMA)?;
    db::ingest(io::stdin().lock(), conn)?;
    Ok(())
}
