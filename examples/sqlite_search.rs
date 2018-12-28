use std::io;

use failure::Error;

use nexers::Event;

fn main() -> Result<(), Error> {
    use std::fs;
    let from = io::BufReader::new(fs::File::open("sample-index")?);
    let mut errors = 0;
    let mut sql = rusqlite::Connection::open("search.db")?;
    sql.execute_batch(include_str!("../schema.sql"))?;
    let tran = sql.transaction()?;
    let mut db = nexers::sqlite::Db::new(tran)?;
    let mut pos = 0usize;
    nexers::read(from, |event| {
        pos += 1;
        if 0 == pos % 10000 {
            println!("{}", pos);
        }
        match event {
            Event::Doc(d) => db.add(&d)?,

            Event::Error { .. } => errors += 1,
            Event::Delete(_) => (),
        }
        Ok(())
    })?;

    db.commit()?;
    println!("..and {} errors", errors);

    //    println!("{:?}", db.find_versions("com.google.guava", "guava")?);

    Ok(())
}
