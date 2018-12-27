use std::io;

use failure::Error;

use nexers::Event;

fn main() -> Result<(), Error> {
    use std::fs;
    let from = io::BufReader::new(fs::File::open("sample-index")?);
    let mut errors = 0;
    let mut db = nexers::Db::default();
    nexers::read(from, |event| {
        match event {
            Event::Doc(d) => db.add(&d)?,

            Event::Error { .. } => errors += 1,
            Event::Delete(_) => (),
        }
        Ok(())
    })?;
    println!("..and {} errors", errors);
    db.stats();

    println!("{:?}", db.find_versions("com.google.guava", "guava")?);

    Ok(())
}
