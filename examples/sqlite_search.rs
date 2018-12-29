use std::fs;
use std::io;
use std::mem;
use std::thread;

use failure::format_err;
use failure::Error;

use nexers::sqlite;
use nexers::Doc;
use nexers::Event;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> Result<(), Error> {
    let from = io::BufReader::new(fs::File::open("sample-index")?);
    let mut errors = 0;

    let (send, recv) = crossbeam_channel::bounded(65_536);

    let writer = thread::spawn(|| write(recv));

    let local_error = nexers::read(from, |event| {
        match event {
            Event::Doc(d) => send.send(d)?,

            Event::Error { .. } => errors += 1,
            Event::Delete(_) => (),
        }
        Ok(())
    });

    mem::drop(send);

    writer.join().map_err(|e| format_err!("panic: {:?}", e))??;

    local_error?;

    println!("..and {} errors", errors);

    Ok(())
}

fn write(recv: crossbeam_channel::Receiver<Doc>) -> Result<(), Error> {
    let mut sql = rusqlite::Connection::open("search.db")?;
    sql.execute_batch(include_str!("../schema.sql"))?;
    let tran = sql.transaction()?;
    let mut db = sqlite::DbBuilder::new(tran)?;

    let mut pos = 0usize;

    while let Some(doc) = recv.recv().ok() {
        db.add(&doc)?;

        pos += 1;
        if 0 == pos % 100_000 {
            println!("written: {:?}", pos);
        }
    }
    let tran = db.done()?;

    println!(
        "{:?}",
        sqlite::find_versions(&tran, "com.google.guava", "guava")?
    );

    tran.commit()?;

    Ok(())
}
