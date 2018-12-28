use std::fs;
use std::io;
use std::mem;
use std::thread;

use failure::format_err;
use failure::Error;

use nexers::Doc;
use nexers::Event;

#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

fn main() -> Result<(), Error> {
    let from = io::BufReader::new(fs::File::open("sample-index")?);
    let mut errors = 0;

    let (send, recv) = crossbeam_channel::bounded(65_536);

    let writer = thread::spawn(|| write(recv));

    let mut pos = 0usize;

    let local_error = nexers::read(from, |event| {
        match event {
            Event::Doc(d) => {
                pos += 1;
                if 0 == pos % 100_000 {
                    println!("in: {}", pos);
                }
                send.send(d)?
            },

            Event::Error { .. } => errors += 1,
            Event::Delete(_) => (),
        }
        Ok(())
    });

    mem::drop(send);

    writer.join().map_err(|e| format_err!("panic: {:?}", e))??;

    local_error?;

    println!("..and {} errors", errors);

    //    println!("{:?}", db.find_versions("com.google.guava", "guava")?);

    Ok(())
}

fn write(recv: crossbeam_channel::Receiver<Doc>) -> Result<(), Error> {
    let mut sql = rusqlite::Connection::open("search.db")?;
    sql.execute_batch(include_str!("../schema.sql"))?;
    let tran = sql.transaction()?;
    let mut db = nexers::sqlite::Db::new(tran)?;

    let mut pos = 0usize;

    while let Some(doc) = recv.recv().ok() {
        pos += 1;
        if 0 == pos % 100_000 {
            println!("ou: {:?}", pos);
        }

        db.add(&doc)?;
    }

    db.commit()?;

    Ok(())
}
