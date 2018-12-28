use std::fs;
use std::io;
use std::mem;
use std::sync::mpsc;
use std::thread;

use failure::format_err;
use failure::Error;

use nexers::Doc;
use nexers::Event;

fn main() -> Result<(), Error> {
    let from = io::BufReader::new(fs::File::open("sample-index")?);
    let mut errors = 0;

    let (send, recv) = mpsc::sync_channel(1_024);

    let writer = write(recv);

    nexers::read(from, |event| {
        match event {
            Event::Doc(d) => send.send(d)?,

            Event::Error { .. } => errors += 1,
            Event::Delete(_) => (),
        }
        Ok(())
    })?;

    mem::drop(send);

    writer.join().map_err(|e| format_err!("panic: {:?}", e))??;

    println!("..and {} errors", errors);

    //    println!("{:?}", db.find_versions("com.google.guava", "guava")?);

    Ok(())
}

fn write(recv: mpsc::Receiver<Doc>) -> thread::JoinHandle<Result<(), Error>> {
    thread::spawn(move || -> Result<(), Error> {
        let mut sql = rusqlite::Connection::open("search.db")?;
        sql.execute_batch(include_str!("../schema.sql"))?;
        let tran = sql.transaction()?;
        let mut db = nexers::sqlite::Db::new(tran)?;

        let mut pos = 0usize;

        while let Some(doc) = recv.recv().ok() {
            pos += 1;
            if 0 == pos % 10000 {
                println!("{}", pos);
            }

            db.add(&doc)?;
        }

        db.commit()?;

        Ok(())
    })
}
