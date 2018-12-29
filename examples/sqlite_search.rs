use std::fs;
use std::io;
use std::mem;
use std::thread;

use failure::format_err;
use failure::Error;

use nexers::sqlite;
use nexers::Doc;
use nexers::Event;

#[cfg(feature = "jemallocator")]
#[global_allocator]
static ALLOC: jemallocator::Jemalloc = jemallocator::Jemalloc;

#[cfg(feature = "crossbeam-channel")]
mod channel {
    pub type Sender = crossbeam_channel::Sender<super::Doc>;
    pub type Receiver = crossbeam_channel::Receiver<super::Doc>;
    pub fn new() -> (Sender, Receiver) {
        crossbeam_channel::bounded(65_536)
    }
}

#[cfg(not(feature = "crossbeam-channel"))]
mod channel {
    use std::sync::mpsc;
    pub type Sender = mpsc::SyncSender<super::Doc>;
    pub type Receiver = mpsc::Receiver<super::Doc>;
    pub fn new() -> (Sender, Receiver) {
        mpsc::sync_channel(65_536)
    }
}

fn main() -> Result<(), Error> {
    let from = io::BufReader::new(fs::File::open("sample-index")?);
    let mut errors = 0;

    let (send, recv) = channel::new();

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

fn write(recv: channel::Receiver) -> Result<(), Error> {
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
