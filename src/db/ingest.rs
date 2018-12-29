use std::io;
use std::mem;
use std::thread;

use failure::format_err;
use failure::Error;

use crate::db;
use crate::nexus::Doc;
use crate::nexus::Event;

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

pub fn ingest<R: io::BufRead>(from: R, tran: rusqlite::Connection) -> Result<Vec<Error>, Error> {
    let mut errors = Vec::new();

    let (send, recv) = channel::new();

    let writer = thread::spawn(move || write(tran, recv));

    let local_error = crate::nexus::read(from, |event| {
        match event {
            Event::Doc(d) => send.send(d)?,

            Event::Error { error, raw: _ } => errors.push(error),
            Event::Delete(_) => (),
        }
        Ok(())
    });

    mem::drop(send);

    writer.join().map_err(|e| format_err!("panic: {:?}", e))??;

    local_error?;

    Ok(errors)
}

fn write(mut tran: rusqlite::Connection, recv: channel::Receiver) -> Result<(), Error> {
    let mut db = db::DbBuilder::new(tran.transaction()?)?;

    while let Some(doc) = recv.recv().ok() {
        db.add(&doc)?;
    }

    db.done()?.commit()?;

    Ok(())
}
