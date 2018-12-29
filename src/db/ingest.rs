use std::io;
use std::mem;
use std::thread;

use failure::format_err;
use failure::Error;
use failure::ResultExt;

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

pub fn ingest<R: io::BufRead>(
    from: R,
    conn: rusqlite::Connection,
) -> Result<rusqlite::Connection, Error> {
    let (send, recv) = channel::new();

    let writer = thread::spawn(move || write(conn, recv));

    let local_error = crate::nexus::read(from, |event| {
        match event {
            Event::Doc(d) => send.send(d)?,

            Event::Error { error, raw } => {
                Err(error).with_context(|_| format_err!("processing {:?}", raw))?
            }
            Event::Delete(_) => (),
        }
        Ok(())
    });

    mem::drop(send);

    let conn = writer.join().map_err(|e| format_err!("panic: {:?}", e))??;

    local_error?;

    Ok(conn)
}

fn write(
    mut conn: rusqlite::Connection,
    recv: channel::Receiver,
) -> Result<rusqlite::Connection, Error> {
    let tran = conn.transaction()?;

    {
        let mut db = db::DbBuilder::new(&tran)?;
        while let Some(doc) = recv.recv().ok() {
            db.add(&doc)?;
        }
    }

    tran.commit()?;

    Ok(conn)
}
