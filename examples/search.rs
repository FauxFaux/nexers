use std::env;
use std::fs;
use std::io;

use anyhow::Result;

use nexers::nexus::Event;

fn main() -> Result<()> {
    let args = env::args().skip(1).collect::<Vec<_>>();
    let path = args.get(0).map(|s| s.as_str()).unwrap_or("sample-index");
    let from = io::BufReader::new(fs::File::open(path)?);
    let mut errors = 0;
    nexers::nexus::read(from, |event| {
        match event {
            Event::Doc(d) => {
                if d.id.group == "com.google.guava" && d.id.artifact == "guava" {
                    println!("{:?} {:?}", d.id, d.object_info);
                }
            }
            Event::Error { .. } => errors += 1,
            Event::Delete(_) => (),
        }
        Ok(())
    })?;
    println!("..and {errors} errors");
    Ok(())
}
