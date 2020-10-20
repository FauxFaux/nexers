use std::io;

use anyhow::Result;

use nexers::nexus::Event;

fn main() -> Result<()> {
    use std::fs;
    let from = io::BufReader::new(fs::File::open("sample-index")?);
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
    println!("..and {} errors", errors);
    Ok(())
}
