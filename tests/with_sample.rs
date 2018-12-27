use std::io;

use failure::Error;

use nexers::Event;

#[test]
fn sample() -> Result<(), Error> {
    use std::fs;
    let from = io::BufReader::new(fs::File::open("sample-index").unwrap());
    let mut errors = 0;
    nexers::read(from, |event| {
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

#[cfg(never)]
fn print() {
    for (e, fields) in &errors {
        println!("Error in doc:");
        for (name, value) in fields {
            println!(" * {:?}: {:?}", name, value);
        }
        println!("{:?}", e);
        println!();
    }

    println!(
        "{} errors, {} deletions, {} docs",
        errors.len(),
        deletions.len(),
        docs.len()
    );

    docs.retain(|v| !deletions.contains(&v.id));

    println!("{} live docs", docs.len());
}
