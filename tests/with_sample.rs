use std::io;

use anyhow::Result;

use nexers::nexus::Event;

#[test]
fn load() -> Result<()> {
    let mut events = Vec::with_capacity(2);

    nexers::nexus::read(
        io::BufReader::new(io::Cursor::new(&include_bytes!("tiny-file")[..])),
        |ev| {
            events.push(ev);
            Ok(())
        },
    )?;

    assert_eq!(2, events.len());

    let d = match &events[0] {
        Event::Doc(d) => d,
        other => panic!("unexpected event: {:?}", other),
    };

    assert_eq!("yom", d.id.group);
    assert_eq!("yom", d.id.artifact);
    assert_eq!("1.0-alpha-2", d.id.version);
    assert_eq!(None, d.id.classifier);
    assert_eq!("jar", d.object_info.packaging);

    let d = match &events[1] {
        Event::Doc(d) => d,
        other => panic!("unexpected event: {:?}", other),
    };

    assert_eq!("yom", d.id.group);
    assert_eq!("yom", d.id.artifact);
    assert_eq!("1.0-alpha-1", d.id.version);
    assert_eq!(None, d.id.classifier);
    assert_eq!("jar", d.object_info.packaging);

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
