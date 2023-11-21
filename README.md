# nexers 

[![](https://img.shields.io/crates/v/nexers.svg)](https://crates.io/crates/nexers)

`nexers` parses Nexus repository indexes, including the one provided by
[maven central](https://central.sonatype.com/). It can optionally build a relational
database based on this data.


## Usage

Build `maven.db` from the latest index, using `pv` to report status and `zcat` to unpack:

```shell
wget -N https://repo1.maven.org/maven2/.index/nexus-maven-repository-index.gz
pv nexus-maven-repository-index.gz \
  | zcat \
  | cargo run --release --example build_db
```


## Minimum Supported Rust Version (MSRV)

`rusqlite` does not commit to an MSRV, so we can't, either.


## License

Licensed under either of

 * Apache License, Version 2.0
 * MIT license

at your option.


### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in the work by you, as defined in the Apache-2.0 license, shall be dual licensed as above, without any
additional terms or conditions.
