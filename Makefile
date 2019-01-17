all: maven.db

maven.db: nexus-maven-repository-index.gz
	rm -f maven.db
	zcat nexus-maven-repository-index.gz | cargo run --features=jemallocator --release --example build_db

nexus-maven-repository-index.gz: FORCE
	wget -N https://repo1.maven.org/maven2/.index/nexus-maven-repository-index.gz

.PHONY: all

FORCE:
