use std::collections::HashMap;

use failure::err_msg;
use failure::Error;

use crate::nexus::AttachmentStatus;
use crate::nexus::Checksum;
use crate::nexus::Doc;

type StringId = usize;

type GroupId = StringId;
type ArtifactId = StringId;
type VersionId = StringId;
type ClassifierId = StringId;
type ExtensionId = StringId;

type PackagingId = StringId;
type NameId = StringId;
type DescriptionId = StringId;

#[derive(Debug, Default, Clone)]
pub struct Db {
    groups: HashMap<GroupId, Group>,

    group_pool: StringPool,
    artifact_pool: StringPool,
    version_pool: StringPool,
    classifier_pool: StringPool,
    extension_pool: StringPool,
    packaging_pool: StringPool,
    name_pool: StringPool,
    description_pool: StringPool,
}

#[derive(Debug, Default, Clone)]
struct Group {
    artifacts: HashMap<ArtifactId, Artifact>,
}

#[derive(Debug, Default, Clone)]
struct Artifact {
    versions: HashMap<VersionId, Version>,
}

#[derive(Debug, Default, Copy, Clone, Hash, PartialEq, Eq)]
struct ClassifierKey {
    classifier: ClassifierId,
    extension: ExtensionId,
}

#[derive(Debug, Default, Clone)]
struct Version {
    docs: HashMap<ClassifierKey, Record>,
}

#[derive(Copy, Clone, Debug, Eq, PartialEq)]
struct Record {
    packaging: PackagingId,
    last_modified: u64,
    size: Option<u64>,
    source_attached: AttachmentStatus,
    javadoc_attached: AttachmentStatus,
    signature_attached: AttachmentStatus,
    name: NameId,
    description: DescriptionId,
    checksum: Option<Checksum>,
}

impl Db {
    pub fn add(&mut self, doc: &Doc) -> Result<(), Error> {
        self.groups
            .entry(self.group_pool.insert(&doc.id.group))
            .or_default()
            .artifacts
            .entry(self.artifact_pool.insert(&doc.id.artifact))
            .or_default()
            .versions
            .entry(self.version_pool.insert(&doc.id.version))
            .or_default()
            .docs
            .insert(
                ClassifierKey {
                    classifier: self
                        .classifier_pool
                        .insert_option(doc.id.classifier.as_ref()),
                    extension: self.extension_pool.insert_option(doc.id.extension.as_ref()),
                },
                Record {
                    packaging: self.packaging_pool.insert(&doc.object_info.packaging),
                    last_modified: doc.object_info.last_modified,
                    size: doc.object_info.size,
                    source_attached: doc.object_info.source_attached,
                    javadoc_attached: doc.object_info.javadoc_attached,
                    signature_attached: doc.object_info.signature_attached,
                    name: self.name_pool.insert_option(doc.name.as_ref()),
                    description: self
                        .description_pool
                        .insert_option(doc.description.as_ref()),
                    checksum: doc.checksum,
                },
            );
        Ok(())
    }

    pub fn stats(&self) {
        println!("{} groups", self.groups.len());
        println!(
            "g/a/v pool: {} / {} / {}",
            self.group_pool.len(),
            self.artifact_pool.len(),
            self.version_pool.len()
        );
    }

    pub fn find_versions(&self, group: &str, artifact: &str) -> Result<Vec<&str>, Error> {
        let group = self
            .group_pool
            .get(group)
            .ok_or_else(|| err_msg("no such group"))?;

        let artifact = self
            .artifact_pool
            .get(artifact)
            .ok_or_else(|| err_msg("no such artifact"))?;

        let mut ret = Vec::with_capacity(10);

        for version in self
            .groups
            .get(&group)
            .ok_or_else(|| err_msg("group, but no artifacts??"))?
            .artifacts
            .get(&artifact)
            .ok_or_else(|| err_msg("artifact, but no versions??"))?
            .versions
            .keys()
        {
            ret.push(self.version_pool.invert(*version).expect("internal"));
        }

        Ok(ret)
    }
}

#[derive(Debug, Default, Clone)]
struct StringPool {
    inner: HashMap<String, usize>,
}

impl StringPool {
    fn get<S: AsRef<str>>(&self, key: S) -> Option<StringId> {
        self.inner.get(key.as_ref()).map(|x| *x)
    }

    fn insert_option<S: ToString>(&mut self, val: Option<S>) -> StringId {
        match val {
            None => 0,
            Some(s) => self.insert(s),
        }
    }

    fn insert<S: ToString>(&mut self, val: S) -> StringId {
        let val = val.to_string();
        // BORROW CHECKER
        let next_val = self.inner.len() + 1 /* 0 == None */;
        *self.inner.entry(val).or_insert(next_val)
    }

    fn invert(&self, val: StringId) -> Option<&str> {
        for (k, v) in &self.inner {
            if val == *v {
                return Some(k.as_ref());
            }
        }

        None
    }

    fn len(&self) -> usize {
        self.inner.len()
    }
}
