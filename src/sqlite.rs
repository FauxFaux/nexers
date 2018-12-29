use std::collections::HashMap;

use cast::i64;
use failure::err_msg;
use failure::Error;
use insideout::InsideOut;
use rusqlite::types::ToSql;
use rusqlite::OptionalExtension;

use crate::nexus::AttachmentStatus;
use crate::nexus::Doc;

type Cache = (&'static str, HashMap<String, i64>);

pub struct Db<'t> {
    conn: rusqlite::Transaction<'t>,
    group_cache: Cache,
    artifact_cache: Cache,
    name_cache: Cache,
    desc_cache: Cache,
}

impl<'t> Db<'t> {
    pub fn new(conn: rusqlite::Transaction) -> Result<Db, Error> {
        let mut us = Db {
            conn,
            group_cache: ("group_names", HashMap::with_capacity(40 * 1_024)),
            artifact_cache: ("artifact_names", HashMap::with_capacity(200 * 1_024)),
            name_cache: ("name_names", HashMap::with_capacity(40 * 1_024)),
            desc_cache: ("desc_names", HashMap::with_capacity(40 * 1_024)),
        };

        for artifact in &[
            "core",
            "parent",
            "common",
            "library",
            "metrics",
            "logging",
            "utils",
            "bootstrap",
            "management",
            "jenkins",
            "client",
            "prometheus",
            "commons",
            "api",
            "social",
            "scala-library",
            "config",
            "testing",
            "sdk",
            "project",
            "jmx",
            "json",
            "server",
            "model",
            "examples",
        ] {
            string_write(&mut us.conn, &mut us.artifact_cache, &artifact.to_string())?;
        }

        // select (select name from group_names where id=group_id) name,cnt from
        // (select group_id,count(*) cnt from versions group by group_id)
        // order by cast(cnt/10000 as int) desc,name limit 256;
        for group in &[
            "com.google.apis",
            "com.amazonaws",
            "org.wso2.carbon.identity.framework",
            "com.lihaoyi",
            "org.apache.camel",
            "org.wso2.carbon.apimgt",
            "com.liferay",
            "org.apereo.cas",
            "org.webjars.npm",
        ] {
            string_write(&mut us.conn, &mut us.group_cache, &group.to_string())?;
        }

        for name in &[
            "${project.groupId}:${project.artifactId}",
            "${project.artifactId}",
            "${project.groupId}.${project.artifactId}",
            "core",
            "Grails",
            "Groovy",
            "Apache ServiceMix :: Bundles :: ${pkgArtifactId}",
            "Restcomm :: Diameter Resources",
            "Restcomm :: Resources :: ${pom.artifactId}",
        ] {
            string_write(&mut us.conn, &mut us.name_cache, &name.to_string())?;
        }

        for desc in &[
            "${project.name}",
            "Grails Web Application Framework",
            "Groovy: A powerful, dynamic language for the JVM",
            "core",
            "This is the core module of the project.",
            "This OSGi bundle wraps ${pkgArtifactId} ${pkgVersion} jar file.",
        ] {
            string_write(&mut us.conn, &mut us.desc_cache, &desc.to_string())?;
        }

        Ok(us)
    }

    pub fn commit(self) -> Result<(), Error> {
        Ok(self.conn.commit()?)
    }

    pub fn add(&mut self, doc: &Doc) -> Result<(), Error> {
        let group_name = string_write(&self.conn, &mut self.group_cache, &doc.id.group)?;
        let artifact_name = string_write(&self.conn, &mut self.artifact_cache, &doc.id.artifact)?;
        let name_name = option_write(&self.conn, &mut self.name_cache, doc.name.as_ref())?;
        let desc_name = option_write(&self.conn, &mut self.desc_cache, doc.description.as_ref())?;

        self.conn
            .prepare_cached(
                r"
insert into versions
  (
   group_id,
   artifact_id,
   last_modified,
   size,
   source_attached,
   javadoc_attached,
   signature_attached,
   name_id,
   desc_id,
   version,
   classifier,
   packaging,
   extension,
   checksum
  ) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
",
            )?
            .insert(&[
                &group_name as &ToSql,
                &artifact_name,
                &i64(doc.object_info.last_modified / 1000)?,
                &doc.object_info.size.map(|s| i64(s)).inside_out()?,
                &attached_bool(doc.object_info.source_attached),
                &attached_bool(doc.object_info.javadoc_attached),
                &attached_bool(doc.object_info.signature_attached),
                &name_name,
                &desc_name,
                &doc.id.version,
                &null_empty(doc.id.classifier.as_ref()),
                &null_empty(Some(&doc.object_info.packaging)),
                &null_empty(Some(&doc.object_info.extension)),
                &doc.checksum.map(|arr| hex::encode(arr)),
            ])?;

        Ok(())
    }
}

#[inline]
fn option_write(
    conn: &rusqlite::Transaction,
    cache: &mut Cache,
    val: Option<&String>,
) -> Result<Option<i64>, Error> {
    val.filter(|name| empty_filter(name.as_str()))
        .map(|name| -> Result<i64, Error> { string_write(conn, cache, name) })
        .inside_out()
}

#[inline]
fn string_write(
    conn: &rusqlite::Transaction,
    cache: &mut Cache,
    val: &String,
) -> Result<i64, Error> {
    let (table, cache) = cache;
    if let Some(id) = cache.get(val) {
        return Ok(*id);
    }

    let new_id = match conn
        .prepare_cached(&format!("insert into {} (name) values (?)", table))?
        .insert(&[val])
    {
        Ok(id) => id,
        Err(rusqlite::Error::SqliteFailure(e, ref _msg))
            if rusqlite::ErrorCode::ConstraintViolation == e.code =>
        {
            conn.prepare_cached(&format!("select id from {} where name=?", table))?
                .query_row(&[val], |row| row.get(0))
                .optional()?
                .ok_or_else(|| err_msg("constraint violation, but row didn't exist"))?
        }
        Err(e) => Err(e)?,
    };

    cache.insert(val.to_string(), new_id);

    Ok(new_id)
}

fn attached_bool(status: AttachmentStatus) -> Option<bool> {
    match status {
        AttachmentStatus::Absent => Some(false),
        AttachmentStatus::Present => Some(true),
        AttachmentStatus::Unavailable => None,
    }
}

#[inline]
fn null_empty(s: Option<&String>) -> Option<&str> {
    s.map(|s| s.trim()).filter(|s| empty_filter(s))
}

fn empty_filter(s: &str) -> bool {
    !s.is_empty() && "null" != s
}
