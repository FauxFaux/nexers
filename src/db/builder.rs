use std::collections::HashMap;
use std::convert::TryFrom;

use failure::ensure;
use failure::err_msg;
use failure::Error;
use insideout::InsideOut;
use rusqlite::types::ToSql;
use rusqlite::OptionalExtension;

use crate::nexus::AttachmentStatus;
use crate::nexus::Doc;

type Cache = (&'static str, HashMap<String, i64>);

pub struct DbBuilder<'t> {
    conn: &'t rusqlite::Connection,
    group_cache: Cache,
    artifact_cache: Cache,
    name_cache: Cache,
    desc_cache: Cache,
    packaging_cache: Cache,
    classifier_cache: Cache,
}

impl<'t> DbBuilder<'t> {
    pub fn new(conn: &rusqlite::Connection) -> Result<DbBuilder, Error> {
        let mut us = DbBuilder {
            conn,
            group_cache: ("group", HashMap::with_capacity(40 * 1_024)),
            artifact_cache: ("artifact", HashMap::with_capacity(200 * 1_024)),
            name_cache: ("name", HashMap::with_capacity(40 * 1_024)),
            desc_cache: ("desc", HashMap::with_capacity(40 * 1_024)),
            packaging_cache: ("packaging", HashMap::with_capacity(1_024)),
            classifier_cache: ("classifier", HashMap::with_capacity(1_024)),
        };

        us.create_string_tables()?;
        us.write_examples()?;

        Ok(us)
    }

    pub fn create_string_tables(&self) -> Result<(), Error> {
        for (name, _cache) in &[
            &self.group_cache,
            &self.artifact_cache,
            &self.name_cache,
            &self.desc_cache,
            &self.packaging_cache,
            &self.classifier_cache,
        ] {
            self.conn.execute(
                &format!(
                    r"
create table if not exists {}_names (
  id integer primary key,
  name varchar not null unique
)",
                    name
                ),
                rusqlite::NO_PARAMS,
            )?;
        }

        Ok(())
    }

    #[rustfmt::skip]
    pub fn write_examples(&mut self) -> Result<(), Error> {
        write_examples(&self.conn, &mut self.group_cache,      include_str!("top/top_group.txt"))?;
        write_examples(&self.conn, &mut self.artifact_cache,   include_str!("top/top_artifact.txt"))?;
        write_examples(&self.conn, &mut self.classifier_cache, include_str!("top/top_classifier.txt"))?;
        write_examples(&self.conn, &mut self.packaging_cache,  include_str!("top/top_packaging.txt"))?;
        write_examples(&self.conn, &mut self.name_cache,       include_str!("top/top_name.txt"))?;
        write_examples(&self.conn, &mut self.desc_cache,       include_str!("top/top_desc.txt"))?;
        Ok(())
    }

    pub fn add(&mut self, doc: &Doc) -> Result<(), Error> {
        let group_name = string_write(&self.conn, &mut self.group_cache, &doc.id.group)?;
        let artifact_name = string_write(&self.conn, &mut self.artifact_cache, &doc.id.artifact)?;
        let name_name = option_write(&self.conn, &mut self.name_cache, doc.name.as_ref())?;
        let desc_name = option_write(&self.conn, &mut self.desc_cache, doc.description.as_ref())?;

        let shared_cache = &mut self.packaging_cache;
        let pkg_name = option_write(&self.conn, shared_cache, Some(&doc.object_info.packaging))?;
        let ext_name = string_write(&self.conn, shared_cache, &doc.object_info.extension)?;

        let classifier_name = option_write(
            &self.conn,
            &mut self.classifier_cache,
            doc.id.classifier.as_ref(),
        )?;

        self.conn
            .prepare_cached(
                r"
insert into versions
  (
   group_id,
   artifact_id,
   version,
   classifier_id,
   extension_id,

   packaging_id,

   last_modified,
   size,
   checksum,

   source_attached,
   javadoc_attached,
   signature_attached,

   name_id,
   desc_id
  ) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?)
",
            )?
            .insert(&[
                &group_name as &dyn ToSql,
                &artifact_name,
                &doc.id.version,
                &classifier_name,
                &ext_name,
                &pkg_name,
                &i64::try_from(doc.object_info.last_modified / 1000)?,
                &doc.object_info.size.map(i64::try_from).inside_out()?,
                &doc.checksum.map(hex::encode),
                &attached_bool(doc.object_info.source_attached),
                &attached_bool(doc.object_info.javadoc_attached),
                &attached_bool(doc.object_info.signature_attached),
                &name_name,
                &desc_name,
            ])?;
        Ok(())
    }
}

#[inline]
fn option_write(
    conn: &rusqlite::Connection,
    cache: &mut Cache,
    val: Option<&String>,
) -> Result<Option<i64>, Error> {
    val.filter(|name| empty_filter(name.as_str()))
        .map(|name| -> Result<i64, Error> { string_write(conn, cache, name) })
        .inside_out()
}

#[inline]
fn string_write(conn: &rusqlite::Connection, cache: &mut Cache, val: &str) -> Result<i64, Error> {
    let (table, cache) = cache;
    if let Some(id) = cache.get(val) {
        return Ok(*id);
    }

    ensure!(
        empty_filter(val.trim()),
        "illegal string: {}: {:?}",
        table,
        val
    );

    let new_id = match conn
        .prepare_cached(&format!("insert into {}_names (name) values (?)", table))?
        .insert(&[val])
    {
        Ok(id) => id,
        Err(rusqlite::Error::SqliteFailure(e, ref _msg))
            if rusqlite::ErrorCode::ConstraintViolation == e.code =>
        {
            conn.prepare_cached(&format!("select id from {}_names where name=?", table))?
                .query_row(&[val], |row| row.get(0))
                .optional()?
                .ok_or_else(|| err_msg("constraint violation, but row didn't exist"))?
        }
        Err(e) => return Err(e.into()),
    };

    cache.insert(val.to_string(), new_id);

    Ok(new_id)
}

#[inline]
fn write_examples(
    conn: &rusqlite::Connection,
    cache: &mut Cache,
    contents: &'static str,
) -> Result<(), Error> {
    for line in contents.trim().split('\n') {
        string_write(conn, cache, &line.trim().to_string())?;
    }
    Ok(())
}

fn attached_bool(status: AttachmentStatus) -> Option<bool> {
    match status {
        AttachmentStatus::Absent => Some(false),
        AttachmentStatus::Present => Some(true),
        AttachmentStatus::Unavailable => None,
    }
}

fn empty_filter(s: &str) -> bool {
    !s.is_empty() && "null" != s
}
