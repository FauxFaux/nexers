use std::collections::HashMap;

use cast::i64;
use failure::Error;
use insideout::InsideOut;
use rusqlite::types::ToSql;
use rusqlite::OptionalExtension;

use crate::nexus::AttachmentStatus;
use crate::nexus::Doc;

type Cache = HashMap<String, i64>;

pub struct Db<'t> {
    conn: rusqlite::Transaction<'t>,
    group_cache: Cache,
    artifact_cache: Cache,
}

impl<'t> Db<'t> {
    pub fn new(conn: rusqlite::Transaction) -> Result<Db, Error> {
        Ok(Db {
            conn,
            group_cache: Cache::with_capacity(1_000),
            artifact_cache: Cache::with_capacity(1_000),
        })
    }

    pub fn commit(self) -> Result<(), Error> {
        Ok(self.conn.commit()?)
    }

    pub fn add(&mut self, doc: &Doc) -> Result<(), Error> {
        let group_name = string_write(
            &self.conn,
            "group_names",
            &mut self.group_cache,
            &doc.id.group,
        )?;
        let artifact_name = string_write(
            &self.conn,
            "artifact_names",
            &mut self.artifact_cache,
            &doc.id.artifact,
        )?;

        let version_id = self
            .conn
            .prepare_cached(
                r"
insert into versions
  (
   last_modified,
   size,
   source_attached,
   javadoc_attached,
   signature_attached,
   version,
   classifier,
   packaging,
   extension,
   name,
   description,
   checksum
  ) values (?,?,?,?,?,?,?,?,?,?,?,?)
",
            )?
            .insert(&[
                &i64(doc.object_info.last_modified)? as &ToSql,
                &doc.object_info.size.map(|s| i64(s)).inside_out()?,
                &attached_bool(doc.object_info.source_attached),
                &attached_bool(doc.object_info.javadoc_attached),
                &attached_bool(doc.object_info.signature_attached),
                &doc.id.version,
                &doc.id.classifier,
                &doc.object_info.packaging,
                &doc.object_info.extension,
                &doc.name,
                &doc.description,
                &doc.checksum.map(|arr| hex::encode(arr)),
            ])?;

        self.conn
            .prepare_cached("insert into group_artifact (group_name, artifact_name) values (?,?)")?
            .insert(&[group_name, artifact_name])?;

        self.conn
            .prepare_cached(
                "insert into artifact_version (artifact_name, version_id) values (?,?)",
            )?
            .insert(&[artifact_name, version_id])?;

        Ok(())
    }
}

#[inline]
fn string_write(
    conn: &rusqlite::Transaction,
    table: &'static str,
    cache: &mut Cache,
    val: &str,
) -> Result<i64, Error> {
    if let Some(id) = cache.get(val) {
        return Ok(*id);
    }

    if let Some(id) = conn
        .prepare_cached(&format!("select id from {} where name=?", table))?
        .query_row(&[val], |row| row.get(0))
        .optional()?
    {
        return Ok(id);
    }

    let new_id = conn
        .prepare_cached(&format!("insert into {} (name) values (?)", table))?
        .insert(&[val])?;

    if cache.len() >= 8192 {
        let target = (new_id % 3) as i64;
        cache.retain(|_name, id| *id % 3 == target);
        println!("bye, {} cache!", table);
    }

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
