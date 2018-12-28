use cast::i64;
use failure::Error;
use insideout::InsideOut;
use rusqlite::types::ToSql;
use rusqlite::OptionalExtension;

use crate::nexus::AttachmentStatus;
use crate::nexus::Doc;

pub struct Db {
    conn: rusqlite::Connection,
}

impl Db {
    pub fn new(conn: rusqlite::Connection) -> Result<Db, Error> {
        Ok(Db { conn })
    }

    pub fn add(&mut self, doc: &Doc) -> Result<(), Error> {
        let group_name = string_write(&mut self.conn, "group_names", &doc.id.group)?;
        let artifact_name = string_write(&mut self.conn, "artifact_names", &doc.id.artifact)?;

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
    conn: &mut rusqlite::Connection,
    table: &'static str,
    val: &str,
) -> Result<i64, Error> {
    if let Some(id) = conn
        .prepare_cached(&format!("select id from {} where name=?", table))?
        .query_row(&[val], |row| row.get(0))
        .optional()?
    {
        return Ok(id);
    }

    Ok(conn
        .prepare_cached(&format!("insert into {} (name) values (?)", table))?
        .insert(&[val])?)
}

fn attached_bool(status: AttachmentStatus) -> Option<bool> {
    match status {
        AttachmentStatus::Absent => Some(false),
        AttachmentStatus::Present => Some(true),
        AttachmentStatus::Unavailable => None,
    }
}
