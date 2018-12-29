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
    name_desc_cache: HashMap<(String, String), i64>,
}

impl<'t> Db<'t> {
    pub fn new(conn: rusqlite::Transaction) -> Result<Db, Error> {
        let mut us = Db {
            conn,
            group_cache: Cache::with_capacity(40 * 1_024),
            artifact_cache: Cache::with_capacity(200 * 1_024),
            name_desc_cache: HashMap::with_capacity(100 * 1_024),
        };

        // ensure the blank name/desc gets id=0; small in the db
        name_desc_write(
            &mut us.conn,
            &mut us.name_desc_cache,
            &Some(String::new()),
            &Some(String::new()),
        )?;

        Ok(us)
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

        let desc_name = name_desc_write(
            &self.conn,
            &mut self.name_desc_cache,
            &doc.name,
            &doc.description,
        )?;

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
   name_desc_id,
   version,
   classifier,
   packaging,
   extension,
   checksum
  ) values (?,?,?,?,?,?,?,?,?,?,?,?,?)
",
            )?
            .insert(&[
                &group_name as &ToSql,
                &artifact_name,
                &i64(doc.object_info.last_modified)?,
                &doc.object_info.size.map(|s| i64(s)).inside_out()?,
                &attached_bool(doc.object_info.source_attached),
                &attached_bool(doc.object_info.javadoc_attached),
                &attached_bool(doc.object_info.signature_attached),
                &desc_name,
                &doc.id.version,
                &doc.id.classifier,
                &doc.object_info.packaging,
                &doc.object_info.extension,
                &doc.checksum.map(|arr| hex::encode(arr)),
            ])?;

        Ok(())
    }
}

#[inline]
fn string_write(
    conn: &rusqlite::Transaction,
    table: &'static str,
    cache: &mut Cache,
    val: &String,
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

    cache.insert(val.to_string(), new_id);

    Ok(new_id)
}

fn name_desc_write(
    conn: &rusqlite::Transaction,
    cache: &mut HashMap<(String, String), i64>,
    name: &Option<String>,
    desc: &Option<String>,
) -> Result<i64, Error> {
    let name = name.clone().unwrap_or_default();
    let desc = desc.clone().unwrap_or_default();

    if let Some(id) = cache.get(&(name.to_string(), desc.to_string())) {
        return Ok(*id);
    }

    if let Some(id) = conn
        .prepare_cached("select id from full_descriptions where name=? and description=?")?
        .query_row(&[&name, &desc], |row| row.get(0))
        .optional()?
    {
        return Ok(id);
    }

    let new_id = conn
        .prepare_cached("insert into full_descriptions (name, description) values (?,?)")?
        .insert(&[&name, &desc])?;

    cache.insert((name, desc), new_id);

    Ok(new_id)
}

fn attached_bool(status: AttachmentStatus) -> Option<bool> {
    match status {
        AttachmentStatus::Absent => Some(false),
        AttachmentStatus::Present => Some(true),
        AttachmentStatus::Unavailable => None,
    }
}
