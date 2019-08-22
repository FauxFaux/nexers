use failure::Error;
use rusqlite::Connection;

mod builder;
mod ingest;

pub use self::builder::DbBuilder;
pub use self::ingest::ingest;

pub const SCHEMA: &'static str = include_str!("../../schema.sql");

pub fn find_versions(conn: &Connection, group: &str, artifact: &str) -> Result<Vec<String>, Error> {
    Ok(conn
        .prepare_cached(
            r"
select version from versions
  where group_id=(select id from group_names where name=?)
    and artifact_id=(select id from artifact_names where name=?)",
        )?
        .query_map(&[group, artifact], |row| row.get(0))?
        .collect::<Result<Vec<String>, _>>()?)
}
