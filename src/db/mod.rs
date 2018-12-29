use failure::Error;
use rusqlite::Transaction;

mod builder;
mod ingest;

pub use self::builder::DbBuilder;
pub use self::ingest::ingest;

pub fn find_versions(
    conn: &Transaction,
    group: &str,
    artifact: &str,
) -> Result<Vec<String>, Error> {
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
