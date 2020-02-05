use crate::traits::sync::{Query, Transaction};
use crate::AppliedMigration;
use chrono::{DateTime, Local};
use rusqlite::{Connection as RqlConnection, Error as RqlError, NO_PARAMS};

fn query_applied_migrations(
    transaction: &RqlConnection,
    query: &str,
) -> Result<Vec<AppliedMigration>, RqlError> {
    let mut stmt = transaction.prepare(query)?;
    let mut rows = stmt.query(NO_PARAMS)?;
    let mut applied = Vec::new();
    while let Some(row) = rows.next()? {
        let version: isize = row.get(0)?;
        let applied_on: String = row.get(2)?;
        let applied_on = DateTime::parse_from_rfc3339(&applied_on)
            .unwrap()
            .with_timezone(&Local);
        //version, name, installed_on, checksum
        applied.push(AppliedMigration {
            version: version as usize,
            name: row.get(1)?,
            applied_on,
            checksum: row.get(3)?,
        });
    }
    Ok(applied)
}

impl Transaction for RqlConnection {
    type Error = RqlError;
    fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let transaction = self.transaction()?;
        let mut count = 0;
        for query in queries.iter() {
            transaction.execute_batch(query)?;
            count += 1;
        }
        transaction.commit()?;
        Ok(count)
    }
}

impl Query<Vec<AppliedMigration>> for RqlConnection {
    fn query(&mut self, query: &str) -> Result<Option<Vec<AppliedMigration>>, Self::Error> {
        let transaction = self.transaction()?;
        let applied = query_applied_migrations(&transaction, query)?;
        transaction.commit()?;
        Ok(Some(applied))
    }
}
