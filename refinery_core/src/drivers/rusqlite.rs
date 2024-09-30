use crate::traits::sync::{Executor, Migrate, QuerySchemaHistory};
use crate::Migration;
use rusqlite::{Connection as RqlConnection, Error as RqlError};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

fn query_applied_migrations(
    transaction: &RqlConnection,
    query: &str,
) -> Result<Vec<Migration>, RqlError> {
    let mut stmt = transaction.prepare(query)?;
    let mut rows = stmt.query([])?;
    let mut applied = Vec::new();
    while let Some(row) = rows.next()? {
        let version = row.get(0)?;
        let applied_on: String = row.get(2)?;
        // Safe to call unwrap, as we stored it in RFC3339 format on the database
        let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339).unwrap();

        let checksum: String = row.get(3)?;
        applied.push(Migration::applied(
            version,
            row.get(1)?,
            applied_on,
            checksum
                .parse::<u64>()
                .expect("checksum must be a valid u64"),
        ));
    }
    Ok(applied)
}

impl Executor for RqlConnection {
    type Error = RqlError;

    fn execute_grouped<'a, T: Iterator<Item = &'a str>>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let tx = self.transaction()?;
        let mut count: usize = 0;
        for query in queries {
            tx.execute_batch(query)?;
            count += 1;
        }
        tx.commit()?;

        Ok(count)
    }

    fn execute<'a, T>(&mut self, queries: T) -> Result<usize, Self::Error>
    where
        T: Iterator<Item = (&'a crate::MigrationContent, &'a str)>,
    {
        let mut count: usize = 0;
        for (content, update) in queries {
            if content.no_transaction() {
                self.execute_batch(content.sql())?;
                if let Err(e) = self.execute_batch(update) {
                    log::error!("applied migration but schema history table update failed");
                    return Err(e);
                }
                count += 2;
            } else {
                let tx = self.transaction()?;
                tx.execute_batch(content.sql())?;
                tx.execute_batch(update)?;
                tx.commit()?;
                count += 2;
            }
        }

        Ok(count)
    }
}

impl QuerySchemaHistory<Vec<Migration>> for RqlConnection {
    fn query_schema_history(&mut self, query: &str) -> Result<Vec<Migration>, Self::Error> {
        let transaction = self.transaction()?;
        let applied = query_applied_migrations(&transaction, query)?;
        transaction.commit()?;
        Ok(applied)
    }
}

impl Migrate for RqlConnection {}
