use postgres::{Client as PgClient, Error as PgError, Transaction as PgTransaction};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::executor::{Executor, QuerySchemaHistory};
use crate::{Migrate, Migration, MigrationContent};

fn query_applied_migrations(
    transaction: &mut PgTransaction,
    query: &str,
) -> Result<Vec<Migration>, PgError> {
    let rows = transaction.query(query, &[])?;
    let mut applied = Vec::new();
    for row in rows.into_iter() {
        let version = row.get(0);
        let applied_on: String = row.get(2);
        // Safe to call unwrap, as we stored it in RFC3339 format on the database
        let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339).unwrap();

        let checksum: String = row.get(3);

        applied.push(Migration::applied(
            version,
            row.get(1),
            applied_on,
            checksum
                .parse::<u64>()
                .expect("checksum must be a valid u64"),
        ));
    }
    Ok(applied)
}

impl Executor for PgClient {
    type Error = PgError;

    fn execute_grouped<'a, T: Iterator<Item = &'a str>>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let mut tx = self.transaction()?;
        let mut count: usize = 0;
        for query in queries {
            tx.batch_execute(query)?;
            count += 1;
        }
        tx.commit()?;
        Ok(count)
    }

    fn execute<'a, T>(&mut self, queries: T) -> Result<usize, Self::Error>
    where
        T: Iterator<Item = (&'a MigrationContent, &'a str)>,
    {
        let mut count: usize = 0;
        for (content, update) in queries {
            if content.no_transaction() {
                self.batch_execute(content.sql())?;
                if let Err(e) = self.batch_execute(update) {
                    log::error!("applied migration but schema history table update failed");
                    return Err(e);
                }
                count += 2;
            } else {
                let mut tx = self.transaction()?;
                tx.batch_execute(content.sql())?;
                tx.batch_execute(update)?;
                tx.commit()?;
                count += 2;
            }
        }

        Ok(count)
    }
}

impl QuerySchemaHistory<Vec<Migration>> for PgClient {
    fn query_schema_history(&mut self, query: &str) -> Result<Vec<Migration>, Self::Error> {
        let mut transaction = PgClient::transaction(self)?;
        let applied = query_applied_migrations(&mut transaction, query)?;
        transaction.commit()?;
        Ok(applied)
    }
}

impl Migrate for PgClient {}
