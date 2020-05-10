use crate::traits::sync::{Query, Transaction};
use crate::AppliedMigration;
use chrono::{DateTime, Local};
use postgres::{Client as PgClient, Error as PgError, Transaction as PgTransaction};

fn query_applied_migrations(
    transaction: &mut PgTransaction,
    query: &str,
) -> Result<Vec<AppliedMigration>, PgError> {
    let rows = transaction.query(query, &[])?;
    let mut applied = Vec::new();
    for row in rows.into_iter() {
        let version = row.get(0);
        let applied_on: String = row.get(2);
        let applied_on = DateTime::parse_from_rfc3339(&applied_on)
            .unwrap()
            .with_timezone(&Local);

        applied.push(AppliedMigration {
            version,
            name: row.get(1),
            applied_on,
            checksum: row.get(3),
        });
    }
    Ok(applied)
}

impl Transaction for PgClient {
    type Error = PgError;

    fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let mut transaction = PgClient::transaction(self)?;
        let mut count = 0;
        for query in queries.iter() {
            PgTransaction::batch_execute(&mut transaction, query)?;
            count += 1;
        }
        transaction.commit()?;
        Ok(count as usize)
    }
}

impl Query<Vec<AppliedMigration>> for PgClient {
    fn query(&mut self, query: &str) -> Result<Vec<AppliedMigration>, Self::Error> {
        let mut transaction = PgClient::transaction(self)?;
        let applied = query_applied_migrations(&mut transaction, query)?;
        transaction.commit()?;
        Ok(applied)
    }
}
