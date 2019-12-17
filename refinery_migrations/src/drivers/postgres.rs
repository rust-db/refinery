use crate::{AppliedMigration, Query, Transaction};
use chrono::{DateTime, Local};
use postgres::{
    transaction::Transaction as PgTransaction, Connection as PgConnection, Error as PgError,
};

fn query_applied_migrations(
    transaction: &PgTransaction,
    query: &str,
) -> Result<Vec<AppliedMigration>, PgError> {
    let rows = transaction.query(query, &[])?;
    let mut applied = Vec::new();
    for row in rows.into_iter() {
        let version: i32 = row.get(0);
        let applied_on: String = row.get(2);
        let applied_on = DateTime::parse_from_rfc3339(&applied_on)
            .unwrap()
            .with_timezone(&Local);

        applied.push(AppliedMigration {
            version: version as usize,
            name: row.get(1),
            applied_on,
            checksum: row.get(3),
        });
    }
    Ok(applied)
}

impl Transaction for PgConnection {
    type Error = PgError;

    fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let transaction = PgConnection::transaction(&self)?;
        let mut count = 0;
        for query in queries.iter() {
            count += PgTransaction::execute(&transaction, query, &[])?;
        }
        transaction.commit()?;
        Ok(count as usize)
    }
}

impl Query<Vec<AppliedMigration>> for PgConnection {
    fn query(&mut self, query: &str) -> Result<Option<Vec<AppliedMigration>>, Self::Error> {
        let transaction = PgConnection::transaction(self)?;
        let applied = query_applied_migrations(&transaction, query)?;
        transaction.commit()?;
        Ok(Some(applied))
    }
}
