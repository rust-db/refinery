use crate::{
    AppliedMigration, CommitTransaction, Error, ExecuteMultiple, Migrate,
    MigrateGrouped, Query, Transaction, WrapMigrationError,
};
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

impl<'a> Transaction for PgTransaction<'a> {
    type Error = PgError;

    fn execute(&mut self, query: &str) -> Result<usize, Self::Error> {
        let count = PgTransaction::execute(self, query, &[])?;
        Ok(count as usize)
    }
}

impl<'a> CommitTransaction for PgTransaction<'a> {
    fn commit(self) -> Result<(), Self::Error> {
        PgTransaction::commit(self)
    }
}

impl<'a> Query<Vec<AppliedMigration>> for PgTransaction<'a> {
    fn query(&mut self, query: &str) -> Result<Option<Vec<AppliedMigration>>, Self::Error> {
        let applied = query_applied_migrations(self, query)?;
        Ok(Some(applied))
    }
}

impl<'a> MigrateGrouped<'a> for PgConnection {
    type Transaction = PgTransaction<'a>;

    fn transaction(&'a mut self) -> Result<PgTransaction<'a>, Error> {
        PgConnection::transaction(self).migration_err("error starting transaction")
    }
}

impl Transaction for PgConnection {
    type Error = PgError;

    fn execute(&mut self, query: &str) -> Result<usize, Self::Error> {
        let transaction = PgConnection::transaction(&self)?;
        let count = PgTransaction::execute(&transaction, query, &[])?;
        transaction.commit()?;
        Ok(count as usize)
    }
}

impl ExecuteMultiple for PgConnection {
    fn execute_multiple(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
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

impl Migrate for PgConnection {}
