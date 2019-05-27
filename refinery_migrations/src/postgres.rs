use crate::{
    CommitTransaction, DefaultQueries, Migrate, MigrateGrouped, Error,
    AppliedMigration, Query, Transaction, WrapMigrationError, ExecuteMultiple
};
use chrono::{DateTime, Local};
use postgres::{
    transaction::Transaction as PgTransaction, Connection as PgConnection, Error as PgError,
};

fn query_migration_version(transaction: &PgTransaction, query: &str) -> Result<Option<AppliedMigration>, PgError> {
    let rows = transaction.query(query, &[])?;
    match rows.into_iter().next() {
        None => Ok(None),
        Some(row) => {
            let version: i32 = row.get(0);
            let _installed_on: String = row.get(2);
            let installed_on = DateTime::parse_from_rfc3339(&_installed_on)
                .unwrap()
                .with_timezone(&Local);

            Ok(Some(AppliedMigration {
                version: version as usize,
                name: row.get(1),
                installed_on,
                checksum: row.get(3),
            }))
        }
    }
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

impl<'a> Query<AppliedMigration> for PgTransaction<'a> {
    fn query(&mut self, query: &str) -> Result<Option<AppliedMigration>, Self::Error> {
        query_migration_version(self, query)
    }
}

impl<'a> DefaultQueries for PgTransaction<'a> {}

impl<'a> MigrateGrouped<'a> for PgConnection {
    type Transaction = PgTransaction<'a>;

    fn transaction(&'a mut self) -> Result<PgTransaction<'a>, Error> {
        PgConnection::transaction(self).migration_err("error starting transaction")
    }
}

impl Transaction for PgConnection {
    type Error = PgError;

    fn execute(&mut self, query: &str) -> Result<usize, Self::Error> {
        let mut transaction = PgConnection::transaction(&self)?;
        let count = PgTransaction::execute(&mut transaction, query, &[])?;
        transaction.commit()?;
        Ok(count as usize)
    }
}

impl ExecuteMultiple for PgConnection {
    fn execute_multiple(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let mut transaction = PgConnection::transaction(&self)?;
        let mut count = 0;
        for query in queries.iter() {
            count += PgTransaction::execute(&mut transaction, query, &[])?;
        }
        transaction.commit()?;
        Ok(count as usize)
    }
}

impl Query<AppliedMigration> for PgConnection {
    fn query(&mut self, query: &str) -> Result<Option<AppliedMigration>, Self::Error> {
        let transaction = PgConnection::transaction(self)?;
        let version = query_migration_version(&transaction, query)?;
        transaction.commit()?;
        Ok(version)
    }
}

impl DefaultQueries for PgConnection {}

impl Migrate for PgConnection {}
