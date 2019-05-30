use crate::{
    AppliedMigration, CommitTransaction, DefaultQueries, Error, ExecuteMultiple, Migrate,
    MigrateGrouped, Query, Transaction, WrapMigrationError,
};
use chrono::{DateTime, Local};
use rusqlite::{
    Connection as RqlConnection, Error as RqlError, Transaction as RqlTransaction, NO_PARAMS,
};

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

impl<'a> Transaction for RqlTransaction<'a> {
    type Error = RqlError;

    fn execute(&mut self, query: &str) -> Result<usize, Self::Error> {
        //Deref<Target = Connection>
        RqlConnection::execute(self, query, NO_PARAMS)
    }
}

impl<'a> CommitTransaction for RqlTransaction<'a> {
    fn commit(self) -> Result<(), Self::Error> {
        RqlTransaction::commit(self)
    }
}

impl<'a> Query<Vec<AppliedMigration>> for RqlTransaction<'a> {
    fn query(&mut self, query: &str) -> Result<Option<Vec<AppliedMigration>>, Self::Error> {
        let applied = query_applied_migrations(self, query)?;
        Ok(Some(applied))
    }
}

impl<'a> DefaultQueries for RqlTransaction<'a> {}

impl<'a> MigrateGrouped<'a> for RqlConnection {
    type Transaction = RqlTransaction<'a>;

    fn transaction(&'a mut self) -> Result<Self::Transaction, Error> {
        RqlConnection::transaction(self).migration_err("error starting transaction")
    }
}

impl Transaction for RqlConnection {
    type Error = RqlError;

    fn execute(&mut self, query: &str) -> Result<usize, Self::Error> {
        let mut transaction = self.transaction()?;
        let count = RqlConnection::execute(&mut transaction, query, NO_PARAMS)?;
        transaction.commit()?;
        Ok(count)
    }
}

impl ExecuteMultiple for RqlConnection {
    fn execute_multiple(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let mut transaction = self.transaction()?;
        let mut count = 0;
        for query in queries.iter() {
            count += transaction.execute(query)?;
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

impl DefaultQueries for RqlConnection {}

impl Migrate for RqlConnection {}
