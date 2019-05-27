use crate::{
    CommitTransaction, DefaultQueries, Migrate, MigrateGrouped, Error,
    AppliedMigration, Query, Transaction, WrapMigrationError, ExecuteMultiple
};
use chrono::{DateTime, Local};
use rusqlite::{
    Connection as RqlConnection, Error as RqlError, OptionalExtension,
    Transaction as RqlTransaction, NO_PARAMS,
};

fn query_migration_version(transaction: &RqlConnection, query: &str) -> Result<Option<AppliedMigration>, RqlError> {
        transaction.query_row(query, NO_PARAMS, |row| {
            //FromSql not implemented for usize
            let version: isize = row.get(0)?;
            let _installed_on: String = row.get(2)?;
            let installed_on = DateTime::parse_from_rfc3339(&_installed_on)
                .unwrap()
                .with_timezone(&Local);
            let mig = AppliedMigration {
                version: version as usize,
                name: row.get(1)?,
                installed_on,
                checksum: row.get(3)?,
            };
            Ok(mig)
        })
        .optional()
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

impl<'a> Query<AppliedMigration> for RqlTransaction<'a> {
    fn query(&mut self, query: &str) -> Result<Option<AppliedMigration>, Self::Error> {
        query_migration_version(self, query)
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

impl Query<AppliedMigration> for RqlConnection {
    fn query(&mut self, query: &str) -> Result<Option<AppliedMigration>, Self::Error> {
        let transaction = self.transaction()?;
        let version = query_migration_version(&transaction, query)?;
        transaction.commit()?;
        Ok(version)
    }
}

impl DefaultQueries for RqlConnection {}

impl Migrate for RqlConnection {}
