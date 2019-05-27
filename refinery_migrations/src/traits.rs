use chrono::Local;
use crate::{Migration, AppliedMigration, Error, WrapMigrationError};

pub trait Transaction {
    type Error: std::error::Error + Send + Sync + 'static;

    fn execute(&mut self, query: &str) -> Result<usize, Self::Error>;
}

pub trait ExecuteMultiple: Transaction {
    fn execute_multiple(&mut self, queries: &[&str]) -> Result<usize, Self::Error>;
}

pub trait CommitTransaction: Transaction
where
    Self: Sized,
{
    fn commit(self) -> Result<(), Self::Error>;
}

pub trait Query<T>: Transaction {
    fn query(&mut self, query: &str) -> Result<Option<T>, Self::Error>;
}

pub trait DefaultQueries: Transaction + Query<AppliedMigration> {
    fn assert_schema_history_table(&mut self) -> Result<usize, Self::Error> {
        self.execute(
            "CREATE TABLE IF NOT EXISTS refinery_schema_history( \
                 version INTEGER PRIMARY KEY,\
                 name VARCHAR(255),\
                 installed_on VARCHAR(255),
                 checksum VARCHAR(255));",
        )
    }

    fn get_current_version(&mut self) -> Result<Option<AppliedMigration>, Self::Error> {
        self.query(
            "SELECT version, name, installed_on, checksum FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)",
        )
    }
}

pub trait MigrateGrouped<'a> {
    type Transaction: DefaultQueries + CommitTransaction;

    fn migrate(&'a mut self, migrations: &[Migration]) -> Result<(), Error> {
        let mut transaction = self.transaction()?;
        transaction
            .assert_schema_history_table()
            .migration_err("error asserting migrations table")?;

        let current = transaction
            .get_current_version()
            .migration_err("error getting current schema version")?
            .unwrap_or(AppliedMigration {
                name: "".into(),
                version: 0,
                installed_on: Local::now(),
                checksum: "".into(),
            });
        log::debug!("current migration: {}", current.version);

        let mut migrations = migrations
            .iter()
            .filter(|m| m.version > current.version)
            .collect::<Vec<_>>();
        migrations.sort();

        if migrations.is_empty() {
            log::debug!("no migrations to apply");
        }

        for migration in migrations.iter() {
            log::debug!("applying migration: {}", migration.name);
            transaction
                .execute(&migration.sql)
                .migration_err(&format!("error applying migration {}", migration))?;

            transaction
                .execute(&format!(
                    "INSERT INTO refinery_schema_history (version, name, installed_on, checksum) VALUES ({}, '{}', '{}', '{}')",
                    migration.version, migration.name, Local::now().to_rfc3339(), migration.checksum().to_string()
                ))
                .migration_err(&format!("error updating schema history to migration: {}", migration))?;
        }

        transaction
            .commit()
            .migration_err("error committing transaction")?;

        Ok(())
    }

    fn transaction(&'a mut self) -> Result<Self::Transaction, Error>;
}

pub trait Migrate: DefaultQueries + ExecuteMultiple {
    fn migrate(&mut self, migrations: &[Migration]) -> Result<(), Error> {
        self.assert_schema_history_table()
            .migration_err("error asserting migrations table")?;
        let current = self
            .get_current_version()
            .migration_err("error getting current schema version")?
            .unwrap_or(AppliedMigration {
                name: "".into(),
                version: 0,
                installed_on: Local::now(),
                checksum: "".into(),
            });

        log::debug!("current migration: {}", current.version);
        let mut migrations = migrations
            .iter()
            .filter(|m| m.version > current.version)
            .collect::<Vec<_>>();
        migrations.sort();

        for migration in migrations.iter() {
            let update_query = &format!(
                "INSERT INTO refinery_schema_history (version, name, installed_on, checksum) VALUES ({}, '{}', '{}', '{}')",
                migration.version, migration.name, Local::now().to_rfc3339(), migration.checksum().to_string());
            self.execute_multiple(&[&migration.sql, update_query])
                .migration_err(&format!("error applying migration {}", migration))?;
        }

        Ok(())
    }
}