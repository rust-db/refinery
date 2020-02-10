use crate::error::WrapMigrationError;
use crate::traits::{check_missing_divergent, ASSERT_MIGRATIONS_TABLE, GET_APPLIED_MIGRATIONS};
use crate::{AppliedMigration, Error, Migration};
use chrono::Local;

pub trait Transaction {
    type Error: std::error::Error + Send + Sync + 'static;

    fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error>;
}

pub trait Query<T>: Transaction {
    fn query(&mut self, query: &str) -> Result<Option<T>, Self::Error>;
}

fn migrate<T: Transaction>(transaction: &mut T, migrations: Vec<Migration>) -> Result<(), Error> {
    for migration in migrations.iter() {
        log::info!("applying migration: {}", migration);
        let update_query = &format!(
                "INSERT INTO refinery_schema_history (version, name, applied_on, checksum) VALUES ({}, '{}', '{}', '{}')",
                migration.version, migration.name, Local::now().to_rfc3339(), migration.checksum().to_string());
        transaction
            .execute(&[&migration.sql, update_query])
            .migration_err(&format!("error applying migration {}", migration))?;
    }
    Ok(())
}

fn migrate_grouped<T: Transaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
) -> Result<(), Error> {
    let mut grouped_migrations = Vec::new();
    let mut display_migrations = Vec::new();
    for migration in migrations.into_iter() {
        let query = format!(
            "INSERT INTO refinery_schema_history (version, name, applied_on, checksum) VALUES ({}, '{}', '{}', '{}')",
            migration.version, migration.name, Local::now().to_rfc3339(), migration.checksum().to_string()
        );
        display_migrations.push(migration.to_string());
        grouped_migrations.push(migration.sql);
        grouped_migrations.push(query);
    }
    log::info!(
        "going to apply batch migrations in single transaction: {:#?}",
        display_migrations
    );

    let refs: Vec<&str> = grouped_migrations.iter().map(AsRef::as_ref).collect();

    transaction
        .execute(refs.as_ref())
        .migration_err("error applying migrations")?;

    Ok(())
}

pub trait Migrate: Query<Vec<AppliedMigration>>
where
    Self: Sized,
{
    fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        grouped: bool,
    ) -> Result<(), Error> {
        self.execute(&[ASSERT_MIGRATIONS_TABLE])
            .migration_err("error asserting migrations table")?;

        let applied_migrations = self
            .query(GET_APPLIED_MIGRATIONS)
            .migration_err("error getting current schema version")?
            .unwrap_or_default();

        let migrations = check_missing_divergent(
            applied_migrations,
            migrations.to_vec(),
            abort_divergent,
            abort_missing,
        )?;

        if migrations.is_empty() {
            log::info!("no migrations to apply");
        }

        if grouped {
            migrate_grouped(self, migrations)
        } else {
            migrate(self, migrations)
        }
    }
}

impl<T: Query<Vec<AppliedMigration>>> Migrate for T {}
