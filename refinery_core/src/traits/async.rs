use crate::error::WrapMigrationError;
use crate::traits::{check_missing_divergent, ASSERT_MIGRATIONS_TABLE, GET_APPLIED_MIGRATIONS};
use crate::{AppliedMigration, Error, Migration};

use async_trait::async_trait;
use chrono::Local;

#[async_trait]
pub trait AsyncTransaction {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn execute(&mut self, query: &[&str]) -> Result<usize, Self::Error>;
}

#[async_trait]
pub trait AsyncQuery<T>: AsyncTransaction {
    async fn query(&mut self, query: &str) -> Result<Option<T>, Self::Error>;
}

async fn migrate<T: AsyncTransaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
) -> Result<(), Error> {
    for migration in migrations.iter() {
        log::info!("applying migration: {}", migration);
        let update_query = &format!(
                "INSERT INTO refinery_schema_history (version, name, applied_on, checksum) VALUES ({}, '{}', '{}', '{}')",
                migration.version, migration.name, Local::now().to_rfc3339(), migration.checksum().to_string());
        transaction
            .execute(&[&migration.sql, update_query])
            .await
            .migration_err(&format!("error applying migration {}", migration))?;
    }
    Ok(())
}

async fn migrate_grouped<T: AsyncTransaction>(
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
        .await
        .migration_err("error applying migrations")?;

    Ok(())
}

#[async_trait]
pub trait AsyncMigrate: AsyncQuery<Vec<AppliedMigration>>
where
    Self: Sized,
{
    async fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        grouped: bool,
    ) -> Result<(), Error> {
        self.execute(&[ASSERT_MIGRATIONS_TABLE])
            .await
            .migration_err("error asserting migrations table")?;

        let applied_migrations = self
            .query(GET_APPLIED_MIGRATIONS)
            .await
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
            migrate_grouped(self, migrations).await?
        } else {
            migrate(self, migrations).await?
        }

        Ok(())
    }
}

impl<T> AsyncMigrate for T where T: AsyncQuery<Vec<AppliedMigration>> {}
