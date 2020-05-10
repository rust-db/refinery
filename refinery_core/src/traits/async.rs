use crate::error::WrapMigrationError;
use crate::traits::{
    check_missing_divergent, ASSERT_MIGRATIONS_TABLE_QUERY, GET_APPLIED_MIGRATIONS_QUERY,
    GET_LAST_APPLIED_MIGRATION_QUERY,
};
use crate::{AppliedMigration, Error, Migration, Target};

use async_trait::async_trait;
use chrono::Local;

#[async_trait]
pub trait AsyncTransaction {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn execute(&mut self, query: &[&str]) -> Result<usize, Self::Error>;
}

#[async_trait]
pub trait AsyncQuery<T>: AsyncTransaction {
    async fn query(&mut self, query: &str) -> Result<T, Self::Error>;
}

async fn migrate<T: AsyncTransaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
    target: Target,
) -> Result<(), Error> {
    for migration in migrations.iter() {
        if let Target::Version(input_target) = target {
            if (input_target as i32) < migration.version {
                log::info!("stoping at migration: {}, due to user option", input_target);
                break;
            }
        }

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
    target: Target,
) -> Result<(), Error> {
    let mut grouped_migrations = Vec::new();
    let mut display_migrations = Vec::new();
    for migration in migrations.into_iter() {
        if let Target::Version(input_target) = target {
            if (input_target as i32) < migration.version {
                break;
            }
        }

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

    if let Target::Version(input_target) = target {
        log::info!("stoping at migration: {}, due to user option", input_target);
    }

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
    async fn get_last_applied_migration(&mut self) -> Result<Option<AppliedMigration>, Error> {
        let mut migrations = self
            .query(GET_LAST_APPLIED_MIGRATION_QUERY)
            .await
            .migration_err("error getting last applied migration")?;

        Ok(migrations.pop())
    }

    async fn get_applied_migrations(&mut self) -> Result<Vec<AppliedMigration>, Error> {
        let migrations = self
            .query(GET_APPLIED_MIGRATIONS_QUERY)
            .await
            .migration_err("error getting applied migrations")?;

        Ok(migrations)
    }

    async fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        grouped: bool,
        target: Target,
    ) -> Result<(), Error> {
        self.execute(&[ASSERT_MIGRATIONS_TABLE_QUERY])
            .await
            .migration_err("error asserting migrations table")?;

        let applied_migrations = self
            .query(GET_APPLIED_MIGRATIONS_QUERY)
            .await
            .migration_err("error getting current schema version")?;

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
            migrate_grouped(self, migrations, target).await?
        } else {
            migrate(self, migrations, target).await?
        }

        Ok(())
    }
}

impl<T> AsyncMigrate for T where T: AsyncQuery<Vec<AppliedMigration>> {}
