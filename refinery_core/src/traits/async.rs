use crate::error::WrapMigrationError;
use crate::traits::{
    check_missing_divergent, ASSERT_MIGRATIONS_TABLE_QUERY, GET_APPLIED_MIGRATIONS_QUERY,
    GET_LAST_APPLIED_MIGRATION_QUERY,
};
use crate::{Error, Migration, Report, Target};

use async_trait::async_trait;
use std::string::ToString;

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
) -> Result<Report, Error> {
    let mut applied_migrations = vec![];

    for mut migration in migrations.into_iter() {
        if let Target::Version(input_target) = target {
            if input_target < migration.version() {
                log::info!("stoping at migration: {}, due to user option", input_target);
                break;
            }
        }

        log::info!("applying migration: {}", migration);
        migration.set_applied();
        let update_query = &format!(
                "INSERT INTO refinery_schema_history (version, name, applied_on, checksum) VALUES ({}, '{}', '{}', '{}')",
                // safe to call unwrap as we just converted it to applied
                migration.version(), migration.name(), migration.applied_on().unwrap().to_rfc3339(), migration.checksum().to_string());
        transaction
            .execute(&[
                migration.sql().as_ref().expect("sql must be Some!"),
                update_query,
            ])
            .await
            .migration_err(
                &format!("error applying migration {}", migration),
                Some(&applied_migrations),
            )?;
        applied_migrations.push(migration);
    }
    Ok(Report::new(applied_migrations))
}

async fn migrate_grouped<T: AsyncTransaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
    target: Target,
) -> Result<Report, Error> {
    let mut grouped_migrations = Vec::new();
    let mut applied_migrations = Vec::new();
    for mut migration in migrations.into_iter() {
        if let Target::Version(input_target) = target {
            if input_target < migration.version() {
                break;
            }
        }

        migration.set_applied();
        let query = format!(
            "INSERT INTO refinery_schema_history (version, name, applied_on, checksum) VALUES ({}, '{}', '{}', '{}')",
            // safe to call unwrap as we just converted migration to applied
            migration.version(), migration.name(), migration.applied_on().unwrap().to_rfc3339(), migration.checksum().to_string()
        );

        let sql = migration.sql().expect("sql must be Some!").to_string();
        applied_migrations.push(migration);
        grouped_migrations.push(sql);
        grouped_migrations.push(query);
    }

    log::info!(
        "going to apply batch migrations in single transaction: {:#?}",
        applied_migrations.iter().map(ToString::to_string)
    );

    if let Target::Version(input_target) = target {
        log::info!("stoping at migration: {}, due to user option", input_target);
    }

    let refs: Vec<&str> = grouped_migrations.iter().map(AsRef::as_ref).collect();

    transaction
        .execute(refs.as_ref())
        .await
        .migration_err("error applying migrations", None)?;

    Ok(Report::new(applied_migrations))
}

#[async_trait]
pub trait AsyncMigrate: AsyncQuery<Vec<Migration>>
where
    Self: Sized,
{
    async fn get_last_applied_migration(&mut self) -> Result<Option<Migration>, Error> {
        let mut migrations = self
            .query(GET_LAST_APPLIED_MIGRATION_QUERY)
            .await
            .migration_err("error getting last applied migration", None)?;

        Ok(migrations.pop())
    }

    async fn get_applied_migrations(&mut self) -> Result<Vec<Migration>, Error> {
        let migrations = self
            .query(GET_APPLIED_MIGRATIONS_QUERY)
            .await
            .migration_err("error getting applied migrations", None)?;

        Ok(migrations)
    }

    async fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        grouped: bool,
        target: Target,
    ) -> Result<Report, Error> {
        self.execute(&[ASSERT_MIGRATIONS_TABLE_QUERY])
            .await
            .migration_err("error asserting migrations table", None)?;

        let applied_migrations = self
            .query(GET_APPLIED_MIGRATIONS_QUERY)
            .await
            .migration_err("error getting current schema version", None)?;

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
            migrate_grouped(self, migrations, target).await
        } else {
            migrate(self, migrations, target).await
        }
    }
}
