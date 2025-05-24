use crate::error::WrapMigrationError;
use crate::runner::RollbackTarget;
use crate::traits::{
    delete_migration_query, insert_migration_query, verify_migrations,
    ASSERT_MIGRATIONS_TABLE_QUERY, GET_APPLIED_MIGRATIONS_QUERY, GET_LAST_APPLIED_MIGRATION_QUERY,
};
use crate::{Error, MigrateTarget, Migration, Report};

use async_trait::async_trait;
use std::string::ToString;

use super::verify_rollback_migrations;

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
    target: MigrateTarget,
    migration_table_name: &str,
) -> Result<Report, Error> {
    let mut applied_migrations = vec![];

    for mut migration in migrations.into_iter() {
        if let MigrateTarget::Version(input_target) = target {
            if input_target < migration.version() {
                log::info!(
                    "stopping at migration: {}, due to user option",
                    input_target
                );
                break;
            }
        }

        log::info!("applying migration: {}", migration);
        migration.set_applied();
        let update_query = insert_migration_query(&migration, migration_table_name);
        transaction
            .execute(&[
                migration.sql().as_ref().expect("sql must be Some!"),
                &update_query,
            ])
            .await
            .migration_err(
                &format!("error applying migration {}", migration),
                Some(&applied_migrations),
            )?;
        applied_migrations.push(migration);
    }
    Ok(Report::applied(applied_migrations))
}

async fn migrate_grouped<T: AsyncTransaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
    target: MigrateTarget,
    migration_table_name: &str,
) -> Result<Report, Error> {
    let mut grouped_migrations = Vec::new();
    let mut applied_migrations = Vec::new();

    for mut migration in migrations.into_iter() {
        if let MigrateTarget::Version(input_target) | MigrateTarget::FakeVersion(input_target) =
            target
        {
            if input_target < migration.version() {
                break;
            }
        }

        migration.set_applied();
        let query = insert_migration_query(&migration, migration_table_name);

        let sql = migration.sql().expect("sql must be Some!").to_string();

        // If Target is Fake, we only update schema migrations table
        if !matches!(target, MigrateTarget::Fake | MigrateTarget::FakeVersion(_)) {
            applied_migrations.push(migration);
            grouped_migrations.push(sql);
        }
        grouped_migrations.push(query);
    }

    match target {
        MigrateTarget::Fake | MigrateTarget::FakeVersion(_) => {
            log::info!("not going to apply any migration as fake flag is enabled");
        }
        MigrateTarget::Latest | MigrateTarget::Version(_) => {
            log::info!(
                "going to apply batch migrations in single transaction: {:#?}",
                applied_migrations.iter().map(ToString::to_string)
            );
        }
    };

    if let MigrateTarget::Version(input_target) = target {
        log::info!(
            "stopping at migration: {}, due to user option",
            input_target
        );
    }

    let refs: Vec<&str> = grouped_migrations.iter().map(AsRef::as_ref).collect();

    transaction
        .execute(refs.as_ref())
        .await
        .migration_err("error applying migrations", None)?;

    Ok(Report::applied(applied_migrations))
}

async fn rollback<T: AsyncTransaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
    target: RollbackTarget,
    migration_table_name: &str,
) -> Result<Report, Error> {
    let mut rolled_back_migrations = vec![];

    for mut migration in migrations.into_iter().rev() {
        if let RollbackTarget::Version(input_target) = target {
            if input_target > migration.version() {
                log::info!(
                    "stopping at migration: {}, due to user option",
                    input_target
                );
                break;
            }
        }

        log::info!("rolling back migration: {}", migration);
        migration.set_rolled_back();
        let update_query = delete_migration_query(&migration, migration_table_name);
        transaction
            .execute(&[
                migration
                    .down_sql()
                    .as_ref()
                    .expect("down_sql must be Some!"),
                &update_query,
            ])
            .await
            .rollback_err(
                &format!("error rolling back migration {}", migration),
                Some(&rolled_back_migrations),
            )?;
        rolled_back_migrations.push(migration);
    }

    Ok(Report::rolled_back(rolled_back_migrations))
}

async fn rollback_grouped<T: AsyncTransaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
    target: RollbackTarget,
    migration_table_name: &str,
) -> Result<Report, Error> {
    let mut grouped_migrations = Vec::new();
    let mut rolled_back_migrations = Vec::new();

    for mut migration in migrations.into_iter().rev() {
        if let RollbackTarget::Version(input_target) = target {
            if input_target > migration.version() {
                break;
            }
        }

        migration.set_rolled_back();
        let query = delete_migration_query(&migration, migration_table_name);

        let sql = migration
            .down_sql()
            .expect("down_sql must be Some!")
            .to_string();

        rolled_back_migrations.push(migration);
        grouped_migrations.push(sql);

        grouped_migrations.push(query);
    }

    if let RollbackTarget::Version(input_target) = target {
        log::info!(
            "stopping at migration: {}, due to user option",
            input_target
        );
    }

    let refs: Vec<&str> = grouped_migrations.iter().map(AsRef::as_ref).collect();

    transaction
        .execute(refs.as_ref())
        .await
        .rollback_err("error rolling back migrations", None)?;

    Ok(Report::rolled_back(rolled_back_migrations))
}

#[async_trait]
pub trait AsyncMigrate: AsyncQuery<Vec<Migration>>
where
    Self: Sized,
{
    // Needed cause some database vendors like Mssql have a non sql standard way of checking the migrations table
    fn assert_migrations_table_query(migration_table_name: &str) -> String {
        ASSERT_MIGRATIONS_TABLE_QUERY.replace("%MIGRATION_TABLE_NAME%", migration_table_name)
    }

    fn get_last_applied_migration_query(migration_table_name: &str) -> String {
        GET_LAST_APPLIED_MIGRATION_QUERY.replace("%MIGRATION_TABLE_NAME%", migration_table_name)
    }

    fn get_applied_migrations_query(migration_table_name: &str) -> String {
        GET_APPLIED_MIGRATIONS_QUERY.replace("%MIGRATION_TABLE_NAME%", migration_table_name)
    }

    async fn get_last_applied_migration(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Option<Migration>, Error> {
        let mut migrations = self
            .query(Self::get_last_applied_migration_query(migration_table_name).as_str())
            .await
            .migration_err("error getting last applied migration", None)?;

        Ok(migrations.pop())
    }

    async fn get_applied_migrations(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        let migrations = self
            .query(Self::get_applied_migrations_query(migration_table_name).as_str())
            .await
            .migration_err("error getting applied migrations", None)?;

        Ok(migrations)
    }

    async fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing_on_filesystem: bool,
        abort_missing_on_applied: bool,
        grouped: bool,
        target: MigrateTarget,
        migration_table_name: &str,
    ) -> Result<Report, Error> {
        self.execute(&[&Self::assert_migrations_table_query(migration_table_name)])
            .await
            .migration_err("error asserting migrations table", None)?;

        let applied_migrations = self
            .get_applied_migrations(migration_table_name)
            .await
            .migration_err("error getting current schema version", None)?;

        let migrations = verify_migrations(
            applied_migrations,
            migrations.to_vec(),
            abort_divergent,
            abort_missing_on_filesystem,
            abort_missing_on_applied,
        )?;

        if migrations.is_empty() {
            log::info!("no migrations to apply");
        }

        if grouped || matches!(target, MigrateTarget::Fake | MigrateTarget::FakeVersion(_)) {
            migrate_grouped(self, migrations, target, migration_table_name).await
        } else {
            migrate(self, migrations, target, migration_table_name).await
        }
    }

    async fn rollback(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing_on_filesystem: bool,
        abort_missing_on_applied: bool,
        grouped: bool,
        target: RollbackTarget,
        migration_table_name: &str,
    ) -> Result<Report, Error> {
        self.execute(&[&Self::assert_migrations_table_query(migration_table_name)])
            .await
            .migration_err("error asserting migrations table", None)?;

        let applied_migrations = self
            .get_applied_migrations(migration_table_name)
            .await
            .migration_err("error getting current schema version", None)?;

        let mut rollback_migrations = verify_rollback_migrations(
            applied_migrations,
            migrations.to_vec(),
            abort_divergent,
            abort_missing_on_filesystem,
            abort_missing_on_applied,
        )?;

        if rollback_migrations.is_empty() {
            log::info!("no migrations to rollback");
        }

        if let RollbackTarget::Count(rollback_count) = target {
            rollback_migrations.truncate(rollback_count.get() as usize);
        }

        if grouped {
            rollback_grouped(self, rollback_migrations, target, migration_table_name).await
        } else {
            rollback(self, rollback_migrations, target, migration_table_name).await
        }
    }
}
