use crate::error::WrapMigrationError;
use crate::runner::RollbackTarget;
use crate::traits::{
    delete_migration_query, insert_migration_query, verify_migrations,
    ASSERT_MIGRATIONS_TABLE_QUERY, GET_APPLIED_MIGRATIONS_QUERY, GET_LAST_APPLIED_MIGRATION_QUERY,
};
use crate::{Error, MigrateTarget, Migration, Report};

use super::verify_rollback_migrations;

pub trait Transaction {
    type Error: std::error::Error + Send + Sync + 'static;

    fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error>;
}

pub trait Query<T>: Transaction {
    fn query(&mut self, query: &str) -> Result<T, Self::Error>;
}

pub fn migrate<T: Transaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
    target: MigrateTarget,
    migration_table_name: &str,
    batched: bool,
) -> Result<Report, Error> {
    let mut migration_batch = Vec::new();
    let mut applied_migrations = Vec::new();

    for mut migration in migrations.into_iter() {
        if let MigrateTarget::Version(input_target) | MigrateTarget::FakeVersion(input_target) =
            target
        {
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
        let insert_migration = insert_migration_query(&migration, migration_table_name);
        let migration_sql = migration.sql().expect("sql must be Some!").to_string();

        // If Target is Fake, we only update schema migrations table
        if !matches!(target, MigrateTarget::Fake | MigrateTarget::FakeVersion(_)) {
            applied_migrations.push(migration);
            migration_batch.push(migration_sql);
        }
        migration_batch.push(insert_migration);
    }

    match (target, batched) {
        (MigrateTarget::Fake | MigrateTarget::FakeVersion(_), _) => {
            log::info!("not going to apply any migration as fake flag is enabled");
        }
        (MigrateTarget::Latest | MigrateTarget::Version(_), true) => {
            log::info!(
                "going to apply batch migrations in single transaction: {:#?}",
                applied_migrations.iter().map(ToString::to_string)
            );
        }
        (MigrateTarget::Latest | MigrateTarget::Version(_), false) => {
            log::info!(
                "preparing to apply {} migrations: {:#?}",
                applied_migrations.len(),
                applied_migrations.iter().map(ToString::to_string)
            );
        }
    };

    let refs: Vec<&str> = migration_batch.iter().map(AsRef::as_ref).collect();

    if batched {
        transaction
            .execute(refs.as_ref())
            .migration_err("error applying migrations", None)?;
    } else {
        for (i, update) in refs.iter().enumerate() {
            transaction
                .execute(&[update])
                .migration_err("error applying update", Some(&applied_migrations[0..i / 2]))?;
        }
    }

    Ok(Report::applied(applied_migrations))
}

pub fn rollback<T: Transaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
    target: RollbackTarget,
    migration_table_name: &str,
    batched: bool,
) -> Result<Report, Error> {
    let mut rollback_batch = Vec::new();
    let mut rolled_back_migrations = Vec::new();

    for migration in migrations.into_iter() {
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

        let rollback_sql = migration
            .down_sql()
            .expect("rollback must be Some!")
            .to_string();

        rollback_batch.push(rollback_sql);
        rollback_batch.push(delete_migration_query(&migration, migration_table_name));

        rolled_back_migrations.push(migration);
    }

    match (target, batched) {
        (RollbackTarget::Count(_), true) => {
            log::info!(
                "going to rollback {} migrations in single transaction: {:#?}",
                rolled_back_migrations.len(),
                rolled_back_migrations.iter().map(ToString::to_string)
            );
        }
        (RollbackTarget::Count(_), false) => {
            log::info!(
                "preparing to rollback {} migrations: {:#?}",
                rolled_back_migrations.len(),
                rolled_back_migrations.iter().map(ToString::to_string)
            );
        }
        (RollbackTarget::Version(version), true) => {
            log::info!(
                "going to rollback batch migrations in single transaction until {}: {:#?}",
                version,
                rolled_back_migrations.iter().map(ToString::to_string)
            );
        }
        (RollbackTarget::Version(version), false) => {
            log::info!(
                "preparing to rollback migrations until {}: {:#?}",
                version,
                rolled_back_migrations.iter().map(ToString::to_string)
            );
        }
        (RollbackTarget::All, true) => {
            log::info!(
                "going to rollback all migrations in single transaction: {:#?}",
                rolled_back_migrations.iter().map(ToString::to_string)
            );
        }
        (RollbackTarget::All, false) => {
            log::info!(
                "preparing to rollback all migrations: {:#?}",
                rolled_back_migrations.iter().map(ToString::to_string)
            );
        }
    };

    let refs: Vec<&str> = rollback_batch.iter().map(AsRef::as_ref).collect();

    if batched {
        transaction
            .execute(refs.as_ref())
            .rollback_err("error rolling back migrations", None)?;
    } else {
        for (i, update) in refs.iter().enumerate() {
            transaction.execute(&[update]).rollback_err(
                "error rolling back update",
                Some(&rolled_back_migrations[0..i / 2]),
            )?;
        }
    }

    Ok(Report::rolled_back(rolled_back_migrations))
}

pub trait Migrate: Query<Vec<Migration>>
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

    fn assert_migrations_table(&mut self, migration_table_name: &str) -> Result<usize, Error> {
        // Needed cause some database vendors like Mssql have a non sql standard way of checking the migrations table,
        // thou on this case it's just to be consistent with the async trait `AsyncMigrate`
        self.execute(&[Self::assert_migrations_table_query(migration_table_name).as_str()])
            .migration_err("error asserting migrations table", None)
    }

    fn get_last_applied_migration(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Option<Migration>, Error> {
        let mut migrations = self
            .query(Self::get_last_applied_migration_query(migration_table_name).as_str())
            .migration_err("error getting last applied migration", None)?;

        Ok(migrations.pop())
    }

    fn get_applied_migrations(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        let migrations = self
            .query(Self::get_applied_migrations_query(migration_table_name).as_str())
            .migration_err("error getting applied migrations", None)?;

        Ok(migrations)
    }

    fn get_unapplied_migrations(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing_on_filesystem: bool,
        abort_missing_on_applied: bool,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        self.assert_migrations_table(migration_table_name)?;

        let applied_migrations = self.get_applied_migrations(migration_table_name)?;

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

        Ok(migrations)
    }

    fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing_on_filesystem: bool,
        abort_missing_on_applied: bool,
        grouped: bool,
        target: MigrateTarget,
        migration_table_name: &str,
    ) -> Result<Report, Error> {
        let migrations = self.get_unapplied_migrations(
            migrations,
            abort_divergent,
            abort_missing_on_filesystem,
            abort_missing_on_applied,
            migration_table_name,
        )?;

        if grouped || matches!(target, MigrateTarget::Fake | MigrateTarget::FakeVersion(_)) {
            migrate(self, migrations, target, migration_table_name, true)
        } else {
            migrate(self, migrations, target, migration_table_name, false)
        }
    }

    fn rollback(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing_on_filesystem: bool,
        abort_missing_on_applied: bool,
        grouped: bool,
        target: RollbackTarget,
        migration_table_name: &str,
    ) -> Result<Report, Error> {
        let applied_migrations = self.get_applied_migrations(migration_table_name)?;

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
            rollback(
                self,
                rollback_migrations,
                target,
                migration_table_name,
                true,
            )
        } else {
            rollback(
                self,
                rollback_migrations,
                target,
                migration_table_name,
                false,
            )
        }
    }
}
