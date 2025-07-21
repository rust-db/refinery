use crate::error::WrapMigrationError;
use crate::traits::{
    insert_migration_query, verify_migrations, ASSERT_MIGRATIONS_TABLE_QUERY,
    GET_APPLIED_MIGRATIONS_QUERY, GET_LAST_APPLIED_MIGRATION_QUERY,
};
use crate::{Error, Migration, MigrationFlags, Report, Target};

pub trait Executor {
    type Error: std::error::Error + Send + Sync + 'static;

    // Run multiple queries implicitly in a transaction
    fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error>;

    // Run single query along with a query to update the migration table.
    // Offers more granular control via MigrationFlags
    fn execute_single(
        &mut self,
        query: &str,
        update_query: &str,
        flags: &MigrationFlags,
    ) -> Result<usize, Self::Error>;
}

pub trait Query<T>: Executor {
    fn query(&mut self, query: &str) -> Result<T, Self::Error>;
}

fn migrate_grouped<T: Executor>(
    executor: &mut T,
    migrations: Vec<Migration>,
    target: Target,
    migration_table_name: &str,
) -> Result<Report, Error> {
    let mut grouped_migrations = Vec::new();
    let mut applied_migrations = Vec::new();

    for mut migration in migrations.into_iter() {
        if let Target::Version(input_target) | Target::FakeVersion(input_target) = target {
            if input_target < migration.version() {
                break;
            }
        }

        migration.set_applied();
        let query = insert_migration_query(&migration, migration_table_name);

        let sql = migration.sql().expect("sql must be Some!").to_owned();

        // If Target is Fake, we only update schema migrations table
        if !matches!(target, Target::Fake | Target::FakeVersion(_)) {
            applied_migrations.push(migration);
            grouped_migrations.push(sql);
        }
        grouped_migrations.push(query);
    }

    match target {
        Target::Fake | Target::FakeVersion(_) => {
            log::info!("not going to apply any migration as fake flag is enabled");
        }
        Target::Latest | Target::Version(_) => {
            log::info!(
                "going to batch apply {} migrations in single transaction.",
                applied_migrations.len()
            );
        }
    };

    if let Target::Version(input_target) = target {
        log::info!(
            "stopping at migration: {}, due to user option",
            input_target
        );
    }

    let refs: Vec<_> = grouped_migrations.iter().map(AsRef::as_ref).collect();

    executor
        .execute(refs.as_ref())
        .migration_err("error applying migrations", None)?;

    Ok(Report::new(applied_migrations))
}

fn migrate_individual<T: Executor>(
    executor: &mut T,
    migrations: Vec<Migration>,
    target: Target,
    migration_table_name: &str,
) -> Result<Report, Error> {
    let mut applied_migrations = vec![];

    for mut migration in migrations.into_iter() {
        if let Target::Version(input_target) = target {
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
        executor
            .execute_single(
                &migration.sql().expect("migration has no content"),
                &update_query,
                migration.flags(),
            )
            .migration_err(
                &format!("error applying migration {}", migration),
                Some(&applied_migrations),
            )?;
        applied_migrations.push(migration);
    }
    Ok(Report::new(applied_migrations))
}

pub fn migrate<T: Executor>(
    executor: &mut T,
    migrations: Vec<Migration>,
    target: Target,
    migration_table_name: &str,
    batched: bool,
) -> Result<Report, Error> {
    if batched {
        migrate_grouped(executor, migrations, target, migration_table_name)
    } else {
        migrate_individual(executor, migrations, target, migration_table_name)
    }
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
        abort_missing: bool,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        self.assert_migrations_table(migration_table_name)?;

        let applied_migrations = self.get_applied_migrations(migration_table_name)?;

        let migrations = verify_migrations(
            applied_migrations,
            migrations.to_vec(),
            abort_divergent,
            abort_missing,
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
        abort_missing: bool,
        grouped: bool,
        target: Target,
        migration_table_name: &str,
    ) -> Result<Report, Error> {
        let migrations = self.get_unapplied_migrations(
            migrations,
            abort_divergent,
            abort_missing,
            migration_table_name,
        )?;

        if grouped || matches!(target, Target::Fake | Target::FakeVersion(_)) {
            migrate(self, migrations, target, migration_table_name, true)
        } else {
            migrate(self, migrations, target, migration_table_name, false)
        }
    }
}
