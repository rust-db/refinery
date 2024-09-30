use super::{
    insert_migration_query, verify_migrations, ASSERT_MIGRATIONS_TABLE_QUERY,
    GET_APPLIED_MIGRATIONS_QUERY, GET_LAST_APPLIED_MIGRATION_QUERY,
};
use crate::error::WrapMigrationError;
use crate::{Error, Migration, MigrationContent, Report, Target};

pub trait Executor {
    type Error: std::error::Error + Send + Sync + 'static;

    /// Runs a collection of migrations in one transaction.  The user
    /// implementing this trait is responsible for the guarantee that
    /// that is correctly done.
    fn execute_grouped<'a, T: Iterator<Item = &'a str>>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error>;

    /// Run a set of tuples of the migration query and the query to update the
    /// schema history table on success. This is done in order but not all
    /// together in one transaction. An individual query may be ran in a
    /// transaction according to the `no_transaction` field of the migration
    /// content struct. If a schema history update query fails, the migration
    /// cycle is halted there.
    fn execute<'a, T>(&mut self, queries: T) -> Result<usize, Self::Error>
    where
        T: Iterator<Item = (&'a MigrationContent, &'a str)>;
}

pub trait QuerySchemaHistory<T>: Executor {
    fn query_schema_history(&mut self, query: &str) -> Result<T, Self::Error>;
}

/// A type that needs the driver to produce the final query.
pub trait FinalizeMigration {
    type Driver: Executor;

    fn finalize(&self) -> Result<String, <Self::Driver as Executor>::Error>;
}

pub fn migrate<T: Executor>(
    executor: &mut T,
    migrations: Vec<Migration>,
    target: Target,
    migration_table_name: &str,
    grouped: bool,
) -> Result<Report, Error> {
    if grouped {
        migrate_grouped(executor, migrations, target, migration_table_name)
    } else {
        migrate_individual(executor, migrations, target, migration_table_name)
    }
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
            .execute(
                [(
                    migration.content().expect("migration has no content"),
                    update_query.as_str(),
                )]
                .into_iter(),
            )
            .migration_err(
                &format!("error applying migration {}", migration),
                Some(&applied_migrations),
            )?;
        applied_migrations.push(migration);
    }
    Ok(Report::new(applied_migrations))
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

        let sql = migration.sql().expect("sql must be Some!");

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
                "going to apply batch migrations in single transaction: {:#?}",
                applied_migrations.iter().map(ToString::to_string)
            );
        }
    };

    if let Target::Version(input_target) = target {
        log::info!(
            "stopping at migration: {}, due to user option",
            input_target
        );
    }

    let refs = grouped_migrations.iter().map(AsRef::as_ref);

    executor
        .execute_grouped(refs)
        .migration_err("error applying migrations", None)?;

    Ok(Report::new(applied_migrations))
}

pub trait Migrate: QuerySchemaHistory<Vec<Migration>>
where
    Self: Sized,
{
    fn assert_migrations_table(&mut self, migration_table_name: &str) -> Result<usize, Error> {
        // Needed cause some database vendors like Mssql have a non sql standard way of checking the migrations table,
        // thouh in this case it's just to be consistent with the async trait `AsyncMigrate`
        self.execute_grouped(
            [ASSERT_MIGRATIONS_TABLE_QUERY
                .replace("%MIGRATION_TABLE_NAME%", migration_table_name)
                .as_str()]
            .into_iter(),
        )
        .migration_err("error asserting migrations table", None)
    }

    fn get_last_applied_migration(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Option<Migration>, Error> {
        let mut migrations = self
            .query_schema_history(
                &GET_LAST_APPLIED_MIGRATION_QUERY
                    .replace("%MIGRATION_TABLE_NAME%", migration_table_name),
            )
            .migration_err("error getting last applied migration", None)?;

        Ok(migrations.pop())
    }

    fn get_applied_migrations(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        let migrations = self
            .query_schema_history(
                &GET_APPLIED_MIGRATIONS_QUERY
                    .replace("%MIGRATION_TABLE_NAME%", migration_table_name),
            )
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
            migrate_grouped(self, migrations, target, migration_table_name)
        } else {
            migrate_individual(self, migrations, target, migration_table_name)
        }
    }
}
