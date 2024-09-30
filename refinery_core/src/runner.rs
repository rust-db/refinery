use std::collections::VecDeque;

use crate::{
    executor::{exec::migrate as sync_migrate, DEFAULT_MIGRATION_TABLE_NAME},
    AsyncMigrate, Error, Migrate, Migration, Target,
};

/// Struct that represents the report of the migration cycle.
/// A `Report` instance is returned by the [`Runner::run`] and [`Runner::run_async`] methods
/// via [`Result`]`<Report, Error>`. If there is an [`Error`] during a migration, you can access
/// the `Report` with [`Error.report`].
///
/// [`Error`]: struct.Error.html
/// [`Runner::run`]: struct.Runner.html#method.run
/// [`Runner::run_async`]: struct.Runner.html#method.run_async
/// [`Result`]: https://doc.rust-lang.org/std/result/enum.Result.html
/// [`Error.report`]:  struct.Error.html#method.report
#[derive(Clone, Debug)]
pub struct Report {
    applied_migrations: Vec<Migration>,
}

impl Report {
    /// Instantiate a new Report
    pub(crate) fn new(applied_migrations: Vec<Migration>) -> Report {
        Report { applied_migrations }
    }

    /// Retrieves the list of applied `Migration` of the migration cycle
    pub fn applied_migrations(&self) -> &Vec<Migration> {
        &self.applied_migrations
    }
}

/// Struct that represents the entrypoint to run the migrations,
/// an instance of this struct is returned by the [`embed_migrations!`] macro.
/// `Runner` should not need to be instantiated manually.
///
/// [`embed_migrations!`]: macro.embed_migrations.html
pub struct Runner {
    grouped: bool,
    abort_divergent: bool,
    abort_missing: bool,
    migrations: Vec<Migration>,
    target: Target,
    migration_table_name: String,
}

impl Runner {
    /// instantiate a new Runner
    pub fn new(migrations: &[Migration]) -> Runner {
        Runner {
            grouped: false,
            target: Target::Latest,
            abort_divergent: true,
            abort_missing: true,
            migrations: migrations.to_vec(),
            migration_table_name: DEFAULT_MIGRATION_TABLE_NAME.into(),
        }
    }

    /// Get the gathered migrations.
    pub fn get_migrations(&self) -> &Vec<Migration> {
        &self.migrations
    }

    /// Set the target version up to which refinery should migrate, Latest migrates to the latest version available
    /// Version migrates to a user provided version, a Version with a higher version than the latest will be ignored,
    /// and Fake doesn't actually run any migration, just creates and updates refinery's schema migration table
    /// by default this is set to Latest
    pub fn set_target(self, target: Target) -> Runner {
        Runner { target, ..self }
    }

    /// Set true if all migrations should be grouped and run in a single transaction.
    /// By default this is set to false, so each migration runs in its own transaction unless
    /// the annotation `refinery:noTransaction` is in a comment in the migration file.
    ///
    /// # Note
    ///
    /// set_grouped won't probably work on MySQL Databases as MySQL lacks support for transactions around schema alteration operations,
    /// meaning that if a migration fails to apply you will have to manually unpick the changes in order to try again (it’s impossible to roll back to an earlier point).
    pub fn set_grouped(self, grouped: bool) -> Runner {
        Runner { grouped, ..self }
    }

    /// Set true if migration process should abort if divergent migrations are found
    /// i.e. applied migrations with the same version but different name or checksum from the ones on the filesystem.
    /// By default this is set to true.
    pub fn set_abort_divergent(self, abort_divergent: bool) -> Runner {
        Runner {
            abort_divergent,
            ..self
        }
    }

    /// Set true if migration process should abort if missing migrations are found
    /// i.e. applied migrations that are not found on the filesystem,
    /// or migrations found on filesystem with a version inferior to the last one applied but not applied.
    /// By default this is set to true.
    pub fn set_abort_missing(self, abort_missing: bool) -> Runner {
        Runner {
            abort_missing,
            ..self
        }
    }

    /// Queries the database for the last applied migration, returns None if there aren't applied Migrations
    pub fn get_last_applied_migration<C>(&self, conn: &'_ mut C) -> Result<Option<Migration>, Error>
    where
        C: Migrate,
    {
        Migrate::get_last_applied_migration(conn, &self.migration_table_name)
    }

    /// Queries the database asynchronously for the last applied migration, returns None if there aren't applied Migrations
    pub async fn get_last_applied_migration_async<C>(
        &self,
        conn: &mut C,
    ) -> Result<Option<Migration>, Error>
    where
        C: AsyncMigrate + Send,
    {
        AsyncMigrate::get_last_applied_migration(conn, &self.migration_table_name).await
    }

    /// Queries the database for all previous applied migrations
    pub fn get_applied_migrations<C>(&self, conn: &'_ mut C) -> Result<Vec<Migration>, Error>
    where
        C: Migrate,
    {
        Migrate::get_applied_migrations(conn, &self.migration_table_name)
    }

    /// Queries the database asynchronously for all previous applied migrations
    pub async fn get_applied_migrations_async<C>(
        &self,
        conn: &mut C,
    ) -> Result<Vec<Migration>, Error>
    where
        C: AsyncMigrate + Send,
    {
        AsyncMigrate::get_applied_migrations(conn, &self.migration_table_name).await
    }

    /// Set the table name to use for the migrations table. The default name is `refinery_schema_history`
    ///
    /// ### Warning
    /// Changing this can be disastrous for your database. You should verify that the migrations table has the same
    /// name as the name you specify here, if this is changed on an existing project.
    ///
    /// # Panics
    ///
    /// If the provided `migration_table_name` is empty
    pub fn set_migration_table_name<S: AsRef<str>>(
        &mut self,
        migration_table_name: S,
    ) -> &mut Self {
        if migration_table_name.as_ref().is_empty() {
            panic!("Migration table name must not be empty");
        }

        self.migration_table_name = migration_table_name.as_ref().to_string();
        self
    }

    /// Creates an iterator over pending migrations, applying each before returning
    /// the result from `next()`. If a migration fails, the iterator will return that
    /// result and further calls to `next()` will return `None`.
    pub fn run_iter<C>(
        self,
        connection: &mut C,
    ) -> impl Iterator<Item = Result<Migration, Error>> + '_
    where
        C: Migrate,
    {
        RunIterator::new(self, connection)
    }

    /// Runs the Migrations in the supplied database connection
    pub fn run<C>(&self, connection: &mut C) -> Result<Report, Error>
    where
        C: Migrate,
    {
        Migrate::migrate(
            connection,
            &self.migrations,
            self.abort_divergent,
            self.abort_missing,
            self.grouped,
            self.target,
            &self.migration_table_name,
        )
    }

    /// Runs the Migrations asynchronously in the supplied database connection
    pub async fn run_async<C>(&self, connection: &mut C) -> Result<Report, Error>
    where
        C: AsyncMigrate + Send,
    {
        AsyncMigrate::migrate(
            connection,
            &self.migrations,
            self.abort_divergent,
            self.abort_missing,
            self.grouped,
            self.target,
            &self.migration_table_name,
        )
        .await
    }
}

pub struct RunIterator<'a, C> {
    connection: &'a mut C,
    target: Target,
    migration_table_name: String,
    items: VecDeque<Migration>,
    failed: bool,
}

impl<'a, C> RunIterator<'a, C>
where
    C: Migrate,
{
    pub(crate) fn new(runner: Runner, connection: &'a mut C) -> RunIterator<'a, C> {
        RunIterator {
            items: VecDeque::from(
                Migrate::get_unapplied_migrations(
                    connection,
                    &runner.migrations,
                    runner.abort_divergent,
                    runner.abort_missing,
                    &runner.migration_table_name,
                )
                .unwrap(),
            ),
            connection,
            target: runner.target,
            migration_table_name: runner.migration_table_name.clone(),
            failed: false,
        }
    }
}

impl<C> Iterator for RunIterator<'_, C>
where
    C: Migrate,
{
    type Item = Result<Migration, Error>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.failed {
            true => None,
            false => self.items.pop_front().and_then(|migration| {
                sync_migrate(
                    self.connection,
                    vec![migration],
                    self.target,
                    &self.migration_table_name,
                    false,
                )
                .map(|r| r.applied_migrations.first().cloned())
                .map_err(|e| {
                    log::error!("migration failed: {e:?}");
                    self.failed = true;
                    e
                })
                .transpose()
            }),
        }
    }
}
