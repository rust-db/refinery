use chrono::{DateTime, Local};
use regex::Regex;
use siphasher::sip::SipHasher13;

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::error::Kind;
use crate::{AsyncMigrate, Error, Migrate};

// regex used to match file names
pub fn file_match_re() -> Regex {
    Regex::new(r"^(V)(\d+(?:\.\d+)?)__(\w+)").unwrap()
}

lazy_static::lazy_static! {
    static ref RE: regex::Regex = file_match_re();
}

/// An enum set that represents the type of the Migration, at the moment only Versioned is supported
#[derive(Clone, Debug)]
enum Type {
    Versioned,
}

/// An enum set that represents the target version up to which refinery should migrate, it is used by [Runner]
#[derive(Clone, Copy)]
pub enum Target {
    Latest,
    Version(u32),
}

// an Enum set that represents the state of the migration: Applied on the database,
// or Unapplied yet to be applied on the database
#[derive(Clone, Debug)]
enum State {
    Applied,
    Unapplied,
}

/// Represents a schema migration to be run on the database,
/// this struct is used by the [`embed_migrations!`] and [`include_migration_mods!`] macros to gather migration files
/// and shouldn't be needed by the user
///
/// [`embed_migrations!`]: macro.embed_migrations.html
/// [`include_migration_mods!`]: macro.include_migration_mods.html
#[derive(Clone, Debug)]
pub struct Migration {
    state: State,
    name: String,
    checksum: u64,
    version: i32,
    prefix: Type,
    sql: Option<String>,
    applied_on: Option<DateTime<Local>>,
}

impl Migration {
    /// Create an unapplied migration, name and version are parsed from the input_name,
    /// which must be named in the format V{1}__{2}.rs where {1} represents the migration version and {2} the name.
    pub fn unapplied(input_name: &str, sql: &str) -> Result<Migration, Error> {
        let captures = RE
            .captures(input_name)
            .filter(|caps| caps.len() == 4)
            .ok_or_else(|| Error::new(Kind::InvalidName, None))?;
        let version: i32 = captures[2]
            .parse()
            .map_err(|_| Error::new(Kind::InvalidVersion, None))?;

        let name: String = (&captures[3]).into();
        let prefix = match &captures[1] {
            "V" => Type::Versioned,
            _ => unreachable!(),
        };

        // Previously, `std::collections::hash_map::DefaultHasher` was used
        // to calculate the checksum and the implementation at that time
        // was SipHasher13. However, that implementation is not guaranteed:
        // > The internal algorithm is not specified, and so it and its
        // > hashes should not be relied upon over releases.
        // We now explicitly use SipHasher13 to both remain compatible with
        // existing migrations and prevent breaking from possible future
        // changes to `DefaultHasher`.
        let mut hasher = SipHasher13::new();
        name.hash(&mut hasher);
        version.hash(&mut hasher);
        sql.hash(&mut hasher);
        let checksum = hasher.finish();

        Ok(Migration {
            state: State::Unapplied,
            name,
            version,
            prefix,
            sql: Some(sql.into()),
            applied_on: None,
            checksum,
        })
    }

    // Create a migration from an applied migration on the database
    pub(crate) fn applied(
        version: i32,
        name: String,
        applied_on: DateTime<Local>,
        checksum: u64,
    ) -> Migration {
        Migration {
            state: State::Applied,
            name,
            checksum,
            version,
            // applied migrations are always versioned
            prefix: Type::Versioned,
            sql: None,
            applied_on: Some(applied_on),
        }
    }

    // convert the Unapplied into an Applied Migration
    pub(crate) fn set_applied(&mut self) {
        self.applied_on = Some(Local::now());
        self.state = State::Applied;
    }

    // Get migration sql content
    pub(crate) fn sql(&self) -> Option<&str> {
        self.sql.as_deref()
    }

    /// Get the Migration version
    pub fn version(&self) -> u32 {
        self.version as u32
    }

    /// Get the Migration Name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the Migration Name
    pub fn applied_on(&self) -> Option<&DateTime<Local>> {
        self.applied_on.as_ref()
    }

    /// Get the Migration checksum. Checksum is formed from the name version and sql of the Migration
    pub fn checksum(&self) -> u64 {
        self.checksum
    }
}

impl fmt::Display for Migration {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "V{}__{}", self.version, self.name)
    }
}

impl Eq for Migration {}

impl PartialEq for Migration {
    fn eq(&self, other: &Migration) -> bool {
        self.version == other.version
            && self.name == other.name
            && self.checksum() == other.checksum()
    }
}

impl Ord for Migration {
    fn cmp(&self, other: &Migration) -> Ordering {
        self.version.cmp(&other.version)
    }
}

impl PartialOrd for Migration {
    fn partial_cmp(&self, other: &Migration) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Struct that represents the report of the migration cycle,
/// a `Report` instance is returned by the [`Runner::run`] and [`Runner::run_async`] methods
/// via [`Result`]`<Report, Error>`, on case of an [`Error`] during a migration, you can acess the `Report` with [`Error.report`]
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
/// an instance of this struct is returned by the [`embed_migrations!`] and [`include_migration_mods!`] macros.
/// `Runner` should not need to be instantiated manually
///
/// [`embed_migrations!`]: macro.embed_migrations.html
/// [`include_migration_mods!`]: macro.include_migration_mods.html
pub struct Runner {
    grouped: bool,
    abort_divergent: bool,
    abort_missing: bool,
    migrations: Vec<Migration>,
    target: Target,
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
        }
    }

    /// set the target version up to which refinery should migrate, Latest migrates to the latest version available
    /// Version migrates to a user provided version, a Version with a higher version than the latest will be ignored.
    /// by default this is set to Latest
    pub fn set_target(self, target: Target) -> Runner {
        Runner { target, ..self }
    }

    /// Set true if all migrations should be grouped and run in a single transaction.
    /// by default this is set to false, each migration runs on their own transaction
    ///
    /// # Note
    ///
    /// set_grouped won't probbaly work on MySQL Databases as MySQL lacks support for transactions around schema alteration operations,
    /// meaning that if a migration fails to apply you will have to manually unpick the changes in order to try again (it’s impossible to roll back to an earlier point).
    pub fn set_grouped(self, grouped: bool) -> Runner {
        Runner { grouped, ..self }
    }

    /// Set true if migration process should abort if divergent migrations are found
    /// i.e. applied migrations with the same version but different name or checksum from the ones on the filesystem.
    /// by default this is set to true
    pub fn set_abort_divergent(self, abort_divergent: bool) -> Runner {
        Runner {
            abort_divergent,
            ..self
        }
    }

    /// Set true if migration process should abort if missing migrations are found
    /// i.e. applied migrations that are not found on the filesystem,
    /// or migrations found on filesystem with a version inferior to the last one applied but not applied.
    /// by default this is set to true
    pub fn set_abort_missing(self, abort_divergent: bool) -> Runner {
        Runner {
            abort_divergent,
            ..self
        }
    }

    /// Queries the database for the last applied migration, returns None if there aren't applied Migrations
    pub fn get_last_applied_migration<'a, C>(
        &self,
        conn: &'a mut C,
    ) -> Result<Option<Migration>, Error>
    where
        C: Migrate,
    {
        Migrate::get_last_applied_migration(conn)
    }

    /// Queries the database asychronously for the last applied migration, returns None if there aren't applied Migrations
    pub async fn get_last_applied_migration_async<C>(
        &self,
        conn: &mut C,
    ) -> Result<Option<Migration>, Error>
    where
        C: AsyncMigrate + Send,
    {
        AsyncMigrate::get_last_applied_migration(conn).await
    }

    /// Queries the database for all previous applied migrations
    pub fn get_applied_migrations<'a, C>(&self, conn: &'a mut C) -> Result<Vec<Migration>, Error>
    where
        C: Migrate,
    {
        Migrate::get_applied_migrations(conn)
    }

    /// Queries the database asynchronously for all previous applied migrations
    pub async fn get_applied_migrations_async<C>(
        &self,
        conn: &mut C,
    ) -> Result<Vec<Migration>, Error>
    where
        C: AsyncMigrate + Send,
    {
        AsyncMigrate::get_applied_migrations(conn).await
    }

    /// Runs the Migrations in the supplied database connection
    pub fn run<'a, C>(&self, conn: &'a mut C) -> Result<Report, Error>
    where
        C: Migrate,
    {
        Migrate::migrate(
            conn,
            &self.migrations,
            self.abort_divergent,
            self.abort_missing,
            self.grouped,
            self.target,
        )
    }

    /// Runs the Migrations asynchronously in the supplied database connection
    pub async fn run_async<C>(&self, conn: &mut C) -> Result<Report, Error>
    where
        C: AsyncMigrate + Send,
    {
        AsyncMigrate::migrate(
            conn,
            &self.migrations,
            self.abort_divergent,
            self.abort_missing,
            self.grouped,
            self.target,
        )
        .await
    }
}
