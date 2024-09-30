use async_trait::async_trait;
use siphasher::sip::SipHasher13;
use std::cmp::Ordering;
use std::fmt;
use std::fmt::Formatter;
use std::hash::{Hash, Hasher};
use time::OffsetDateTime;

use crate::executor::{AsyncExecutor, Executor};
use crate::util::parse_migration_name;
use crate::{error::WrapMigrationError, Error};

/// An enum set that represents the type of the Migration
#[derive(Clone, PartialEq)]
pub enum Type {
    Versioned,
    Unversioned,
}

impl fmt::Display for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let version_type = match self {
            Type::Versioned => "V",
            Type::Unversioned => "U",
        };
        write!(f, "{}", version_type)
    }
}

impl fmt::Debug for Type {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let version_type = match self {
            Type::Versioned => "Versioned",
            Type::Unversioned => "Unversioned",
        };
        write!(f, "{}", version_type)
    }
}

/// An enum that represents the target version up to which refinery should migrate.
/// It is used by [Runner].
#[derive(Clone, Copy, Debug)]
pub enum Target {
    Latest,
    Version(u32),
    Fake,
    FakeVersion(u32),
}

// an Enum set that represents the state of the migration: Applied on the database,
// or Unapplied yet to be applied on the database
#[derive(Clone, Debug)]
enum State {
    Applied,
    Unapplied,
}

/// The query defining the migration and if the file where it was defined
/// came annotated to indicate that it shouldn't be ran in a transaction.
#[derive(Clone, Debug)]
pub struct MigrationContent {
    sql: String,
    no_transaction: bool,
}

impl MigrationContent {
    pub fn new(no_transaction: Option<bool>, sql: String) -> Self {
        Self {
            sql,
            no_transaction: no_transaction.unwrap_or_default(),
        }
    }

    pub fn sql(&self) -> &str {
        &self.sql
    }

    pub fn no_transaction(&self) -> bool {
        self.no_transaction
    }
}

/// A type that needs the driver to provide the query to run.
pub trait FinalizeMigration<C>: Sized
where
    C: Executor,
{
    /// Create an instance of this type from a connection.
    fn initialize(conn: &mut C) -> Result<Self, <C as Executor>::Error>;

    /// Produce the SQL for the migration.
    fn finalize(&self, conn: &mut C) -> Result<String, <C as Executor>::Error>;
}

/// A type that needs the driver to asynchronously provide the query to run.
#[async_trait]
pub trait AsyncFinalizeMigration<C>: Sized
where
    C: AsyncExecutor + Send,
{
    /// Create an instance of this type from a connection.
    async fn initialize(conn: &mut C) -> Result<Self, <C as AsyncExecutor>::Error>;

    /// Produce the SQL for the migration.
    async fn finalize(&self, conn: &mut C) -> Result<String, <C as AsyncExecutor>::Error>;
}

/// Represents a migration that is either waiting to be
/// applied or already has been.
/// This is used by the [`embed_migrations!`] macro to gather
/// migration files and shouldn't be needed by the user.
///
/// [`embed_migrations!`]: macro.embed_migrations.html
#[derive(Clone, Debug)]
pub struct Migration {
    state: State,
    name: String,
    checksum: u64,
    version: i32,
    prefix: Type,
    content: Option<MigrationContent>,
    applied_on: Option<OffsetDateTime>,
}

impl Migration {
    /// Create an unapplied migration, name and version are parsed from the input_name,
    /// which must be named in the format (U|V){1}__{2}.rs where {1} represents the migration version and {2} the name.
    pub fn unapplied(
        input_name: &str,
        no_transaction: Option<bool>,
        sql: &str,
    ) -> Result<Migration, Error> {
        let (prefix, version, name) = parse_migration_name(input_name)?;

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
        let content = Some(MigrationContent {
            no_transaction: no_transaction.unwrap_or_default(),
            sql: sql.to_string(),
        });

        Ok(Migration {
            state: State::Unapplied,
            name,
            version,
            prefix,
            content,
            applied_on: None,
            checksum,
        })
    }

    /// Create an unapplied migration that needs its SQL query to be created first.
    pub fn finalize_unapplied<Fin, C: Executor>(
        conn: &mut C,
        input_name: &str,
        no_transaction: Option<bool>,
    ) -> Result<Migration, Error>
    where
        Fin: FinalizeMigration<C>,
    {
        let finalizer = Fin::initialize(conn).migration_err(
            &format!("unable to create finalizer for {input_name}"),
            None,
        )?;
        let sql = finalizer
            .finalize(conn)
            .migration_err(&format!("unable to finalize query for {input_name}"), None)?;
        Self::unapplied(input_name, no_transaction, &sql)
    }

    /// Create an unapplied migration that needs its SQL query to be created first.
    pub async fn async_finalize_unapplied<Fin, C: AsyncExecutor + Send>(
        conn: &mut C,
        input_name: &str,
        no_transaction: Option<bool>,
    ) -> Result<Migration, Error>
    where
        Fin: AsyncFinalizeMigration<C>,
    {
        let finalizer = Fin::initialize(conn).await.migration_err(
            &format!("unable to create finalizer for {input_name}"),
            None,
        )?;
        let sql = finalizer
            .finalize(conn)
            .await
            .migration_err(&format!("unable to finalize query for {input_name}"), None)?;
        Self::unapplied(input_name, no_transaction, &sql)
    }

    // Create a migration from an applied migration on the database
    pub fn applied(
        version: i32,
        name: String,
        applied_on: OffsetDateTime,
        checksum: u64,
    ) -> Migration {
        Migration {
            state: State::Applied,
            name,
            checksum,
            version,
            // applied migrations are always versioned
            prefix: Type::Versioned,
            content: None,
            applied_on: Some(applied_on),
        }
    }

    // convert the Unapplied into an Applied Migration
    pub fn set_applied(&mut self) {
        self.applied_on = Some(OffsetDateTime::now_utc());
        self.state = State::Applied;
    }

    /// Get the content of the migration
    pub fn content(&self) -> Option<&MigrationContent> {
        self.content.as_ref()
    }

    /// Get the SQL of the migration content
    pub fn sql(&self) -> Option<String> {
        self.content().map(|c| c.sql.clone())
    }

    /// Get the flag for running this migration in a transaction
    pub fn no_transaction(&self) -> Option<bool> {
        self.content().map(|c| c.no_transaction)
    }

    /// Get the Migration version
    pub fn version(&self) -> u32 {
        self.version as u32
    }

    /// Get the Prefix
    pub fn prefix(&self) -> &Type {
        &self.prefix
    }

    /// Get the Migration name
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Get the timestamp from when the Migration was applied. `None` when unapplied.
    /// Migrations returned from Runner::get_migrations() will always have `None`.
    pub fn applied_on(&self) -> Option<&OffsetDateTime> {
        self.applied_on.as_ref()
    }

    /// Get the Migration checksum. Checksum is formed from the name version and sql of the Migration
    pub fn checksum(&self) -> u64 {
        self.checksum
    }
}

impl fmt::Display for Migration {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "{}{}__{}", self.prefix, self.version, self.name)
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
