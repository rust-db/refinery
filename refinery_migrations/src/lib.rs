mod error;
mod traits;

use regex::Regex;
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};
use chrono::{DateTime, Local};

pub use error::{Error, WrapMigrationError};
pub use traits::{Transaction, DefaultQueries, CommitTransaction, ExecuteMultiple, Query, Migrate, MigrateGrouped};

#[cfg(feature = "rusqlite")]
pub mod rusqlite;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "mysql")]
pub mod mysql;

// regex used to match file names
pub fn file_match_re() -> Regex {
    Regex::new(r"^([V])([\d|\.]+)__(\w+)").unwrap()
}

lazy_static::lazy_static! {
    static ref RE: regex::Regex = file_match_re();
}

/// An enum set that represents the prefix for the Migration, at the moment only Versioned is supported
#[derive(Clone, Debug)]
pub enum MigrationPrefix {
    Versioned,
}

/// Represents a schema migration to be run on the database,
/// this struct is used by the [embed_migrations](../refinery_macros/macro.embed_migrations.html) and the [mod_migrations](../refinery_macros/macro.mod_migrations.html) to gather migration files
/// and shouldn't be needed by the user
#[derive(Clone, Debug)]
pub struct Migration {
    name: String,
    version: usize,
    prefix: MigrationPrefix,
    sql: String,
}

impl Migration {
    pub fn from_filename(name: &str, sql: &str) -> Result<Migration, Error> {
        let captures = RE
            .captures(name)
            .filter(|caps| caps.len() == 4)
            .ok_or(Error::InvalidName)?;
        let version = captures[2]
            .parse()
            .map_err(|_| Error::InvalidVersion)?;

        let name = (&captures[3]).into();
        let prefix = match &captures[1] {
            "V" => MigrationPrefix::Versioned,
            _ => unreachable!(),
        };

        Ok(Migration {
            name,
            version,
            sql: sql.into(),
            prefix
        })
    }

    pub fn checksum(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.name.hash(&mut hasher);
        self.version.hash(&mut hasher);
        self.sql.hash(&mut hasher);
        hasher.finish()
    }
}

impl fmt::Display for Migration {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "V{}__{}", self.version, self.name)
    }
}

impl Eq for Migration {}

impl Ord for Migration {
    fn cmp(&self, other: &Migration) -> Ordering {
        self.version.cmp(&other.version)
    }
}

impl PartialEq for Migration {
    fn eq(&self, other: &Migration) -> bool {
        self.version == other.version
    }
}

impl PartialOrd for Migration {
    fn partial_cmp(&self, other: &Migration) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub struct AppliedMigration {
    name: String,
    version: usize,
    installed_on: DateTime<Local>,
    checksum: String,
}

///Struct that represents the entrypoint to run the migrations,
///an instance of this struct is returned by the [embed_migrations](../refinery_macros/macro.embed_migrations.html) and the [mod_migrations](../refinery_macros/macro.mod_migrations.html)
/// runner function, Runner should not need to be instantiated manually
pub struct Runner {
    grouped: bool,
    migrations: Vec<Migration>
}

impl Runner {
    pub fn new(migrations: &[Migration]) -> Runner {
        Runner {
            grouped: false,
            migrations: migrations.to_vec()
        }
    }

    /// Set true if all migrations should be grouped and run in a single transaction
    pub fn set_grouped(self, grouped: bool) -> Runner{
        Runner {grouped, ..self}
    }

    /// Runs the Migrations in the supplied database connection
    pub fn run<'a, C>(&self, conn: &'a mut C) -> Result<(), Error> where C: MigrateGrouped<'a> + Migrate {
        if self.grouped {
            MigrateGrouped::migrate(conn, &self.migrations)?;
        } else {
            Migrate::migrate(conn, &self.migrations)?;
        }
        Ok(())
    }
}
