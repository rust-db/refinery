mod config;
mod error;
mod traits;
mod utils;

use chrono::{DateTime, Local};
use std::cmp::Ordering;
use std::collections::hash_map::DefaultHasher;
use std::fmt;
use std::hash::{Hash, Hasher};

pub use config::{Config, ConfigDbType, Main};
pub use error::{Error, WrapMigrationError};
pub use traits::{
    CommitTransaction, DefaultQueries, ExecuteMultiple, Migrate, MigrateGrouped, Query, Transaction,
};
use utils::RE;
pub use utils::{file_match_re, find_migrations_filenames, MigrationType};

#[cfg(all(feature = "mysql", feature = "postgres", feature = "rusqlite"))]
pub use utils::migrate_from_config;

#[cfg(feature = "rusqlite")]
pub mod rusqlite;

#[cfg(feature = "postgres")]
pub mod postgres;

#[cfg(feature = "mysql")]
pub mod mysql;

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
    pub name: String,
    pub version: usize,
    pub prefix: MigrationPrefix,
    pub sql: String,
}

impl Migration {
    pub fn from_filename(name: &str, sql: &str) -> Result<Migration, Error> {
        let captures = RE
            .captures(name)
            .filter(|caps| caps.len() == 4)
            .ok_or(Error::InvalidName)?;
        let version = captures[2].parse().map_err(|_| Error::InvalidVersion)?;

        let name = (&captures[3]).into();
        let prefix = match &captures[1] {
            "V" => MigrationPrefix::Versioned,
            _ => unreachable!(),
        };

        Ok(Migration {
            name,
            version,
            sql: sql.into(),
            prefix,
        })
    }

    pub fn checksum(&self) -> u64 {
        let mut hasher = DefaultHasher::new();
        self.name.hash(&mut hasher);
        self.version.hash(&mut hasher);
        self.sql.hash(&mut hasher);
        hasher.finish()
    }

    pub fn to_applied(&self) -> AppliedMigration {
        AppliedMigration {
            name: self.name.clone(),
            version: self.version,
            checksum: self.checksum().to_string(),
            applied_on: Local::now(),
        }
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

#[derive(Clone, Debug)]
pub struct AppliedMigration {
    pub name: String,
    pub version: usize,
    pub applied_on: DateTime<Local>,
    pub checksum: String,
}

impl Eq for AppliedMigration {}

impl PartialEq for AppliedMigration {
    fn eq(&self, other: &AppliedMigration) -> bool {
        self.version == other.version && self.name == other.name && self.checksum == other.checksum
    }
}

impl fmt::Display for AppliedMigration {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(fmt, "V{}__{}", self.version, self.name)
    }
}

///Struct that represents the entrypoint to run the migrations,
///an instance of this struct is returned by the [embed_migrations](../refinery_macros/macro.embed_migrations.html) and the [mod_migrations](../refinery_macros/macro.mod_migrations.html)
/// runner function, Runner should not need to be instantiated manually
pub struct Runner {
    grouped: bool,
    abort_divergent: bool,
    abort_missing: bool,
    migrations: Vec<Migration>,
}

impl Runner {
    pub fn new(migrations: &[Migration]) -> Runner {
        Runner {
            grouped: false,
            abort_divergent: true,
            abort_missing: true,
            migrations: migrations.to_vec(),
        }
    }

    /// Set true if all migrations should be grouped and run in a single transaction
    /// by default this is set to false
    pub fn set_grouped(self, grouped: bool) -> Runner {
        Runner { grouped, ..self }
    }

    /// Set true if migration process should abort if divergent migrations are found
    /// i.e. applied migrations with the same version but different name or checksum from the ones on the filesystem
    /// by default this is set to true
    pub fn set_abort_divergent(self, abort_divergent: bool) -> Runner {
        Runner {
            abort_divergent,
            ..self
        }
    }

    /// Set true if migration process should abort if missing migrations are found
    /// i.e. applied migrations that are not found on the filesystem, or migrations found on filesystem with a version inferior to the last one applied but not applied
    pub fn set_abort_missing(self, abort_divergent: bool) -> Runner {
        Runner {
            abort_divergent,
            ..self
        }
    }

    /// Runs the Migrations in the supplied database connection
    pub fn run<'a, C>(&self, conn: &'a mut C) -> Result<(), Error>
    where
        C: MigrateGrouped<'a> + Migrate,
    {
        if self.grouped {
            MigrateGrouped::migrate(
                conn,
                &self.migrations,
                self.abort_divergent,
                self.abort_missing,
            )?;
        } else {
            Migrate::migrate(
                conn,
                &self.migrations,
                self.abort_divergent,
                self.abort_missing,
            )?;
        }
        Ok(())
    }
}
