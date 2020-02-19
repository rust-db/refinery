use chrono::{DateTime, Local};
use regex::Regex;
use siphasher::sip::SipHasher13;

use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

use crate::{AsyncMigrate, Error, Migrate};

// regex used to match file names
pub fn file_match_re() -> Regex {
    Regex::new(r"^(V)(\d+(?:\.\d+)?)__(\w+)").unwrap()
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
/// this struct is used by the [`embed_migrations!`] and [`include_migration_mods!`] macros to gather migration files
/// and shouldn't be needed by the user
///
/// [`embed_migrations!`]: macro.embed_migrations.html
/// [`include_migration_mods!`]: macro.include_migration_mods.html
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
        // Previously, `std::collections::hash_map::DefaultHasher` was used
        // to calculate the checksum and the implementation at that time
        // was SipHasher13. However, that implementation is not guaranteed:
        // > The internal algorithm is not specified, and so it and its
        // > hashes should not be relied upon over releases.
        // We now explicitly use SipHasher13 to both remain compatible with
        // existing migrations and prevent breaking from possible future
        // changes to `DefaultHasher`.
        let mut hasher = SipHasher13::new();
        self.name.hash(&mut hasher);
        self.version.hash(&mut hasher);
        self.sql.hash(&mut hasher);
        hasher.finish()
    }

    pub fn as_applied(&self) -> AppliedMigration {
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

    /// Runs the Migrations in the supplied database connection
    pub fn run<'a, C>(&self, conn: &'a mut C) -> Result<(), Error>
    where
        C: Migrate,
    {
        Migrate::migrate(
            conn,
            &self.migrations,
            self.abort_divergent,
            self.abort_missing,
            self.grouped,
        )
    }

    /// Runs the Migrations asynchronously in the supplied database connection
    pub async fn run_async<C>(&self, conn: &mut C) -> Result<(), Error>
    where
        C: AsyncMigrate + Send,
    {
        AsyncMigrate::migrate(
            conn,
            &self.migrations,
            self.abort_divergent,
            self.abort_missing,
            self.grouped,
        )
        .await
    }
}
