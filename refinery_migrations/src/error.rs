use crate::{AppliedMigration, Migration};
use std::path::PathBuf;
use thiserror::Error;

/// Enum listing possible errors from Refinery.
#[derive(Debug, Error)]
pub enum Error {
    /// An Error from an invalid file name migration
    #[error("migration name must be in the format V{{number}}__{{name}}")]
    InvalidName,
    /// An Error from an invalid version on a file name migration
    #[error("migration version must be a valid integer")]
    InvalidVersion,
    /// An Error from an divergent version, the applied version is different to the filesystem one
    #[error("applied migration {0} is different than filesystem one {1}")]
    DivergentVersion(AppliedMigration, Migration),
    /// An Error from an divergent version, the applied version is missing on the filesystem
    #[error("migration {0} is missing from the filesystem")]
    MissingVersion(AppliedMigration),
    /// An Error from an invalid migrations path location
    #[error("invalid migrations path {0}, {1}")]
    InvalidMigrationPath(PathBuf, std::io::Error),
    /// An Error from an underlying database connection Error
    #[error("Error parsing config: {0}")]
    ConfigError(String),
    #[error("`{0}`, `{1}`")]
    Connection(String, #[source] Box<dyn std::error::Error + Sync + Send>),
}

pub trait WrapMigrationError<T, E> {
    fn migration_err(self, msg: &str) -> Result<T, Error>;
}

impl<T, E> WrapMigrationError<T, E> for Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn migration_err(self, msg: &str) -> Result<T, Error> {
        self.map_err(|err| Error::Connection(msg.into(), Box::new(err)))
    }
}
