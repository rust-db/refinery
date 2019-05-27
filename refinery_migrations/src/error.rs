use std::fmt;

/// Enum listing possible errors from Refinery.
#[derive(Debug)]
pub enum Error {
    /// An Error from an invalid file name migration
    InvalidName,
    /// An Error from an invalid version on a file name migration
    InvalidVersion,
    /// An Error from an underlying database connection Error
    ConnectionError(String, Box<dyn std::error::Error + Sync + Send>),
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::InvalidName => write!(
                fmt,
                "migration name must be in the format V{{number}}__{{name}}"
            )?,
            Error::InvalidVersion => {
                write!(fmt, "migration version must be a valid integer")?
            }
            Error::ConnectionError(msg, cause) => {
                write!(fmt, "{}, {}", msg, cause)?
            }
        }
        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::ConnectionError(_migration, cause) => Some(&**cause),
            _ => None,
        }
    }
}

pub trait WrapMigrationError<T, E> {
    fn migration_err(self, msg: &str) -> Result<T, Error>;
}

impl<T, E> WrapMigrationError<T, E> for Result<T, E>
where
    E: std::error::Error + Send + Sync + 'static,
{
    fn migration_err(self, msg: &str) -> Result<T, Error> {
        self.map_err(|err| Error::ConnectionError(msg.into(), Box::new(err)))
    }
}
