#[cfg(feature = "config")]
pub mod config;
mod drivers;
pub mod error;
mod runner;
pub mod traits;
mod util;

pub use crate::error::Error;
pub use crate::runner::{Migration, Report, Runner, Target};
pub use crate::traits::r#async::AsyncMigrate;
pub use crate::traits::sync::Migrate;
pub use crate::util::{
    find_migration_files, load_sql_migrations, parse_migration_name, MigrationType, SchemaVersion,
};

#[cfg(feature = "rusqlite")]
pub use rusqlite;

#[cfg(feature = "postgres-no-tls")]
pub use postgres;

#[cfg(feature = "mysql")]
pub use mysql;

#[cfg(feature = "tokio-postgres-no-tls")]
pub use tokio_postgres;

#[cfg(feature = "mysql_async")]
pub use mysql_async;

#[cfg(feature = "tiberius")]
pub use tiberius;
