pub mod config;
mod drivers;
pub mod error;
mod runner;
pub mod traits;
mod util;

pub use crate::error::Error;
pub use crate::runner::{Migration, MigrationEnum, Report, Runner, Target};
pub use crate::traits::r#async::AsyncMigrate;
pub use crate::traits::sync::Migrate;
pub use crate::util::{find_migration_files, parse_migration_name, MigrationType};

#[cfg(feature = "rusqlite")]
pub use rusqlite;

#[cfg(feature = "postgres")]
pub use postgres;

#[cfg(feature = "mysql")]
pub use mysql;

#[cfg(feature = "tokio-postgres")]
pub use tokio_postgres;

#[cfg(feature = "mysql_async")]
pub use mysql_async;

#[cfg(feature = "tiberius")]
pub use tiberius;
