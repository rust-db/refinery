pub mod config;
mod drivers;
pub mod error;
mod runner;
mod traits;
mod util;

pub use crate::error::Error;
pub use crate::runner::{Migration, Report, Runner, Target};
pub use crate::traits::r#async::AsyncMigrate;
pub use crate::traits::sync::Migrate;
pub use crate::util::{find_migration_files, MigrationType};

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

#[cfg(feature = "tokio")]
pub use tokio;
