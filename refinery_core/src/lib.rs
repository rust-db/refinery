pub mod config;
mod drivers;
pub mod error;
mod runner;
pub mod traits;
mod util;

pub use crate::error::Error;
pub use crate::runner::{Migration, MigrationContent, Report, Runner, Target};
pub use crate::traits::r#async::AsyncMigrate;
pub use crate::traits::sync::Migrate;
pub use crate::util::{
    find_migration_files, load_sql_migrations, parse_migration_name, parse_no_transaction,
    MigrationType,
};

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

#[cfg(feature = "sqlx-postgres")]
pub mod sqlx_postgres {
    pub use sqlx::postgres::*;
}
