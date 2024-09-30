pub mod config;
mod drivers;
pub mod error;
pub mod executor;
mod migration;
mod runner;
mod util;

pub use self::error::Error;
pub use self::executor::{
    async_exec::{AsyncExecutor, AsyncMigrate},
    exec::{Executor, Migrate},
};
pub use self::migration::{
    AsyncFinalizeMigration, FinalizeMigration, Migration, MigrationContent, Target,
};
pub use self::runner::{Report, Runner};
pub use self::util::{
    find_migration_files, load_sql_migrations, parse_finalize_migration, parse_migration_name,
    parse_no_transaction, MigrationType,
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
