//! Defines the CLI application

use std::{num::NonZero, path::PathBuf};

use clap::{Args, Parser};

#[derive(Parser)]
#[clap(version)]
pub enum Cli {
    /// Run the refinery setup hooks to generate the config file
    Setup,

    /// Refinery's main migrate operation
    Migrate(MigrateArgs),

    /// Rollback migrations
    Rollback(RollbackArgs),
}

#[derive(Args)]
pub struct MigrateArgs {
    /// Config file location
    #[clap(short, default_value = "./refinery.toml")]
    pub config: PathBuf,

    /// Migrations directory path
    #[clap(short, default_value = "./migrations")]
    pub path: PathBuf,

    /// Load database from the given environment variable
    #[clap(short)]
    pub env_var: Option<String>,

    /// Run migrations grouped in a single transaction
    #[clap(short)]
    pub grouped: bool,

    /// Do not actually run migrations, just create and update refinery's schema migration table
    #[clap(short)]
    pub fake: bool,

    /// Migrate to the specified target version
    #[clap(long)]
    pub target: Option<i64>,

    /// Set migration table name
    #[clap(long, default_value = "refinery_schema_history")]
    pub table_name: String,

    /// Should abort if divergent migrations are found
    #[clap(long)]
    pub divergent: bool,

    /// Should abort if missing migrations are found
    #[clap(long)]
    pub missing_on_filesystem: bool,

    /// Should abort if the migration is not found in the filesystem
    #[clap(long)]
    pub missing_on_applied: bool,
}

#[derive(Args)]
pub struct RollbackArgs {
    /// Config file location
    #[clap(short, default_value = "./refinery.toml")]
    pub config: PathBuf,

    /// Migrations directory path
    #[clap(short, default_value = "./migrations")]
    pub path: PathBuf,

    /// Load database from the given environment variable
    #[clap(short)]
    pub env_var: Option<String>,

    /// Run migrations grouped in a single transaction
    #[clap(short)]
    pub grouped: bool,

    /// Migrate to the specified target version
    #[clap(long)]
    pub target: Option<i64>,

    /// Rollback only this many migrations, by default only the last one is rolled back
    #[clap(long)]
    pub count: Option<NonZero<u32>>,

    /// Rollback all migrations, regardless of the target version
    #[clap(long)]
    pub all: bool,

    /// Set migration table name
    #[clap(long, default_value = "refinery_schema_history")]
    pub table_name: String,

    /// Should abort if divergent migrations are found
    #[clap(long)]
    pub divergent: bool,

    /// Should abort if missing migrations are found
    #[clap(long)]
    pub missing_on_filesystem: bool,

    /// Should abort if the migration is not found in the filesystem
    #[clap(long)]
    pub missing_on_applied: bool,
}
