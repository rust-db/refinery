//! Defines the CLI application

use std::path::PathBuf;

use clap::{Args, Parser};

#[derive(Parser)]
#[clap(version)]
pub enum Cli {
    /// Run the refinery setup hooks to generate the config file
    Setup,

    /// Refinery's main migrate operation
    Migrate(MigrateArgs),
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
    #[clap(short)]
    pub target: Option<i64>,

    /// Set migration table name
    #[clap(long, default_value = "refinery_schema_history")]
    pub table_name: String,

    /// Should abort if divergent migrations are found
    #[clap(short)]
    pub divergent: bool,

    /// Should abort if missing migrations are found
    #[clap(short)]
    pub missing: bool,
}
