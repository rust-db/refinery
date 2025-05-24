//! Main entry point for the refinery cli tool

mod cli;
mod generate;
mod migrate;
mod rollback;
mod setup;

use anyhow::{Context, Error};
use clap::Parser;
use env_logger::{Builder, Target};
use log::LevelFilter;
use refinery_core::config::Config;
use std::{io::Write, path::Path};

use cli::Cli;

fn main() -> Result<(), Error> {
    human_panic::setup_panic!();

    let mut builder = Builder::new();
    builder
        .format(|buf, record| writeln!(buf, "{}", record.args()))
        .filter(Some("refinery_core::traits"), LevelFilter::Info)
        .target(Target::Stdout)
        .init();

    let cli = Cli::parse();

    match cli {
        Cli::Setup => setup::handle_setup()?,
        Cli::Migrate(args) => migrate::handle_migration_command(args)?,
        Cli::Rollback(args) => rollback::handle_rollback_command(args)?,
        Cli::Generate(args) => generate::handle_generate_command(args)?,
    }

    Ok(())
}

fn config(config_location: &Path, env_var_opt: Option<&str>) -> anyhow::Result<Config> {
    if let Some(env_var) = env_var_opt {
        Config::from_env_var(env_var).context("could not environment variable")
    } else {
        Config::from_file_location(config_location).context("could not parse the config file")
    }
}
