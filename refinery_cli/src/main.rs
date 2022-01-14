//! Main entry point for the refinery cli tool

mod cli;
mod migrate;
mod setup;

use anyhow::Error;
use clap::Parser;
use env_logger::{Builder, Target};
use log::LevelFilter;
use std::io::Write;

use cli::Cli;

const APP_NAME: &str = "refinery";
const VERSION: &str = env!("CARGO_PKG_VERSION");

fn main() -> Result<(), Error> {
    human_panic::setup_panic!(Metadata {
        name: APP_NAME.into(),
        version: VERSION.into(),
        authors: "Katharina Fey <kookie@spacekookie.de>, João Oliveira <hello@jxs.pt>".into(),
        homepage: "https://github.com/rust-db/refinery/".into(),
    });

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
    }

    Ok(())
}
