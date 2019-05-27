//! Main entry point for the refinery cli tool
//!
//!

use clap::ArgMatches;

mod cli;
mod setup;
mod config;

fn main() {
    let matches = cli::create_cli().get_matches();

    match matches.subcommand() {
        ("migrations", Some(matches)) => run_migration_command(matches),
        ("setup", Some(matches)) => setup::handle_setup(matches),
        _ => unreachable!("Can't touch this..."),
    }
}

fn run_migration_command(matches: &ArgMatches) {
    match matches.subcommand() {
        ("generate", Some(_)) => {}
        ("list", Some(_)) => {}
        ("up", Some(_)) => {}
        ("down", Some(_)) => {}
        _ => unreachable!("Can't touch this..."),
    }
}
