//! Defines the CLI application

use crate::{APP_NAME, VERSION};
use clap::{App, AppSettings, Arg, SubCommand};

/// Initialise the CLI parser for our app
pub fn create_cli() -> App<'static, 'static> {
    /* The setup cmd handles initialisation */
    let setup = SubCommand::with_name("setup")
        .about("Run the refinery setup hooks to generate the config file");

    let migrate = SubCommand::with_name("migrate")
        .about("Refinery's main migrate operation")
        .arg(
            Arg::with_name("config")
                .short("c")
                .help("give a config file location")
                .default_value("./Refinery.toml"),
        )
        .arg(
            Arg::with_name("grouped")
                .short("g")
                .help("run migrations grouped in a single transaction")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("divergent")
                .short("d")
                .help("if set, migrates even if divergent migrations are found")
                .takes_value(false),
        )
        .arg(
            Arg::with_name("missing")
                .short("m")
                .help("if set, migrates even if missing migrations are found")
                .takes_value(false),
        )
        // .subcommand(
        //     SubCommand::with_name("mod")
        //         .display_order(1)
        //         .about("Run migrations in rust modules")
        //         .arg_from_usage("<location> 'migrations module path i.e. crate::migrations'"),
        // )
        .subcommand(
            SubCommand::with_name("files")
                .display_order(2)
                .about("Run migrations in .sql files")
                .arg(
                    Arg::with_name("path")
                        .short("p")
                        .help("migrations dir path")
                        .default_value("./migrations")
                        .empty_values(false),
                ),
        )
        .setting(AppSettings::SubcommandRequired);

    /* Create an app and return it */
    App::new(APP_NAME)
        .version(VERSION)
        .subcommand(setup)
        .subcommand(migrate)
        .setting(AppSettings::SubcommandRequiredElseHelp)
}
