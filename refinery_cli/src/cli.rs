//! Defines the CLI application

use clap::{App, Arg, SubCommand};

const APP_NAME: &'static str = "refinery";
const VERSION: &'static str = env!("CARGO_PKG_VERSION");

/// Initialise the CLI parser for our app
pub fn create_cli() -> App<'static, 'static> {
    /* The setup cmd handles initialisation */
    let setup = SubCommand::with_name("setup")
        .about("Run the refinary setup hooks")
        .arg(
            Arg::with_name("MIGRATION_DIR")
                .short("d")
                .long("migrations-dir")
                .help(
                    "Specify a location for the migrations directory. By default \
                     this will create a `migrations` folder for all future operations",
                ),
        );

    /* The migrations cmd handles all migration actions */
    let migrations = SubCommand::with_name("migrations")
        .about("A series of commands to operate on  migrations.")
        .subcommand(
            SubCommand::with_name("up")
                .display_order(1)
                .about("Run a series of up migrations.")
                .arg(number_arg()),
        )
        .subcommand(
            SubCommand::with_name("down")
                .display_order(2)
                .about("Run a series of down migrations.")
                .arg(number_arg()),
        )
        .subcommand(
            SubCommand::with_name("generate")
                .about("Generate a new migration directory and files")
                .arg(
                    Arg::with_name("type")
                        .short("t")
                        .long("type")
                        .help(
                            "Choose between having a single `change` function for a \
                             migration or using an `up` and `down` function seperately.",
                        )
                        .possible_values(&["changed", "updown"])
                        .default_value("updown"),
                ),
        )
        .subcommand(SubCommand::with_name("list").about(
            "List currently available migrations and their\
             applied state in the database",
        ));

    /* Create an app and return it */
    return App::new(APP_NAME)
        .version(VERSION)
        .subcommand(setup)
        .subcommand(migrations);
}

fn number_arg() -> Arg<'static, 'static> {
    return Arg::with_name("number").help("Specify the number of migrations to run");
}
