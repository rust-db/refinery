use std::path::Path;

use anyhow::{Context, Result};
use clap::ArgMatches;
use refinery_core::{config::Config, find_migration_files, Migration, MigrationType, Runner};

pub fn handle_migration_command(args: &ArgMatches) -> Result<()> {
    //safe to call unwrap as we specified default values
    let config_location = args.value_of("config").unwrap();
    let grouped = args.is_present("grouped");
    let divergent = !args.is_present("divergent");
    let missing = !args.is_present("missing");

    match args.subcommand() {
        ("files", Some(args)) => {
            run_files_migrations(config_location, grouped, divergent, missing, args)?
        }
        _ => unreachable!("Can't touch this..."),
    }
    Ok(())
}

fn run_files_migrations(
    config_location: &str,
    grouped: bool,
    divergent: bool,
    missing: bool,
    arg: &ArgMatches,
) -> Result<()> {
    //safe to call unwrap as we specified default value
    let path = arg.value_of("path").unwrap();
    let path = Path::new(path);
    let migration_files_path = find_migration_files(path, MigrationType::Sql)?;
    let mut migrations = Vec::new();
    for path in migration_files_path {
        let sql = std::fs::read_to_string(path.as_path())
            .with_context(|| format!("could not read migration file name {}", path.display()))?;

        //safe to call unwrap as find_migration_filenames returns canonical paths
        let filename = path
            .file_stem()
            .and_then(|file| file.to_os_string().into_string().ok())
            .unwrap();

        let migration = Migration::unapplied(&filename, &sql)
            .with_context(|| format!("could not read migration file name {}", path.display()))?;
        migrations.push(migration);
    }
    let mut config =
        Config::from_file_location(config_location).context("could not parse the config file")?;
    Runner::new(&migrations)
        .set_grouped(grouped)
        .set_abort_divergent(divergent)
        .set_abort_missing(missing)
        .run(&mut config)?;
    Ok(())
}
