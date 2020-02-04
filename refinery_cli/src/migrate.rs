use std::path::Path;

use anyhow::{Context, Result};
use clap::ArgMatches;
use refinery::{
    config::{migrate_from_config, Config},
    Migration,
};

use crate::util::find_migration_files;

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
    let migration_files_path = find_migration_files(path)?;
    let mut migrations = Vec::new();
    for path in migration_files_path {
        let sql = std::fs::read_to_string(path.as_path())
            .with_context(|| format!("could not read migration file name {}", path.display()))?;

        //safe to call unwrap as find_migration_filenames returns canonical paths
        let filename = path
            .file_stem()
            .and_then(|file| file.to_os_string().into_string().ok())
            .unwrap();

        let migration = Migration::from_filename(&filename, &sql)
            .with_context(|| format!("could not read migration file name {}", path.display()))?;
        migrations.push(migration);
    }
    let config =
        Config::from_file_location(config_location).context("could not parse the config file")?;
    migrate_from_config(&config, grouped, divergent, missing, &migrations)?;
    Ok(())
}
