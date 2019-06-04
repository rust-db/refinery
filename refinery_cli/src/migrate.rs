use clap::ArgMatches;
use failure::{format_err, Error, ResultExt};
use refinery_migrations::{
    find_migrations_filenames, migrate_from_config, Config, ConfigDbType, Migration, MigrationType,
};
use std::fs;
use std::path::Path;

pub fn get_config(location: &str) -> Result<Config, Error> {
    let file = std::fs::read_to_string(location)
        .with_context(|err| format!("could not open config file, {}", err))?;

    let mut config: Config = toml::from_str(&file)
        .with_context(|err| format!("could not parse config file, {}", err))?;

    //replace relative path with canonical path in case of Sqlite db
    if config.main.db_type == ConfigDbType::Sqlite {
        let config_db_path = config
            .main
            .db_path
            .ok_or_else(|| format_err!("field path must be present for Sqlite database type"))?;
        let mut config_db_path = Path::new(&config_db_path).to_path_buf();

        if config_db_path.is_relative() {
            let mut config_db_dir = Path::new(location)
                .parent()
                //safe to call unwrap in the below cases as the current dir exists and if config was opened there are permissions on the current dir
                .unwrap_or(&std::env::current_dir().unwrap())
                .to_path_buf();

            config_db_dir = fs::canonicalize(config_db_dir).unwrap();
            config_db_path = config_db_dir.join(&config_db_path)
        }

        let config_db_path = config_db_path
            .into_os_string()
            .into_string()
            .map_err(|_| format_err!("sqlite db file location must be a valid utf-8 string"))?;
        config.main.db_path = Some(config_db_path);
    }

    Ok(config)
}

pub fn handle_migration_command(args: &ArgMatches) -> Result<(), Error> {
    //safe to call unwrap as we specified default values
    let config_location = args.value_of("config").unwrap();
    let grouped = args.is_present("grouped");
    let divergent = !args.is_present("divergent");
    let missing = !args.is_present("missing");

    let config = get_config(config_location)?;

    match args.subcommand() {
        ("files", Some(args)) => run_files_migrations(config, grouped, divergent, missing, args)?,
        _ => unreachable!("Can't touch this..."),
    }
    Ok(())
}

fn run_files_migrations(
    config: Config,
    grouped: bool,
    divergent: bool,
    missing: bool,
    arg: &ArgMatches,
) -> Result<(), Error> {
    //safe to call unwrap as we specified default value
    let path = arg.value_of("path").unwrap();
    let path = Path::new(path);
    let migration_files_path = find_migrations_filenames(Some(path), MigrationType::Sql, true)?;
    let mut migrations = Vec::new();
    for path in migration_files_path.iter() {
        let sql = std::fs::read_to_string(path)
            .with_context(|err| format!("could not read migration file name {}, {}", path, err))?;

        //safe to call unwrap as find_migration_filenames returns canonical paths
        let filename = Path::new(path)
            .file_stem()
            .and_then(|file| file.to_os_string().into_string().ok())
            .unwrap();

        let migration = Migration::from_filename(&filename, &sql)
            .with_context(|err| format!("could not read migration file name {}, {}", path, err))?;
        migrations.push(migration);
    }
    migrate_from_config(&config, grouped, divergent, missing, &migrations)?;
    Ok(())
}
