use std::path::Path;

use anyhow::Context;
use clap::ArgMatches;
use refinery_core::{
    config::Config, find_migration_files, Migration, MigrationType, Runner, Target,
};

pub fn handle_migration_command(args: &ArgMatches) -> anyhow::Result<()> {
    //safe to call unwrap as we specified default values
    let config_location = args.value_of("config").unwrap();
    let grouped = args.is_present("grouped");
    let divergent = !args.is_present("divergent");
    let missing = !args.is_present("missing");
    let env_var_opt = args.value_of("env-var");
    let fake = args.is_present("fake");
    let target = args.value_of("target");
    //safe to call unwrap as we specified default value
    let path = args.value_of("path").unwrap();

    run_migrations(
        config_location,
        grouped,
        divergent,
        missing,
        fake,
        target,
        env_var_opt,
        path,
    )?;
    Ok(())
}

fn run_migrations(
    config_location: &str,
    grouped: bool,
    divergent: bool,
    missing: bool,
    fake: bool,
    target: Option<&str>,
    env_var_opt: Option<&str>,
    path: &str,
) -> anyhow::Result<()> {
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
    let mut config = config(config_location, env_var_opt)?;

    let target = match (fake, target) {
        (true, None) => Target::Fake,
        (false, None) => Target::Latest,
        (true, Some(t)) => {
            Target::FakeVersion(t.parse::<u32>().expect("could not parse target version"))
        }
        (false, Some(t)) => {
            Target::Version(t.parse::<u32>().expect("could not parse target version"))
        }
    };

    cfg_if::cfg_if! {
        if #[cfg(any(feature = "mysql", feature = "postgresql", feature = "sqlite"))] {
            Runner::new(&migrations)
                .set_grouped(grouped)
                .set_abort_divergent(divergent)
                .set_abort_missing(missing)
                .set_target(target)
                .run(&mut config)?;
        }
    }

    cfg_if::cfg_if! {
        // tiberius is an async driver so we spawn tokio runtime and run the migrations
        if #[cfg(feature = "mssql")] {
            use tokio::runtime::Builder;

            let runtime = Builder::new_current_thread()
            .enable_all()
            .build()
            .context("Can't start tokio runtime")?;

            runtime.block_on(async {
                Runner::new(&migrations)
                    .set_grouped(grouped)
                    .set_abort_divergent(divergent)
                    .set_abort_missing(missing)
                    .run_async(&mut config)
                    .await
                })?;
        }
    }

    Ok(())
}

fn config(config_location: &str, env_var_opt: Option<&str>) -> anyhow::Result<Config> {
    if let Some(env_var) = env_var_opt {
        Config::from_env_var(env_var).context("could not environment variable")
    } else {
        Config::from_file_location(config_location).context("could not parse the config file")
    }
}
