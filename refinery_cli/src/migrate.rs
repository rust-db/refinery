use std::path::Path;

use anyhow::Context;
use refinery_core::{
    config::{Config, ConfigDbType},
    find_migration_files, parse_sql_migration_files, MigrationType, Runner, Target,
};

use crate::cli::MigrateArgs;

pub fn handle_migration_command(args: MigrateArgs) -> anyhow::Result<()> {
    run_migrations(
        &args.config,
        args.grouped,
        args.divergent,
        args.missing,
        args.fake,
        args.target,
        args.env_var.as_deref(),
        &args.path,
        &args.table_name,
    )?;
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn run_migrations(
    config_location: &Path,
    grouped: bool,
    divergent: bool,
    missing: bool,
    fake: bool,
    target: Option<u32>,
    env_var_opt: Option<&str>,
    path: &Path,
    table_name: &str,
) -> anyhow::Result<()> {
    let migration_files = find_migration_files(path, MigrationType::Sql)?;
    let migrations = parse_sql_migration_files(migration_files)?;
    let mut config = config(config_location, env_var_opt)?;

    let target = match (fake, target) {
        (true, None) => Target::Fake,
        (false, None) => Target::Latest,
        (true, Some(version)) => Target::FakeVersion(version),
        (false, Some(version)) => Target::Version(version),
    };

    match config.db_type() {
        ConfigDbType::Mssql => {
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
                            .set_target(target)
                            .set_abort_divergent(divergent)
                            .set_abort_missing(missing)
                            .set_migration_table_name(table_name)
                            .run_async(&mut config)
                            .await
                    })?;
                } else {
                    panic!("tried to migrate async from config for a mssql database, but mssql feature was not enabled!");
                }
            }
        }
        _db_type @ (ConfigDbType::Mysql | ConfigDbType::Postgres | ConfigDbType::Sqlite) => {
            cfg_if::cfg_if! {
                if #[cfg(any(feature = "mysql", feature = "postgresql", feature = "sqlite"))] {
                    Runner::new(&migrations)
                        .set_grouped(grouped)
                        .set_abort_divergent(divergent)
                        .set_abort_missing(missing)
                        .set_target(target)
                        .set_migration_table_name(table_name)
                        .run(&mut config)?;
                } else {
                    panic!("tried to migrate async from config for a {:?} database, but it's matching feature was not enabled!", _db_type);
                }
            }
        }
    };

    Ok(())
}

fn config(config_location: &Path, env_var_opt: Option<&str>) -> anyhow::Result<Config> {
    if let Some(env_var) = env_var_opt {
        Config::from_env_var(env_var).context("could not environment variable")
    } else {
        Config::from_file_location(config_location).context("could not parse the config file")
    }
}
