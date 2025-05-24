use std::{num::NonZero, path::Path};

use anyhow::{bail, Context};
use refinery_core::{
    config::ConfigDbType, find_migration_files, parse_sql_migration_files, MigrationType,
    RollbackTarget, Runner,
};

use crate::{cli::RollbackArgs, config};

pub fn handle_rollback_command(args: RollbackArgs) -> anyhow::Result<()> {
    let target = parse_target(args.target, args.count, args.all)?;

    run_rollback(
        &args.config,
        args.grouped,
        args.divergent,
        args.missing_on_filesystem,
        args.missing_on_applied,
        target,
        args.env_var.as_deref(),
        &args.path,
        &args.table_name,
    )?;
    Ok(())
}

fn parse_target(
    target: Option<i64>,
    rollback_count: Option<NonZero<u32>>,
    rollback_all: bool,
) -> anyhow::Result<RollbackTarget> {
    let conflicting_targets = [rollback_count.is_some(), rollback_all, target.is_some()]
        .iter()
        .filter(|x| **x)
        .count()
        > 1;

    if conflicting_targets {
        bail!("You can only specify one of --count, --all or --target options at a time.");
    }

    if rollback_all {
        Ok(RollbackTarget::All)
    } else if let Some(count) = rollback_count {
        Ok(RollbackTarget::Count(count))
    } else if let Some(version) = target {
        Ok(RollbackTarget::Version(version))
    } else {
        Ok(RollbackTarget::Count(NonZero::<u32>::new(1).unwrap()))
    }
}

#[allow(clippy::too_many_arguments)]
fn run_rollback(
    config_location: &Path,
    grouped: bool,
    divergent: bool,
    missing_on_filesystem: bool,
    missing_on_applied: bool,
    target: RollbackTarget,
    env_var_opt: Option<&str>,
    path: &Path,
    table_name: &str,
) -> anyhow::Result<()> {
    let migration_files = find_migration_files(path, MigrationType::Sql)?;
    let migrations = parse_sql_migration_files(migration_files)?;
    let mut config = config(config_location, env_var_opt)?;

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
                            .set_rollback_target(target)
                            .set_abort_divergent(divergent)
                            .set_abort_missing_on_filesystem(missing_on_filesystem)
                            .set_abort_missing_on_applied(missing_on_applied)
                            .set_migration_table_name(table_name)
                            .rollback_async(&mut config)
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
                        .set_abort_missing_on_filesystem(missing_on_filesystem)
                        .set_abort_missing_on_applied(missing_on_applied)
                        .set_rollback_target(target)
                        .set_migration_table_name(table_name)
                        .rollback(&mut config)?;
                } else {
                    panic!("tried to migrate async from config for a {:?} database, but it's matching feature was not enabled!", _db_type);
                }
            }
        }
    };

    Ok(())
}
