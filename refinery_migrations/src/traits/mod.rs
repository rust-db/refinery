#[cfg(feature = "async")]
pub mod r#async;
#[cfg(feature = "sync")]
pub mod sync;

use crate::{AppliedMigration, Error, Migration};

//checks for missing migrations on filesystem or apllied migrations with a different name and checksum but same version
//if abort_divergent or abort_missing are true returns Err on those cases, else returns the list of migrations to be applied
pub(crate) fn check_missing_divergent(
    applied: Vec<AppliedMigration>,
    mut migrations: Vec<Migration>,
    abort_divergent: bool,
    abort_missing: bool,
) -> Result<Vec<Migration>, Error> {
    migrations.sort();
    let current = match applied.last() {
        Some(last) => last.clone(),
        None => {
            log::info!("schema history table is empty, going to apply all migrations");
            return Ok(migrations);
        }
    };

    for app in applied.iter() {
        // iterate applied migrations on database and assert all migrations
        // applied on database exist on the filesyste and have the same checksum
        match migrations.iter().find(|m| m.version == app.version) {
            None => {
                if abort_missing {
                    return Err(Error::MissingVersion(app.clone()));
                } else {
                    log::error!("migration {} is missing from the filesystem", app);
                }
            }
            Some(migration) => {
                if &migration.as_applied() != app {
                    if abort_divergent {
                        return Err(Error::DivergentVersion(app.clone(), migration.clone()));
                    } else {
                        log::error!(
                            "applied migration {} is different than filesystem one {}",
                            app,
                            migration
                        );
                    }
                }
            }
        }
    }

    log::info!("current version: {}", current.version);
    let mut to_be_applied = Vec::new();
    // iterate all migration files found on file system and assert that there are not migrations missing:
    // migrations which its version is inferior to the current version on the database, yet were not applied.
    // select to be applied all migrations with version greater than current
    for migration in migrations.into_iter() {
        if applied
            .iter()
            .find(|app| app.version == migration.version)
            .is_none()
        {
            if current.version >= migration.version {
                if abort_missing {
                    return Err(Error::MissingVersion(migration.as_applied()));
                } else {
                    log::error!("found migration on filsystem {} not applied", migration);
                }
            } else {
                to_be_applied.push(migration);
            }
        }
    }
    // with these two iterations we both assert that all migrations found on the database
    // exist on the file system and have the same checksum, and all migrations found
    // on the file system are either on the database, or greater than the current, and therefore going to be applied
    Ok(to_be_applied)
}

pub(crate) const ASSERT_MIGRATIONS_TABLE: &str =
    "CREATE TABLE IF NOT EXISTS refinery_schema_history( \
             version INTEGER PRIMARY KEY,\
             name VARCHAR(255),\
             applied_on VARCHAR(255),
             checksum VARCHAR(255));";

pub(crate) const GET_APPLIED_MIGRATIONS: &str =
    "SELECT version, name, applied_on, checksum \
     FROM refinery_schema_history ORDER BY version ASC;";
