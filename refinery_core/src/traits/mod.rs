pub mod r#async;
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
             version INT4 PRIMARY KEY,\
             name VARCHAR(255),\
             applied_on VARCHAR(255),
             checksum VARCHAR(255));";

pub(crate) const GET_APPLIED_MIGRATIONS: &str = "SELECT version, name, applied_on, checksum \
     FROM refinery_schema_history ORDER BY version ASC;";

#[cfg(test)]
mod tests {
    use super::{check_missing_divergent, AppliedMigration, Error, Migration};

    fn get_migrations() -> Vec<Migration> {
        let migration1 = Migration::from_filename(
            "V1__initial.sql",
            include_str!("../../../refinery/tests/sql_migrations/V1-2/V1__initial.sql"),
        )
        .unwrap();

        let migration2 = Migration::from_filename(
            "V2__add_cars_and_motos_table.sql",
            include_str!(
                "../../../refinery/tests/sql_migrations/V1-2/V2__add_cars_and_motos_table.sql"
            ),
        )
        .unwrap();

        let migration3 = Migration::from_filename(
            "V3__add_brand_to_cars_table",
            include_str!(
                "../../../refinery/tests/sql_migrations/V3/V3__add_brand_to_cars_table.sql"
            ),
        )
        .unwrap();

        let migration4 = Migration::from_filename(
            "V4__add_year_field_to_cars",
            &"ALTER TABLE cars ADD year INTEGER;",
        )
        .unwrap();

        vec![migration1, migration2, migration3, migration4]
    }

    #[test]
    fn check_missing_divergent_returns_all_migrations_if_applied_are_empty() {
        let migrations = get_migrations();
        let applied: Vec<AppliedMigration> = Vec::new();
        let result = check_missing_divergent(applied, migrations.clone(), true, true).unwrap();
        assert_eq!(migrations, result);
    }

    #[test]
    fn check_missing_divergent_returns_unapplied() {
        let migrations = get_migrations();
        let applied: Vec<AppliedMigration> = vec![
            migrations[0].clone().as_applied(),
            migrations[1].clone().as_applied(),
            migrations[2].clone().as_applied(),
        ];
        let remaining = vec![migrations[3].clone()];
        let result = check_missing_divergent(applied, migrations, true, true).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn check_missing_divergent_fails_on_divergent() {
        let migrations = get_migrations();
        let mut applied: Vec<AppliedMigration> = vec![
            migrations[0].clone().as_applied(),
            migrations[1].clone().as_applied(),
            migrations[2].clone().as_applied(),
        ];
        applied[2].checksum = "3a6d3a3".into();
        let migration = migrations[2].clone();
        let err = check_missing_divergent(applied, migrations, true, true).unwrap_err();
        match err {
            Error::DivergentVersion(applied, divergent) => {
                assert_eq!(migration, divergent);
                assert_eq!("add_brand_to_cars_table", applied.name);
            }
            _ => panic!("failed test"),
        }
    }

    #[test]
    fn check_missing_divergent_doesnt_fail_on_divergent() {
        let migrations = get_migrations();
        let mut applied: Vec<AppliedMigration> = vec![
            migrations[0].clone().as_applied(),
            migrations[1].clone().as_applied(),
            migrations[2].clone().as_applied(),
        ];
        applied[2].checksum = "3a6d3a3".into();
        let remaining = vec![migrations[3].clone()];
        let result = check_missing_divergent(applied, migrations, false, true).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn check_missing_divergent_fails_on_missing_on_applied() {
        let migrations = get_migrations();
        let applied: Vec<AppliedMigration> = vec![
            migrations[0].clone().as_applied(),
            migrations[2].clone().as_applied(),
        ];
        let migration = migrations[1].clone();
        let err = check_missing_divergent(applied, migrations, true, true).unwrap_err();
        match err {
            Error::MissingVersion(missing) => {
                assert_eq!(migration.as_applied(), missing);
            }
            _ => panic!("failed test"),
        }
    }

    #[test]
    fn check_missing_divergent_fails_on_missing_on_filesystem() {
        let mut migrations = get_migrations();
        let applied: Vec<AppliedMigration> = vec![
            migrations[0].clone().as_applied(),
            migrations[1].clone().as_applied(),
            migrations[2].clone().as_applied(),
        ];
        let migration = migrations.remove(1);
        let err = check_missing_divergent(applied, migrations, true, true).unwrap_err();
        match err {
            Error::MissingVersion(missing) => {
                assert_eq!(migration.as_applied(), missing);
            }
            _ => panic!("failed test"),
        }
    }

    #[test]
    fn check_missing_divergent_doesnt_fail_on_missing_on_applied() {
        let migrations = get_migrations();
        let applied: Vec<AppliedMigration> = vec![
            migrations[0].clone().as_applied(),
            migrations[2].clone().as_applied(),
        ];
        let remaining = vec![migrations[3].clone()];
        let result = check_missing_divergent(applied, migrations, true, false).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn check_missing_divergent_doesnt_fail_on_missing_on_filesystem() {
        let mut migrations = get_migrations();
        let applied: Vec<AppliedMigration> = vec![
            migrations[0].clone().as_applied(),
            migrations[1].clone().as_applied(),
            migrations[2].clone().as_applied(),
        ];
        migrations.remove(1);
        let remaining = vec![migrations[2].clone()];
        let result = check_missing_divergent(applied, migrations, true, false).unwrap();
        assert_eq!(remaining, result);
    }
}
