pub mod r#async;
pub mod sync;

use crate::runner::Type;
use crate::{error::Kind, Error, Migration};

// Verifies applied and to be applied migrations returning Error if:
// - `abort_divergent` is true and there are applied migrations with a different name and checksum but same version as a migration to be applied.
// - `abort_missing` is true and there are applied migrations that are missing on the file system
// - there are repeated migrations with the same version to be applied
pub(crate) fn verify_migrations(
    applied: Vec<Migration>,
    mut migrations: Vec<Migration>,
    abort_divergent: bool,
    abort_missing: bool,
) -> Result<Vec<Migration>, Error> {
    migrations.sort();

    for app in applied.iter() {
        // iterate applied migrations on database and assert all migrations
        // applied on database exist on the file system and have the same checksum
        match migrations.iter().find(|m| m.version() == app.version()) {
            None => {
                if abort_missing {
                    return Err(Error::new(Kind::MissingVersion(app.clone()), None));
                } else {
                    log::error!("migration {} is missing from the filesystem", app);
                }
            }
            Some(migration) => {
                if migration != app {
                    if abort_divergent {
                        return Err(Error::new(
                            Kind::DivergentVersion(app.clone(), migration.clone()),
                            None,
                        ));
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

    let current: i32 = match applied.last() {
        Some(last) => {
            log::info!("current version: {}", last.version());
            last.version() as i32
        }
        None => {
            log::info!("schema history table is empty, going to apply all migrations");
            // use -1 as versions might start with 0
            -1
        }
    };

    let mut to_be_applied = Vec::new();
    // iterate all migration files found on file system and assert that there are not migrations missing:
    // migrations which its version is inferior to the current version on the database, yet were not applied.
    // select to be applied all migrations with version greater than current
    for migration in migrations.into_iter() {
        if applied
            .iter()
            .find(|app| app.version() == migration.version())
            .is_none()
        {
            if to_be_applied.contains(&migration) {
                return Err(Error::new(Kind::RepeatedVersion(migration), None));
            } else if migration.prefix() == &Type::Versioned
                && current >= migration.version() as i32
            {
                if abort_missing {
                    return Err(Error::new(Kind::MissingVersion(migration), None));
                } else {
                    log::error!("found migration on file system {} not applied", migration);
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

#[cfg(feature = "tiberius")]
pub(crate) const ASSERT_MIGRATIONS_TABLE_QUERY: &str =
    "IF NOT EXISTS(SELECT 1 FROM sys.Tables WHERE  Name = N'refinery_scgema_history')
    BEGIN
      CREATE TABLE refinery_schema_history(
             version INT PRIMARY KEY,
             name VARCHAR(255),
             applied_on VARCHAR(255),
             checksum VARCHAR(255));
    END";

#[cfg(not(feature = "tiberius"))]
pub(crate) const ASSERT_MIGRATIONS_TABLE_QUERY: &str =
    "CREATE TABLE IF NOT EXISTS refinery_schema_history(
             version INT4 PRIMARY KEY,
             name VARCHAR(255),
             applied_on VARCHAR(255),
             checksum VARCHAR(255));";

pub(crate) const GET_APPLIED_MIGRATIONS_QUERY: &str = "SELECT version, name, applied_on, checksum \
    FROM refinery_schema_history ORDER BY version ASC;";

pub(crate) const GET_LAST_APPLIED_MIGRATION_QUERY: &str =
    "SELECT version, name, applied_on, checksum
    FROM refinery_schema_history WHERE version=(SELECT MAX(version) from refinery_schema_history)";

#[cfg(test)]
mod tests {
    use super::{verify_migrations, Kind, Migration};

    fn get_migrations() -> Vec<Migration> {
        let migration1 = Migration::unapplied(
            "V1__initial.sql",
            include_str!("../../../refinery/tests/sql_migrations/V1-2/V1__initial.sql"),
        )
        .unwrap();

        let migration2 = Migration::unapplied(
            "V2__add_cars_and_motos_table.sql",
            include_str!(
                "../../../refinery/tests/sql_migrations/V1-2/V2__add_cars_and_motos_table.sql"
            ),
        )
        .unwrap();

        let migration3 = Migration::unapplied(
            "V3__add_brand_to_cars_table",
            include_str!(
                "../../../refinery/tests/sql_migrations/V3/V3__add_brand_to_cars_table.sql"
            ),
        )
        .unwrap();

        let migration4 = Migration::unapplied(
            "V4__add_year_field_to_cars",
            &"ALTER TABLE cars ADD year INTEGER;",
        )
        .unwrap();

        vec![migration1, migration2, migration3, migration4]
    }

    #[test]
    fn verify_migrations_returns_all_migrations_if_applied_are_empty() {
        let migrations = get_migrations();
        let applied: Vec<Migration> = Vec::new();
        let result = verify_migrations(applied, migrations.clone(), true, true).unwrap();
        assert_eq!(migrations, result);
    }

    #[test]
    fn verify_migrations_returns_unapplied() {
        let migrations = get_migrations();
        let applied: Vec<Migration> = vec![
            migrations[0].clone(),
            migrations[1].clone(),
            migrations[2].clone(),
        ];
        let remaining = vec![migrations[3].clone()];
        let result = verify_migrations(applied, migrations, true, true).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn verify_migrations_fails_on_divergent() {
        let migrations = get_migrations();
        let applied: Vec<Migration> = vec![
            migrations[0].clone(),
            migrations[1].clone(),
            Migration::unapplied(
                "V3__add_brand_to_cars_tableeee",
                include_str!(
                    "../../../refinery/tests/sql_migrations/V3/V3__add_brand_to_cars_table.sql"
                ),
            )
            .unwrap(),
        ];

        let migration = migrations[2].clone();
        let err = verify_migrations(applied, migrations, true, true).unwrap_err();
        match err.kind() {
            Kind::DivergentVersion(applied, divergent) => {
                assert_eq!(&migration, divergent);
                assert_eq!("add_brand_to_cars_tableeee", applied.name());
            }
            _ => panic!("failed test"),
        }
    }

    #[test]
    fn verify_migrations_doesnt_fail_on_divergent() {
        let migrations = get_migrations();
        let applied: Vec<Migration> = vec![
            migrations[0].clone(),
            migrations[1].clone(),
            Migration::unapplied(
                "V3__add_brand_to_cars_tableeee",
                include_str!(
                    "../../../refinery/tests/sql_migrations/V3/V3__add_brand_to_cars_table.sql"
                ),
            )
            .unwrap(),
        ];
        let remaining = vec![migrations[3].clone()];
        let result = verify_migrations(applied, migrations, false, true).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn verify_migrations_fails_on_missing_on_applied() {
        let migrations = get_migrations();
        let applied: Vec<Migration> = vec![migrations[0].clone(), migrations[2].clone()];
        let migration = migrations[1].clone();
        let err = verify_migrations(applied, migrations, true, true).unwrap_err();
        match err.kind() {
            Kind::MissingVersion(missing) => {
                assert_eq!(&migration, missing);
            }
            _ => panic!("failed test"),
        }
    }

    #[test]
    fn verify_migrations_fails_on_missing_on_filesystem() {
        let mut migrations = get_migrations();
        let applied: Vec<Migration> = vec![
            migrations[0].clone(),
            migrations[1].clone(),
            migrations[2].clone(),
        ];
        let migration = migrations.remove(1);
        let err = verify_migrations(applied, migrations, true, true).unwrap_err();
        match err.kind() {
            Kind::MissingVersion(missing) => {
                assert_eq!(&migration, missing);
            }
            _ => panic!("failed test"),
        }
    }

    #[test]
    fn verify_migrations_doesnt_fail_on_missing_on_applied() {
        let migrations = get_migrations();
        let applied: Vec<Migration> = vec![migrations[0].clone(), migrations[2].clone()];
        let remaining = vec![migrations[3].clone()];
        let result = verify_migrations(applied, migrations, true, false).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn verify_migrations_doesnt_fail_on_missing_on_filesystem() {
        let mut migrations = get_migrations();
        let applied: Vec<Migration> = vec![
            migrations[0].clone(),
            migrations[1].clone(),
            migrations[2].clone(),
        ];
        migrations.remove(1);
        let remaining = vec![migrations[2].clone()];
        let result = verify_migrations(applied, migrations, true, false).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn verify_migrations_checks_unversioned_out_of_order_doesnt_fail() {
        let mut migrations = get_migrations();
        migrations.push(
            Migration::unapplied(
                "U0__merge_out_of_order",
                include_str!(
                    "../../../refinery/tests/sql_migrations_unversioned/U0__merge_out_of_order.sql"
                ),
            )
            .unwrap(),
        );
        let applied: Vec<Migration> = vec![
            migrations[0].clone(),
            migrations[1].clone(),
            migrations[2].clone(),
            migrations[3].clone(),
        ];

        let remaining = vec![migrations[4].clone()];
        let result = verify_migrations(applied, migrations, true, true).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn verify_migrations_fails_on_repeated_migration() {
        let mut migrations = get_migrations();
        let repeated = migrations[0].clone();
        migrations.push(repeated);

        let err = verify_migrations(vec![], migrations, false, true).unwrap_err();
        match err.kind() {
            Kind::RepeatedVersion(repeated) => {
                assert_eq!(repeated, repeated);
            }
            _ => panic!("failed test"),
        }
    }
}
