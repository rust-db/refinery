use time::format_description::well_known::Rfc3339;

pub mod r#async;
pub mod sync;

use crate::{error::Kind, Error, Migration};

/// Verifies applied and returns to be applied migrations
///
/// Returns error if:
/// - `abort_divergent` is true and there are applied migrations with a different name and checksum but same version as a migration to be applied.
/// - `abort_missing_on_filesystem` is true and there are applied migrations that are missing on the file system
/// - `abort_missing_on_applied` is true and there are migrations to be applied that have a earlier version than the last applied migration
/// - there are repeated migrations with the same version to be applied
pub(crate) fn verify_migrations(
    applied: Vec<Migration>,
    mut migrations: Vec<Migration>, // FIXME: remove mut
    abort_divergent: bool,
    abort_missing_on_filesystem: bool,
    abort_missing_on_applied: bool,
) -> Result<Vec<Migration>, Error> {
    migrations.sort();

    for app in applied.iter() {
        // iterate applied migrations on database and assert all migrations
        // applied on database exist on the file system and have the same checksum
        match migrations.iter().find(|m| m.version() == app.version()) {
            None => {
                if abort_missing_on_filesystem {
                    return Err(Error::new(Kind::MissingVersion(app.clone()), None));
                } else {
                    log::error!(target: "refinery_core::traits::missing", "migration {} is missing from the filesystem", app);
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
                            target: "refinery_core::traits::divergent",
                            "applied migration {} is different than filesystem one {}",
                            app,
                            migration
                        );
                    }
                }
            }
        }
    }

    let current = match applied.last() {
        Some(last) => {
            log::info!("current version: {}", last.version());
            last.version() as i64
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
        if !applied
            .iter()
            .any(|app| app.version() == migration.version())
        {
            if to_be_applied.contains(&migration) {
                return Err(Error::new(Kind::RepeatedVersion(migration), None));
            } else if current >= migration.version() {
                if abort_missing_on_applied {
                    return Err(Error::new(Kind::MissingVersion(migration), None));
                } else {
                    log::warn!(target: "refinery_core::traits::missing", "found migration on file system {} not applied", migration);
                    to_be_applied.push(migration);
                }
            } else {
                to_be_applied.push(migration);
            }
        }
    }

    // with these two iterations we both assert that all migrations found on the database
    // exist on the file system and have the same checksum, and all migrations found
    // on the file system that are not database therefore going to be applied, unless
    // `abort_missing_on_applied` is true, in which case we return an error

    Ok(to_be_applied)
}

pub(crate) fn insert_migration_query(migration: &Migration, migration_table_name: &str) -> String {
    format!(
        "INSERT INTO {} (version, name, applied_on, checksum) VALUES ({}, '{}', '{}', '{}')",
        // safe to call unwrap as we just converted it to applied, and we are sure it can be formatted according to RFC 33339
        migration_table_name,
        migration.version(),
        migration.name(),
        migration.applied_on().unwrap().format(&Rfc3339).unwrap(),
        migration.checksum()
    )
}

pub(crate) const ASSERT_MIGRATIONS_TABLE_QUERY: &str =
    "CREATE TABLE IF NOT EXISTS %MIGRATION_TABLE_NAME%(
             version BIGINT PRIMARY KEY,
             name VARCHAR(255),
             applied_on VARCHAR(255),
             checksum VARCHAR(255));";

pub(crate) const GET_APPLIED_MIGRATIONS_QUERY: &str = "SELECT version, name, applied_on, checksum \
    FROM %MIGRATION_TABLE_NAME% ORDER BY version ASC;";

pub(crate) const GET_LAST_APPLIED_MIGRATION_QUERY: &str =
    "SELECT version, name, applied_on, checksum
    FROM %MIGRATION_TABLE_NAME% WHERE version=(SELECT MAX(version) from %MIGRATION_TABLE_NAME%)";

pub(crate) const DEFAULT_MIGRATION_TABLE_NAME: &str = "refinery_schema_history";

#[cfg(test)]
mod tests {
    use super::{verify_migrations, Kind, Migration};

    fn get_migrations() -> Vec<Migration> {
        let migration1 = Migration::unapplied(
            "20250501_000000_initial.sql",
            "CREATE TABLE persons (id int, name varchar(255), city varchar(255));",
            "DROP TABLE persons;",
        )
        .unwrap();

        let migration2 = Migration::unapplied(
            "20250502_000000_add_cars_and_motos_table.sql",
            include_str!(
                "../../../refinery/tests/migrations/20250502_000000_add_cars_table/up.sql"
            ),
            include_str!(
                "../../../refinery/tests/migrations/20250502_000000_add_cars_table/up.sql"
            ),
        )
        .unwrap();

        let migration3 = Migration::unapplied(
            "20250503_000000_add_brand_to_cars",
            "ALTER TABLE cars ADD brand varchar(255);",
            "ALTER TABLE cars DROP brand;",
        )
        .unwrap();

        let migration4 = Migration::unapplied(
            "20250504_000000_add_year_field_to_cars",
            "ALTER TABLE cars ADD year INTEGER;",
            "ALTER TABLE cars DROP year;",
        )
        .unwrap();

        vec![migration1, migration2, migration3, migration4]
    }

    #[test]
    fn verify_migrations_returns_all_migrations_if_applied_are_empty() {
        let migrations = get_migrations();
        let applied: Vec<Migration> = Vec::new();
        let result = verify_migrations(applied, migrations.clone(), true, true, true).unwrap();
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
        let result = verify_migrations(applied, migrations, true, true, true).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn verify_migrations_fails_on_divergent() {
        let migrations = get_migrations();
        let applied: Vec<Migration> = vec![
            migrations[0].clone(),
            migrations[1].clone(),
            Migration::unapplied(
                "20250503_000000_add_brand_to_cars_tableeee",
                "ALTER TABLE cars ADD brand varchar(255);",
                "ALTER TABLE cars DROP brand;",
            )
            .unwrap(),
        ];

        let migration = migrations[2].clone();
        let err = verify_migrations(applied, migrations, true, true, true).unwrap_err();
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
                "20250503_000000_add_brand_to_cars_tableeee",
                "ALTER TABLE cars ADD brand varchar(255);",
                "ALTER TABLE cars DROP brand;",
            )
            .unwrap(),
        ];
        let remaining = vec![migrations[3].clone()];
        let result = verify_migrations(applied, migrations, false, true, true).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn verify_migrations_fails_on_missing_on_applied() {
        let migrations = get_migrations();
        let applied: Vec<Migration> = vec![migrations[0].clone(), migrations[2].clone()];
        let migration = migrations[1].clone();
        let err = verify_migrations(applied, migrations, false, false, true).unwrap_err();
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
        let err = verify_migrations(applied, migrations, true, true, true).unwrap_err();
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
        let remaining = vec![migrations[1].clone(), migrations[3].clone()];
        let result = verify_migrations(applied, migrations, true, false, false).unwrap();
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
        let result = verify_migrations(applied, migrations, true, false, true).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn verify_migrations_fails_on_repeated_migration() {
        let mut migrations = get_migrations();
        let repeated = migrations[0].clone();
        migrations.push(repeated.clone());

        let err = verify_migrations(vec![], migrations, false, true, true).unwrap_err();
        match err.kind() {
            Kind::RepeatedVersion(m) => {
                assert_eq!(m, &repeated);
            }
            _ => panic!("failed test"),
        }
    }
}
