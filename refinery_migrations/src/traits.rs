use crate::{AppliedMigration, Error, Migration, WrapMigrationError};
use chrono::Local;

pub trait Transaction {
    type Error: std::error::Error + Send + Sync + 'static;

    fn execute(&mut self, query: &str) -> Result<usize, Self::Error>;
}

pub trait ExecuteMultiple: Transaction {
    fn execute_multiple(&mut self, queries: &[&str]) -> Result<usize, Self::Error>;
}

pub trait CommitTransaction: Transaction
where
    Self: Sized,
{
    fn commit(self) -> Result<(), Self::Error>;
}

pub trait Query<T>: Transaction {
    fn query(&mut self, query: &str) -> Result<Option<T>, Self::Error>;
}

pub trait DefaultQueries: Transaction + Query<Vec<AppliedMigration>> {
    fn assert_schema_history_table(&mut self) -> Result<usize, Self::Error> {
        self.execute(
            "CREATE TABLE IF NOT EXISTS refinery_schema_history( \
             version INTEGER PRIMARY KEY,\
             name VARCHAR(255),\
             applied_on VARCHAR(255),
             checksum VARCHAR(255));",
        )
    }

    fn get_applied_migrations(&mut self) -> Result<Vec<AppliedMigration>, Self::Error> {
        let result = self.query(
            "SELECT version, name, applied_on, checksum FROM refinery_schema_history ORDER BY version ASC",
        )?;
        Ok(result.unwrap_or_default())
    }
}

//checks for missing migrations on filesystem or apllied migrations with a different name and checksum but same version
//if abort_divergent or abort_missing are true returns Err on those cases, else returns the list of migrations to be applied
fn check_missing_divergent(
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
        match migrations.iter().find(|m| m.version == app.version) {
            None => {
                if abort_missing {
                    return Err(Error::MissingVersion(app.clone()));
                } else {
                    log::error!("migration {} is missing from the filesystem", app);
                }
            }
            Some(migration) => {
                if &migration.to_applied() != app {
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
    for migration in migrations.into_iter() {
        if applied
            .iter()
            .find(|app| app.version == migration.version)
            .is_none()
        {
            if current.version >= migration.version {
                if abort_missing {
                    return Err(Error::MissingVersion(migration.to_applied()));
                } else {
                    log::error!("found migration on filsystem {} not applied", migration);
                }
            } else {
                to_be_applied.push(migration);
            }
        }
    }
    Ok(to_be_applied)
}

pub trait MigrateGrouped<'a> {
    type Transaction: DefaultQueries + CommitTransaction;

    fn migrate(
        &'a mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
    ) -> Result<(), Error> {
        let mut transaction = self.transaction()?;
        transaction
            .assert_schema_history_table()
            .migration_err("error asserting migrations table")?;

        let applied_migrations = transaction
            .get_applied_migrations()
            .migration_err("error getting current schema version")?;

        let migrations = check_missing_divergent(
            applied_migrations,
            migrations.to_vec(),
            abort_divergent,
            abort_missing,
        )?;
        if migrations.is_empty() {
            log::info!("no migrations to apply");
        }

        for migration in migrations.iter() {
            log::info!("applying migration: {}", migration.name);
            transaction
                .execute(&migration.sql)
                .migration_err(&format!("error applying migration {}", migration))?;

            transaction
                .execute(&format!(
                    "INSERT INTO refinery_schema_history (version, name, applied_on, checksum) VALUES ({}, '{}', '{}', '{}')",
                    migration.version, migration.name, Local::now().to_rfc3339(), migration.checksum().to_string()
                ))
                .migration_err(&format!("error updating schema history to migration: {}", migration))?;
        }

        transaction
            .commit()
            .migration_err("error committing transaction")?;

        Ok(())
    }

    fn transaction(&'a mut self) -> Result<Self::Transaction, Error>;
}

pub trait Migrate: DefaultQueries + ExecuteMultiple {
    fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
    ) -> Result<(), Error> {
        self.assert_schema_history_table()
            .migration_err("error asserting migrations table")?;

        let applied_migrations = self
            .get_applied_migrations()
            .migration_err("error getting current schema version")?;

        let migrations = check_missing_divergent(
            applied_migrations,
            migrations.to_vec(),
            abort_divergent,
            abort_missing,
        )?;
        if migrations.is_empty() {
            log::info!("no migrations to apply");
        }

        for migration in migrations.iter() {
            log::info!("applying migration: {}", migration);
            let update_query = &format!(
                "INSERT INTO refinery_schema_history (version, name, applied_on, checksum) VALUES ({}, '{}', '{}', '{}')",
                migration.version, migration.name, Local::now().to_rfc3339(), migration.checksum().to_string());
            self.execute_multiple(&[&migration.sql, update_query])
                .migration_err(&format!("error applying migration {}", migration))?;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{check_missing_divergent, Error, AppliedMigration, Migration};

    fn get_migrations() -> Vec<Migration> {
        let migration1 = Migration::from_filename(
            "V1__initial.sql",
            include_str!("../../refinery/tests/sql_migrations/V1__initial.sql"),
        )
        .unwrap();

        let migration2 = Migration::from_filename(
            "V2__add_cars_table",
            include_str!("../../refinery/tests/sql_migrations/V2__add_cars_table.sql"),
        )
        .unwrap();

        let migration3 = Migration::from_filename(
            "V3__add_brand_to_cars_table",
            include_str!("../../refinery/tests/sql_migrations/V3__add_brand_to_cars_table.sql"),
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
            migrations[0].clone().to_applied(),
            migrations[1].clone().to_applied(),
            migrations[2].clone().to_applied(),
        ];
        let remaining = vec![migrations[3].clone()];
        let result = check_missing_divergent(applied, migrations, true, true).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn check_missing_divergent_fails_on_divergent() {
        let migrations = get_migrations();
        let mut applied: Vec<AppliedMigration> = vec![
            migrations[0].clone().to_applied(),
            migrations[1].clone().to_applied(),
            migrations[2].clone().to_applied(),
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
            migrations[0].clone().to_applied(),
            migrations[1].clone().to_applied(),
            migrations[2].clone().to_applied(),
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
            migrations[0].clone().to_applied(),
            migrations[2].clone().to_applied(),
        ];
        let migration = migrations[1].clone();
        let err = check_missing_divergent(applied, migrations, true, true).unwrap_err();
        match err {
            Error::MissingVersion(missing) => {
                assert_eq!(migration.to_applied(), missing);
            }
            _ => panic!("failed test"),
        }

    }

    #[test]
    fn check_missing_divergent_fails_on_missing_on_filesystem() {
        let mut migrations = get_migrations();
        let applied: Vec<AppliedMigration> = vec![
            migrations[0].clone().to_applied(),
            migrations[1].clone().to_applied(),
            migrations[2].clone().to_applied(),
        ];
        let migration = migrations.remove(1);
        let err = check_missing_divergent(applied, migrations, true, true).unwrap_err();
        match err {
            Error::MissingVersion(missing) => {
                assert_eq!(migration.to_applied(), missing);
            }
            _ => panic!("failed test"),
        }

    }

    #[test]
    fn check_missing_divergent_doesnt_fail_on_missing_on_applied() {
        let migrations = get_migrations();
        let applied: Vec<AppliedMigration> = vec![
            migrations[0].clone().to_applied(),
            migrations[2].clone().to_applied(),
        ];
        let remaining = vec![migrations[3].clone()];
        let result = check_missing_divergent(applied, migrations, true, false).unwrap();
        assert_eq!(remaining, result);
    }

    #[test]
    fn check_missing_divergent_doesnt_fail_on_missing_on_filesystem() {
        let mut migrations = get_migrations();
        let applied: Vec<AppliedMigration> = vec![
            migrations[0].clone().to_applied(),
            migrations[1].clone().to_applied(),
            migrations[2].clone().to_applied(),
        ];
        migrations.remove(1);
        let remaining = vec![migrations[2].clone()];
        let result = check_missing_divergent(applied, migrations, true, false).unwrap();
        assert_eq!(remaining, result);
    }
}
