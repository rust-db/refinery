use crate::traits::{check_missing_divergent, ASSERT_MIGRATIONS_TABLE, GET_APPLIED_MIGRATIONS};
use crate::{AppliedMigration, Error, Migration, WrapMigrationError};
use chrono::Local;

pub trait Transaction {
    type Error: std::error::Error + Send + Sync + 'static;

    fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error>;
}

pub trait Query<T>: Transaction {
    fn query(&mut self, query: &str) -> Result<Option<T>, Self::Error>;
}

fn migrate<T: Transaction>(transaction: &mut T, migrations: Vec<Migration>) -> Result<(), Error> {
    for migration in migrations.iter() {
        log::info!("applying migration: {}", migration);
        let update_query = &format!(
                "INSERT INTO refinery_schema_history (version, name, applied_on, checksum) VALUES ({}, '{}', '{}', '{}')",
                migration.version, migration.name, Local::now().to_rfc3339(), migration.checksum().to_string());
        transaction
            .execute(&[&migration.sql, update_query])
            .migration_err(&format!("error applying migration {}", migration))?;
    }
    Ok(())
}

fn migrate_grouped<T: Transaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
) -> Result<(), Error> {
    let mut grouped_migrations = Vec::new();
    let mut display_migrations = Vec::new();
    for migration in migrations.into_iter() {
        let query = format!(
            "INSERT INTO refinery_schema_history (version, name, applied_on, checksum) VALUES ({}, '{}', '{}', '{}')",
            migration.version, migration.name, Local::now().to_rfc3339(), migration.checksum().to_string()
        );
        display_migrations.push(migration.to_string());
        grouped_migrations.push(migration.sql);
        grouped_migrations.push(query);
    }
    log::info!(
        "going to apply batch migrations in single transaction: {:#?}",
        display_migrations
    );

    let refs: Vec<&str> = grouped_migrations.iter().map(AsRef::as_ref).collect();

    transaction
        .execute(refs.as_ref())
        .migration_err("error applying migrations")?;

    Ok(())
}

pub trait Migrate: Query<Vec<AppliedMigration>>
where
    Self: Sized,
{
    fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        grouped: bool,
    ) -> Result<(), Error> {
        self.execute(&[ASSERT_MIGRATIONS_TABLE])
            .migration_err("error asserting migrations table")?;

        let applied_migrations = self
            .query(GET_APPLIED_MIGRATIONS)
            .migration_err("error getting current schema version")?
            .unwrap_or_default();

        let migrations = check_missing_divergent(
            applied_migrations,
            migrations.to_vec(),
            abort_divergent,
            abort_missing,
        )?;

        if migrations.is_empty() {
            log::info!("no migrations to apply");
        }

        if grouped {
            migrate_grouped(self, migrations)
        } else {
            migrate(self, migrations)
        }
    }
}

impl<T: Query<Vec<AppliedMigration>>> Migrate for T {}

#[cfg(test)]
mod tests {
    use super::{check_missing_divergent, AppliedMigration, Error, Migration};

    fn get_migrations() -> Vec<Migration> {
        let migration1 = Migration::from_filename(
            "V1__initial.sql",
            include_str!("../../../refinery/tests/sql_migrations/V1__initial.sql"),
        )
        .unwrap();

        let migration2 = Migration::from_filename(
            "V2__add_cars_table",
            include_str!("../../../refinery/tests/sql_migrations/V2__add_cars_table.sql"),
        )
        .unwrap();

        let migration3 = Migration::from_filename(
            "V3__add_brand_to_cars_table",
            include_str!("../../../refinery/tests/sql_migrations/V3__add_brand_to_cars_table.sql"),
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
