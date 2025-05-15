use crate::error::WrapMigrationError;
use crate::traits::{
    insert_migration_query, verify_migrations, ASSERT_MIGRATIONS_TABLE_QUERY,
    GET_APPLIED_MIGRATIONS_QUERY, GET_LAST_APPLIED_MIGRATION_QUERY,
};
use crate::{Error, Migration, Report, Target};

pub trait Transaction {
    type Error: std::error::Error + Send + Sync + 'static;

    fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error>;
}

pub trait Query<T>: Transaction {
    fn query(&mut self, query: &str) -> Result<T, Self::Error>;
}

pub fn migrate<T: Transaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
    target: Target,
    migration_table_name: &str,
    batched: bool,
) -> Result<Report, Error> {
    let mut migration_batch = Vec::new();
    let mut applied_migrations = Vec::new();

    for mut migration in migrations.into_iter() {
        if let Target::Version(input_target) | Target::FakeVersion(input_target) = target {
            if input_target < migration.version() {
                log::info!(
                    "stopping at migration: {}, due to user option",
                    input_target
                );
                break;
            }
        }

        migration.set_applied();
        let insert_migration = insert_migration_query(&migration, migration_table_name);
        let migration_sql = migration.sql().expect("sql must be Some!").to_string();

        // If Target is Fake, we only update schema migrations table
        if !matches!(target, Target::Fake | Target::FakeVersion(_)) {
            applied_migrations.push(migration);
            migration_batch.push(migration_sql);
        }
        migration_batch.push(insert_migration);
    }

    match (target, batched) {
        (Target::Fake | Target::FakeVersion(_), _) => {
            log::info!("not going to apply any migration as fake flag is enabled.");
        }
        (Target::Latest | Target::Version(_), true) => {
            log::info!(
                "going to batch apply {} migrations in single transaction.",
                applied_migrations.len()
            );
        }
        (Target::Latest | Target::Version(_), false) => {
            log::info!(
                "going to apply {} migrations in multiple transactions.",
                applied_migrations.len(),
            );
        }
    };

    let refs: Vec<&str> = migration_batch.iter().map(AsRef::as_ref).collect();

    if batched {
        let migrations_display = applied_migrations
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<String>>()
            .join("\n");
        log::info!("going to apply batch migrations in single transaction:\n{migrations_display}");
        transaction
            .execute(refs.as_ref())
            .migration_err("error applying migrations", None)?;
    } else {
        for (i, update) in refs.iter().enumerate() {
            let applying_migration = i % 2 == 0;

            let current_migration = &applied_migrations[i / 2];
            if applying_migration {
                log::info!("applying migration: {current_migration} ...");
            } else {
                //Writing the migration state to the db
                log::info!("applied migration:  {current_migration} writing state to db.");
            }
            transaction
                .execute(&[update])
                .migration_err("error applying update", Some(&applied_migrations[0..i / 2]))?;
        }
    }

    Ok(Report::new(applied_migrations))
}

pub trait Migrate: Query<Vec<Migration>>
where
    Self: Sized,
{
    // Needed cause some database vendors like Mssql have a non sql standard way of checking the migrations table
    fn assert_migrations_table_query(migration_table_name: &str) -> String {
        ASSERT_MIGRATIONS_TABLE_QUERY.replace("%MIGRATION_TABLE_NAME%", migration_table_name)
    }

    fn get_last_applied_migration_query(migration_table_name: &str) -> String {
        GET_LAST_APPLIED_MIGRATION_QUERY.replace("%MIGRATION_TABLE_NAME%", migration_table_name)
    }

    fn get_applied_migrations_query(migration_table_name: &str) -> String {
        GET_APPLIED_MIGRATIONS_QUERY.replace("%MIGRATION_TABLE_NAME%", migration_table_name)
    }

    fn assert_migrations_table(&mut self, migration_table_name: &str) -> Result<usize, Error> {
        // Needed cause some database vendors like Mssql have a non sql standard way of checking the migrations table,
        // thou on this case it's just to be consistent with the async trait `AsyncMigrate`
        self.execute(&[Self::assert_migrations_table_query(migration_table_name).as_str()])
            .migration_err("error asserting migrations table", None)
    }

    fn get_last_applied_migration(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Option<Migration>, Error> {
        let mut migrations = self
            .query(Self::get_last_applied_migration_query(migration_table_name).as_str())
            .migration_err("error getting last applied migration", None)?;

        Ok(migrations.pop())
    }

    fn get_applied_migrations(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        let migrations = self
            .query(Self::get_applied_migrations_query(migration_table_name).as_str())
            .migration_err("error getting applied migrations", None)?;

        Ok(migrations)
    }

    fn get_unapplied_migrations(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        self.assert_migrations_table(migration_table_name)?;

        let applied_migrations = self.get_applied_migrations(migration_table_name)?;

        let migrations = verify_migrations(
            applied_migrations,
            migrations.to_vec(),
            abort_divergent,
            abort_missing,
        )?;

        if migrations.is_empty() {
            log::info!("no migrations to apply");
        }

        Ok(migrations)
    }

    fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        grouped: bool,
        target: Target,
        migration_table_name: &str,
    ) -> Result<Report, Error> {
        let migrations = self.get_unapplied_migrations(
            migrations,
            abort_divergent,
            abort_missing,
            migration_table_name,
        )?;

        if grouped || matches!(target, Target::Fake | Target::FakeVersion(_)) {
            migrate(self, migrations, target, migration_table_name, true)
        } else {
            migrate(self, migrations, target, migration_table_name, false)
        }
    }
}
