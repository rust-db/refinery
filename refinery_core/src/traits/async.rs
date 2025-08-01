use crate::error::WrapMigrationError;
use crate::traits::{
    insert_migration_query, verify_migrations, GET_APPLIED_MIGRATIONS_QUERY,
    GET_LAST_APPLIED_MIGRATION_QUERY,
};
use crate::{Error, Migration, Report, Target};

use async_trait::async_trait;
use std::string::ToString;

#[async_trait]
pub trait AsyncTransaction {
    type Error: std::error::Error + Send + Sync + 'static;

    async fn execute<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error>;
}

#[async_trait]
pub trait AsyncQuery<T>: AsyncTransaction {
    async fn query(&mut self, query: &str) -> Result<T, Self::Error>;
}

async fn migrate<T: AsyncTransaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
    target: Target,
    migration_table_name: &str,
) -> Result<Report, Error> {
    let mut applied_migrations = vec![];

    for mut migration in migrations.into_iter() {
        if let Target::Version(input_target) = target {
            if input_target < migration.version() {
                log::info!(
                    "stopping at migration: {}, due to user option",
                    input_target
                );
                break;
            }
        }

        log::info!("applying migration: {}", migration);
        migration.set_applied();
        let update_query = insert_migration_query(&migration, migration_table_name);
        transaction
            .execute(
                [
                    migration.sql().as_ref().expect("sql must be Some!"),
                    update_query.as_str(),
                ]
                .into_iter(),
            )
            .await
            .migration_err(
                &format!("error applying migration {migration}"),
                Some(&applied_migrations),
            )?;
        applied_migrations.push(migration);
    }
    Ok(Report::new(applied_migrations))
}

async fn migrate_grouped<T: AsyncTransaction>(
    transaction: &mut T,
    migrations: Vec<Migration>,
    target: Target,
    migration_table_name: &str,
) -> Result<Report, Error> {
    let mut grouped_migrations = Vec::new();
    let mut applied_migrations = Vec::new();

    for mut migration in migrations.into_iter() {
        if let Target::Version(input_target) | Target::FakeVersion(input_target) = target {
            if input_target < migration.version() {
                break;
            }
        }

        migration.set_applied();
        let query = insert_migration_query(&migration, migration_table_name);

        let sql = migration.sql().expect("sql must be Some!").to_string();

        // If Target is Fake, we only update schema migrations table
        if !matches!(target, Target::Fake | Target::FakeVersion(_)) {
            applied_migrations.push(migration);
            grouped_migrations.push(sql);
        }
        grouped_migrations.push(query);
    }

    match target {
        Target::Fake | Target::FakeVersion(_) => {
            log::info!("not going to apply any migration as fake flag is enabled");
        }
        Target::Latest | Target::Version(_) => {
            let migrations_display = applied_migrations
                .iter()
                .map(ToString::to_string)
                .collect::<Vec<String>>()
                .join("\n");
            log::info!(
                "going to apply batch migrations in single transaction:\n{migrations_display}"
            );
        }
    };

    if let Target::Version(input_target) = target {
        log::info!(
            "stopping at migration: {}, due to user option",
            input_target
        );
    }

    transaction
        .execute(grouped_migrations.iter().map(AsRef::as_ref))
        .await
        .migration_err("error applying migrations", None)?;

    Ok(Report::new(applied_migrations))
}

#[async_trait]
pub trait AsyncMigrate: AsyncQuery<Vec<Migration>>
where
    Self: Sized,
{
    // Needed cause some database vendors like Mssql have a non sql standard way of checking the migrations table
    fn assert_migrations_table_query(migration_table_name: &str) -> String {
        super::assert_migrations_table_query(migration_table_name)
    }

    fn get_last_applied_migration_query(migration_table_name: &str) -> String {
        GET_LAST_APPLIED_MIGRATION_QUERY.replace("%MIGRATION_TABLE_NAME%", migration_table_name)
    }

    fn get_applied_migrations_query(migration_table_name: &str) -> String {
        GET_APPLIED_MIGRATIONS_QUERY.replace("%MIGRATION_TABLE_NAME%", migration_table_name)
    }

    async fn get_last_applied_migration(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Option<Migration>, Error> {
        let mut migrations = self
            .query(Self::get_last_applied_migration_query(migration_table_name).as_ref())
            .await
            .migration_err("error getting last applied migration", None)?;

        Ok(migrations.pop())
    }

    async fn get_applied_migrations(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        let migrations = self
            .query(Self::get_applied_migrations_query(migration_table_name).as_ref())
            .await
            .migration_err("error getting applied migrations", None)?;

        Ok(migrations)
    }

    async fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        grouped: bool,
        target: Target,
        migration_table_name: &str,
    ) -> Result<Report, Error> {
        self.execute(
            [Self::assert_migrations_table_query(migration_table_name).as_ref()].into_iter(),
        )
        .await
        .migration_err("error asserting migrations table", None)?;

        let applied_migrations = self
            .get_applied_migrations(migration_table_name)
            .await
            .migration_err("error getting current schema version", None)?;

        let migrations = verify_migrations(
            applied_migrations,
            migrations.to_vec(),
            abort_divergent,
            abort_missing,
        )?;

        if migrations.is_empty() {
            log::info!("no migrations to apply");
        }

        if grouped || matches!(target, Target::Fake | Target::FakeVersion(_)) {
            migrate_grouped(self, migrations, target, migration_table_name).await
        } else {
            migrate(self, migrations, target, migration_table_name).await
        }
    }
}
