use crate::error::WrapMigrationError;
use crate::traits::r#async::{AsyncQuery, AsyncTransaction};
use crate::{AsyncMigrate, Error, Migration};
use async_trait::async_trait;
use serde::Deserialize;
use surrealdb::Surreal;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

#[derive(Deserialize)]
struct MigrationEntry {
    version: i32,
    name: String,
    applied_on: String,
    checksum: String,
}

async fn query_applied_migrations<C: surrealdb::Connection>(
    db: &Surreal<C>,
    query: &str,
) -> Result<Vec<Migration>, surrealdb::Error> {
    let rows: Vec<MigrationEntry> = db.query(query).await?.take(0)?;
    let mut applied = Vec::new();
    for entry in rows.into_iter() {
        let version = entry.version;
        let applied_on: String = entry.applied_on;
        // Safe to call unwrap, as we stored it in RFC3339 format on the database
        let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339).unwrap();

        let checksum: String = entry.checksum;

        applied.push(Migration::applied(
            version,
            entry.name,
            applied_on,
            checksum
                .parse::<u64>()
                .expect("checksum must be a valid u64"),
        ));
    }
    Ok(applied)
}

#[async_trait]
impl<C: surrealdb::Connection> AsyncQuery<Vec<Migration>> for Surreal<C> {
    async fn query(&mut self, query: &str) -> Result<Vec<Migration>, Self::Error> {
        Ok(query_applied_migrations(self, query).await?)
    }
}

#[async_trait]
impl<C: surrealdb::Connection> AsyncTransaction for Surreal<C> {
    type Error = surrealdb::Error;
    async fn execute(&mut self, query: &[&str]) -> Result<usize, Self::Error> {
        let queries = query.join("\n");
        let result = Surreal::query(self, "BEGIN TRANSACTION")
            .query(queries)
            .query("COMMIT TRANSACTION")
            .await?;
        Ok(result.num_statements())
    }
}

const ASSERT_MIGRATIONS_TABLE_QUERY: &'static str = r##"
    DEFINE TABLE %MIGRATION_TABLE_NAME% SCHEMAFULL;
    DEFINE FIELD version ON TABLE %MIGRATION_TABLE_NAME% TYPE int;
    DEFINE FIELD name ON TABLE %MIGRATION_TABLE_NAME% TYPE string;
    DEFINE FIELD applied_on ON TABLE %MIGRATION_TABLE_NAME% TYPE string;
    DEFINE FIELD checksum ON TABLE %MIGRATION_TABLE_NAME% TYPE string;
"##;

const GET_APPLIED_MIGRATIONS_QUERY: &'static str = r##"
    SELECT version, name, applied_on, checksum
    FROM %MIGRATION_TABLE_NAME% ORDER BY version COLLATE ASC;
"##;

const GET_LAST_APPLIED_MIGRATION_QUERY: &'static str = r##"
    SELECT version, name, applied_on, checksum
    FROM %MIGRATION_TABLE_NAME% ORDER BY version COLLATE DESC LIMIT 1;
"##;

#[async_trait]
impl<C: surrealdb::Connection> AsyncMigrate for Surreal<C> {
    fn assert_migrations_table_query(migration_table_name: &str) -> String {
        ASSERT_MIGRATIONS_TABLE_QUERY.replace("%MIGRATION_TABLE_NAME%", migration_table_name)
    }

    async fn get_last_applied_migration(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Option<Migration>, Error> {
        let table_query = GET_LAST_APPLIED_MIGRATION_QUERY
            .replace("%MIGRATION_TABLE_NAME%", migration_table_name);

        let mut results = query_applied_migrations(self, table_query.as_str())
            .await
            .migration_err("failed to get applied migrations", None)?;

        Ok(results.pop())
    }

    async fn get_applied_migrations(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        let table_query =
            GET_APPLIED_MIGRATIONS_QUERY.replace("%MIGRATION_TABLE_NAME%", migration_table_name);

        Ok(query_applied_migrations(self, table_query.as_str())
            .await
            .migration_err("failed to get applied migrations", None)?)
    }
}
