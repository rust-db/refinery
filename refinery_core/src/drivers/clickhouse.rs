use async_trait::async_trait;
use crate::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use crate::Migration;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use klickhouse::{Client, KlickhouseError, Result, Row, query_parser};

#[derive(Row)]
struct MigrationRow {
    version: i32,
    name: String,
    applied_on: String,
    checksum: u64,
}

#[async_trait]
impl AsyncTransaction for Client {
    type Error = KlickhouseError;

    async fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        for query in queries {
            for query in query_parser::split_query_statements(query).into_iter().filter(|x| !x.trim().is_empty()) {
                Client::execute(self, query).await?;
            }
        }
        Ok(queries.len())
    }
}

#[async_trait]
impl AsyncQuery<Vec<Migration>> for Client {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        assert!(!query.is_empty());
        self.query_collect::<MigrationRow>(query).await?
            .into_iter()
            .map(|row| Ok(Migration::applied(row.version, row.name, OffsetDateTime::parse(&*row.applied_on, &Rfc3339).map_err(|e| {
                KlickhouseError::DeserializeError(format!("failed to parse time: {:?}", e))
            })?, row.checksum)))
            .collect()
    }
}

impl AsyncMigrate for Client {
    fn assert_migrations_table_query(migration_table_name: &str) -> String {
        format!(
            "CREATE TABLE IF NOT EXISTS {migration_table_name}(
            version INT,
            name VARCHAR(255),
            applied_on VARCHAR(255),
            checksum UInt64) Engine=MergeTree() ORDER BY version;"
        )
    }
}
