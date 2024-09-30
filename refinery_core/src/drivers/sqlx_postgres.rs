use async_trait::async_trait;
use futures::prelude::*;
use sqlx::{Acquire, PgPool, Postgres};
use time::OffsetDateTime;

use crate::traits::r#async::{AsyncExecutor, AsyncMigrate, AsyncQuerySchemaHistory};
use crate::{Migration, MigrationContent};

/// A representation of a row in the schema
/// history table where migrations are persisted.
#[derive(Debug, Clone, sqlx::FromRow)]
struct SchemaHistory {
    version: i32,
    name: String,
    applied_on: Option<OffsetDateTime>,
    checksum: i64,
}

async fn query_applied_migrations(
    pool: &PgPool,
    query: &str,
) -> Result<Vec<Migration>, sqlx::Error> {
    sqlx::query_as::<Postgres, SchemaHistory>(query)
        .fetch(pool)
        .map_ok(|r| {
            Migration::applied(
                r.version,
                r.name,
                r.applied_on
                    .expect("applied migration missing `applied_on`"),
                u64::try_from(r.checksum).expect("checksum not u64"),
            )
        })
        .try_collect::<Vec<Migration>>()
        .await
}

#[async_trait]
impl AsyncExecutor for PgPool {
    type Error = sqlx::Error;

    async fn execute_grouped<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let mut tx = self.begin().await?;
        let mut count: usize = 0;
        for q in queries {
            let conn = tx.acquire().await?;
            sqlx::query(q).execute(&mut *conn).await?;
            count += 1;
        }
        tx.commit().await?;

        Ok(count)
    }

    async fn execute<'a, T>(&mut self, queries: T) -> Result<usize, Self::Error>
    where
        T: Iterator<Item = (&'a MigrationContent, &'a str)> + Send,
    {
        let mut count: usize = 0;
        for (content, update) in queries {
            if content.no_transaction() {
                let mut conn = self.acquire().await?;
                sqlx::query(content.sql()).execute(&mut *conn).await?;
                if let Err(e) = sqlx::query(update).execute(&mut *conn).await {
                    log::error!("applied migration but schema history table update failed");
                    return Err(e);
                };
                count += 2;
            } else {
                let mut tx = self.begin().await?;
                let conn = tx.acquire().await?;
                sqlx::query(content.sql()).execute(&mut *conn).await?;
                sqlx::query(update).execute(&mut *conn).await?;
                tx.commit().await?;
                count += 2;
            }
        }

        Ok(count)
    }
}

#[async_trait]
impl AsyncQuerySchemaHistory<Vec<Migration>> for PgPool {
    async fn query_schema_history(&mut self, query: &str) -> Result<Vec<Migration>, sqlx::Error> {
        let applied = query_applied_migrations(&self, query).await?;

        Ok(applied)
    }
}

impl AsyncMigrate for PgPool {}
