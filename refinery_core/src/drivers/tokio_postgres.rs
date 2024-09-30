use async_trait::async_trait;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio_postgres::error::Error as PgError;
use tokio_postgres::{Client, Transaction as PgTransaction};

use crate::executor::{AsyncExecutor, AsyncQuerySchemaHistory};
use crate::{AsyncMigrate, Migration, MigrationContent};

async fn query_applied_migrations(
    transaction: &PgTransaction<'_>,
    query: &str,
) -> Result<Vec<Migration>, PgError> {
    let rows = transaction.query(query, &[]).await?;
    let mut applied = Vec::new();
    for row in rows.into_iter() {
        let version = row.get(0);
        let applied_on: String = row.get(2);
        // Safe to call unwrap, as we stored it in RFC3339 format on the database
        let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339).unwrap();
        let checksum: String = row.get(3);

        applied.push(Migration::applied(
            version,
            row.get(1),
            applied_on,
            checksum
                .parse::<u64>()
                .expect("checksum must be a valid u64"),
        ));
    }
    Ok(applied)
}

#[async_trait]
impl AsyncExecutor for Client {
    type Error = PgError;

    async fn execute_grouped<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let transaction = self.transaction().await?;
        let mut count: usize = 0;
        for query in queries {
            transaction.batch_execute(query).await?;
            count += 1;
        }
        transaction.commit().await?;
        Ok(count)
    }

    async fn execute<'a, T>(&mut self, queries: T) -> Result<usize, Self::Error>
    where
        T: Iterator<Item = (&'a MigrationContent, &'a str)> + Send,
    {
        let mut count: usize = 0;
        for (content, update) in queries {
            if content.no_transaction() {
                self.batch_execute(content.sql()).await?;
                if let Err(e) = self.batch_execute(update).await {
                    log::error!("applied migration but schema history table update failed");
                    return Err(e);
                };
                count += 2;
            } else {
                let tx = self.transaction().await?;
                tx.batch_execute(content.sql()).await?;
                tx.batch_execute(update).await?;
                tx.commit().await?;
                count += 2;
            }
        }

        Ok(count)
    }
}

#[async_trait]
impl AsyncQuerySchemaHistory<Vec<Migration>> for Client {
    async fn query_schema_history(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncExecutor>::Error> {
        let transaction = self.transaction().await?;
        let applied = query_applied_migrations(&transaction, query).await?;
        transaction.commit().await?;
        Ok(applied)
    }
}

impl AsyncMigrate for Client {}
