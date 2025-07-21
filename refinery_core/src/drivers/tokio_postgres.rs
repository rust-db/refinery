use crate::traits::r#async::{AsyncExecutor, AsyncMigrate, AsyncQuery};
use crate::{Migration, MigrationFlags};
use async_trait::async_trait;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use tokio_postgres::error::Error as PgError;
use tokio_postgres::{Client, Transaction as PgTransaction};

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

    async fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let transaction = self.transaction().await?;
        let mut count = 0;
        for query in queries {
            transaction.batch_execute(query).await?;
            count += 1;
        }
        transaction.commit().await?;
        Ok(count as usize)
    }

    async fn execute_single(
        &mut self,
        query: &str,
        update_query: &str,
        flags: &MigrationFlags,
    ) -> Result<usize, Self::Error> {
        if flags.run_in_transaction {
            AsyncExecutor::execute(self, &[query, update_query]).await
        } else {
            self.simple_query(query).await?;
            if let Err(e) = self.simple_query(update_query).await {
                log::error!("applied migration but schema history table update failed");
                return Err(e);
            }
            Ok(2)
        }
    }
}

#[async_trait]
impl AsyncQuery<Vec<Migration>> for Client {
    async fn query(
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
