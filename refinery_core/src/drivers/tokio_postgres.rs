use crate::traits::r#async::{AsyncQuery, AsyncTransaction};
use crate::AppliedMigration;
use async_trait::async_trait;
use chrono::{DateTime, Local};
use tokio_postgres::error::Error as PgError;
use tokio_postgres::{Client, Transaction as PgTransaction};

async fn query_applied_migrations(
    transaction: &PgTransaction<'_>,
    query: &str,
) -> Result<Vec<AppliedMigration>, PgError> {
    let rows = transaction.query(query, &[]).await?;
    let mut applied = Vec::new();
    for row in rows.into_iter() {
        let version = row.get(0);
        let applied_on: String = row.get(2);
        let applied_on = DateTime::parse_from_rfc3339(&applied_on)
            .unwrap()
            .with_timezone(&Local);

        applied.push(AppliedMigration {
            version,
            name: row.get(1),
            applied_on,
            checksum: row.get(3),
        });
    }
    Ok(applied)
}

#[async_trait]
impl AsyncTransaction for Client {
    type Error = PgError;

    async fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let transaction = self.transaction().await?;
        let mut count = 0;
        for query in queries {
            transaction.batch_execute(*query).await?;
            count += 1;
        }
        transaction.commit().await?;
        Ok(count as usize)
    }
}

#[async_trait]
impl AsyncQuery<Vec<AppliedMigration>> for Client {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Option<Vec<AppliedMigration>>, <Self as AsyncTransaction>::Error> {
        let transaction = self.transaction().await?;
        let applied = query_applied_migrations(&transaction, query).await?;
        transaction.commit().await?;
        Ok(Some(applied))
    }
}
