use crate::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use crate::util::SchemaVersion;
use crate::Migration;
use async_trait::async_trait;
use libsql::{Connection, Error as TursoError, Transaction as TursoTransaction};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

async fn query_applied_migrations(
    transaction: &TursoTransaction,
    query: &str,
) -> Result<Vec<Migration>, TursoError> {
    let mut rows = transaction.query(query, ()).await?;
    let mut applied = Vec::new();
    while let Some(row) = rows.next().await? {
        let version: SchemaVersion = row.get(0)?;
        let applied_on: String = row.get(2)?;
        // Safe to call unwrap, as we stored it in RFC3339 format on the database
        let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339).unwrap();

        let checksum: String = row.get(3)?;

        applied.push(Migration::applied(
            version,
            row.get(1)?,
            applied_on,
            checksum
                .parse::<u64>()
                .expect("checksum must be a valid u64"),
        ));
    }
    Ok(applied)
}

#[async_trait]
impl AsyncTransaction for Connection {
    type Error = TursoError;

    async fn execute<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let transaction = self.transaction().await?;
        let mut count = 0;
        for query in queries {
            transaction.execute_batch(query).await?;
            count += 1;
        }
        transaction.commit().await?;
        Ok(count)
    }
}

#[async_trait]
impl AsyncQuery<Vec<Migration>> for Connection {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        let transaction = self.transaction().await?;
        let applied = query_applied_migrations(&transaction, query).await?;
        transaction.commit().await?;
        Ok(applied)
    }
}

impl AsyncMigrate for Connection {}
