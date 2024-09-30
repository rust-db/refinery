use crate::traits::r#async::{AsyncExecutor, AsyncMigrate, AsyncQuerySchemaHistory};
use crate::{Migration, MigrationContent};
use async_trait::async_trait;
use mysql_async::{
    prelude::Queryable, Error as MError, IsolationLevel, Pool, Transaction as MTransaction, TxOpts,
};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

async fn query_applied_migrations<'a>(
    mut transaction: MTransaction<'a>,
    query: &str,
) -> Result<(MTransaction<'a>, Vec<Migration>), MError> {
    let result = transaction.query(query).await?;

    let applied = result
        .into_iter()
        .map(|row| {
            let (version, name, applied_on, checksum): (i32, String, String, String) =
                mysql_async::from_row(row);

            // Safe to call unwrap, as we stored it in RFC3339 format on the database
            let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339).unwrap();
            Migration::applied(
                version,
                name,
                applied_on,
                checksum
                    .parse::<u64>()
                    .expect("checksum must be a valid u64"),
            )
        })
        .collect();

    Ok((transaction, applied))
}

#[async_trait]
impl AsyncExecutor for Pool {
    type Error = MError;

    async fn execute_grouped<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let tx_opts = TxOpts::default()
            .with_isolation_level(Some(IsolationLevel::ReadCommitted))
            .clone();
        let mut tx = self.start_transaction(tx_opts).await?;
        let mut count: usize = 0;
        for query in queries {
            tx.query_drop(query).await?;
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
            let mut conn = self.get_conn().await?;
            if content.no_transaction() {
                conn.query_drop(content.sql()).await?;
                if let Err(e) = conn.query_drop(update).await {
                    log::error!("applied migration but schema history table update failed");
                    return Err(e);
                };
                count += 2;
            } else {
                let tx_opts = TxOpts::default()
                    .with_isolation_level(Some(IsolationLevel::ReadCommitted))
                    .clone();
                let mut tx = self.start_transaction(tx_opts).await?;
                tx.query_drop(content.sql()).await?;
                tx.query_drop(update).await?;
                tx.commit().await?;
                count += 2;
            }
        }

        Ok(count)
    }
}

#[async_trait]
impl AsyncQuerySchemaHistory<Vec<Migration>> for Pool {
    async fn query_schema_history(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncExecutor>::Error> {
        let tx_opts = TxOpts::default()
            .with_isolation_level(Some(IsolationLevel::ReadCommitted))
            .clone();
        let tx = self.start_transaction(tx_opts).await?;
        let (tx, applied) = query_applied_migrations(tx, query).await?;
        tx.commit().await?;

        Ok(applied)
    }
}

impl AsyncMigrate for Pool {}
