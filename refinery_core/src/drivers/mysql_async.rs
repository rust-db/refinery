use crate::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use crate::Migration;
use async_trait::async_trait;
use chrono::{DateTime, Local};
use mysql_async::{
    prelude::Queryable, Error as MError, IsolationLevel, Pool, Transaction as MTransaction, TxOpts,
};

async fn query_applied_migrations<'a>(
    mut transaction: MTransaction<'a>,
    query: &str,
) -> Result<(MTransaction<'a>, Vec<Migration>), MError> {
    let result = transaction.query(query).await?;

    let applied = result
        .into_iter()
        .map(|row| {
            let (version, name, applied_on, checksum): (i64, String, String, String) =
                mysql_async::from_row(row);

            let applied_on = DateTime::parse_from_rfc3339(&applied_on)
                .unwrap()
                .with_timezone(&Local);
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
impl AsyncTransaction for Pool {
    type Error = MError;

    async fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let mut conn = self.get_conn().await?;
        let mut options = TxOpts::new();
        options.with_isolation_level(Some(IsolationLevel::ReadCommitted));

        let mut transaction = conn.start_transaction(options).await?;
        let mut count = 0;
        for query in queries {
            transaction.query_drop(query).await?;
            count += 1;
        }
        transaction.commit().await?;
        Ok(count as usize)
    }
}

#[async_trait]
impl AsyncQuery<Vec<Migration>> for Pool {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        let mut conn = self.get_conn().await?;
        let mut options = TxOpts::new();
        options.with_isolation_level(Some(IsolationLevel::ReadCommitted));
        let transaction = conn.start_transaction(options).await?;

        let (transaction, applied) = query_applied_migrations(transaction, query).await?;
        transaction.commit().await?;
        Ok(applied)
    }
}

impl AsyncMigrate for Pool {}
