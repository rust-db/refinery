use crate::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use crate::Migration;
use async_trait::async_trait;
use chrono::{DateTime, Local};
use mysql_async::{
    error::Error as MError, prelude::Queryable, Conn, IsolationLevel, Pool,
    Transaction as MTransaction, TransactionOptions,
};

async fn query_applied_migrations(
    transaction: MTransaction<Conn>,
    query: &str,
) -> Result<(MTransaction<Conn>, Vec<Migration>), MError> {
    let result = transaction.query(query).await?;

    let (transaction, applied) = result
        .map_and_drop(|row| {
            let (version, name, applied_on, checksum): (i32, String, String, String) =
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
        .await?;

    Ok((transaction, applied))
}

#[async_trait]
impl AsyncTransaction for Pool {
    type Error = MError;

    async fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let conn = self.get_conn().await?;
        let mut options = TransactionOptions::new();
        options.set_isolation_level(Some(IsolationLevel::ReadCommitted));

        let mut transaction = conn.start_transaction(options).await?;
        let mut count = 0;
        for query in queries {
            transaction = transaction.query(query).await?.drop_result().await?;
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
        let conn = self.get_conn().await?;
        let mut options = TransactionOptions::new();
        options.set_isolation_level(Some(IsolationLevel::ReadCommitted));
        let transaction = conn.start_transaction(options).await?;

        let (transaction, applied) = query_applied_migrations(transaction, query).await?;
        transaction.commit().await?;
        Ok(applied)
    }
}

impl AsyncMigrate for Pool {}
