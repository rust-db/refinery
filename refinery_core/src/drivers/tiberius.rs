use async_trait::async_trait;
use futures::{
    io::{AsyncRead, AsyncWrite},
    TryStreamExt,
};
use tiberius::{error::Error, Client, QueryItem};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::executor::{AsyncExecutor, AsyncQuerySchemaHistory};
use crate::{AsyncMigrate, Migration, MigrationContent};

async fn query_applied_migrations<S: AsyncRead + AsyncWrite + Unpin + Send>(
    client: &mut Client<S>,
    query: &str,
) -> Result<Vec<Migration>, Error> {
    let mut rows = client.simple_query(query).await?;
    let mut applied = Vec::new();
    // Unfortunately too many unwraps as `Row::get` maps to Option<T> instead of T
    while let Some(item) = rows.try_next().await? {
        if let QueryItem::Row(row) = item {
            let version = row.get::<i32, usize>(0).unwrap();
            let applied_on: &str = row.get::<&str, usize>(2).unwrap();
            // Safe to call unwrap, as we stored it in RFC3339 format on the database
            let applied_on = OffsetDateTime::parse(applied_on, &Rfc3339).unwrap();
            let checksum: String = row.get::<&str, usize>(3).unwrap().to_string();

            applied.push(Migration::applied(
                version,
                row.get::<&str, usize>(1).unwrap().to_string(),
                applied_on,
                checksum
                    .parse::<u64>()
                    .expect("checksum must be a valid u64"),
            ));
        }
    }

    Ok(applied)
}

#[async_trait]
impl<S> AsyncExecutor for Client<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    type Error = Error;

    async fn execute_grouped<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        // Tiberius doesn't support transactions, see https://github.com/prisma/tiberius/issues/28
        self.simple_query("BEGIN TRAN T1;").await?;
        let mut count: usize = 0;
        for query in queries {
            // Drop the returning `QueryStream<'a>` to avoid compiler complaning regarding lifetimes
            if let Err(err) = self.simple_query(query).await.map(drop) {
                if let Err(err) = self.simple_query("ROLLBACK TRAN T1").await {
                    log::error!("could not ROLLBACK transaction, {}", err);
                }
                return Err(err);
            }
            count += 1;
        }
        self.simple_query("COMMIT TRAN T1").await?;

        Ok(count)
    }

    async fn execute<'a, T>(&mut self, queries: T) -> Result<usize, Self::Error>
    where
        T: Iterator<Item = (&'a MigrationContent, &'a str)> + Send,
    {
        let mut count: usize = 0;
        for (content, update) in queries {
            if content.no_transaction() {
                self.simple_query(content.sql()).await?;
                if let Err(e) = self.simple_query(update).await {
                    log::error!("applied migration but schema history table update failed");
                    return Err(e);
                }
                count += 2;
            } else {
                self.simple_query("BEGIN TRAN T1;").await?;
                self.simple_query(content.sql()).await?;
                self.simple_query(update).await?;
                self.simple_query("COMMIT TRAN T1;").await?;
                count += 2;
            }
        }

        Ok(count)
    }
}

#[async_trait]
impl<S> AsyncQuerySchemaHistory<Vec<Migration>> for Client<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    async fn query_schema_history(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncExecutor>::Error> {
        let applied = query_applied_migrations(self, query).await?;
        Ok(applied)
    }
}

impl<S> AsyncMigrate for Client<S>
where
    S: AsyncRead + AsyncWrite + Unpin + Send,
{
    fn assert_migrations_table_query(migration_table_name: &str) -> String {
        format!(
            "IF NOT EXISTS(SELECT 1 FROM sys.Tables WHERE  Name = N'{table_name}')
         BEGIN
           CREATE TABLE {table_name}(
             version INT PRIMARY KEY,
             name VARCHAR(255),
             applied_on VARCHAR(255),
             checksum VARCHAR(255));
         END",
            table_name = migration_table_name
        )
    }
}
