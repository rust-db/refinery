use crate::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use crate::Migration;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use turso::{params, Connection, Error as TursoError};

async fn query_applied_migrations(
    conn: &Connection,
    query: &str,
) -> Result<Vec<Migration>, TursoError> {
    let mut rows = conn.query(query, params!()).await?;
    let mut applied = Vec::new();
    while let Some(row) = rows.next().await? {
        let version = row.get(0)?;
        let name: String = row.get(1)?;
        let applied_on: String = row.get(2)?;
        // Safe to call unwrap, as we stored it in RFC3339 format on the database.
        let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339).unwrap();
        let checksum: String = row.get(3)?;
        applied.push(Migration::applied(
            version,
            name,
            applied_on,
            checksum
                .parse::<u64>()
                .expect("checksum must be a valid u64"),
        ));
    }
    Ok(applied)
}

impl AsyncTransaction for Connection {
    type Error = TursoError;

    // Turso exposes no `Connection::transaction()` helper; drive the
    // transaction via plain SQL and use `execute_batch` so multi-statement
    // migrations (and comments with embedded `;`) are parsed by Turso's own
    // tokenizer instead of a hand-rolled splitter.
    async fn execute<'a, T: Iterator<Item = &'a str> + Send + 'a>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        Connection::execute(self, "BEGIN IMMEDIATE", params!()).await?;
        let mut count = 0;
        for query in queries {
            if let Err(err) = self.execute_batch(query).await {
                Connection::execute(self, "ROLLBACK", params!()).await?;
                return Err(err);
            }
            count += 1;
        }
        Connection::execute(self, "COMMIT", params!()).await?;
        Ok(count)
    }
}

impl AsyncQuery<Vec<Migration>> for Connection {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        query_applied_migrations(self, query).await
    }
}

impl AsyncMigrate for Connection {}
