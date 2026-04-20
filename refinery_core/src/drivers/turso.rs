//! Async driver for [Turso](https://turso.tech/), an in-process SQLite
//! rewrite that ships its own parser and executor.
//!
//! Enable via the `turso` feature. Multi-statement migrations (and
//! migrations containing SQL comments with embedded `;`) are handed to
//! Turso's own tokenizer through `Connection::execute_batch`, so no
//! statement splitting happens in refinery.
//!
//! ```rust,no_run
//! use refinery_core::turso;
//!
//! mod embedded {
//!     use refinery::embed_migrations;
//!     embed_migrations!("./migrations");
//! }
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let db = turso::Builder::new_local("app.db").build().await?;
//! let mut conn = db.connect()?;
//! embedded::migrations::runner().run_async(&mut conn).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Concurrency
//!
//! `Builder::build` takes an exclusive file lock by default. If you need
//! to run migrations while another process holds an open connection to
//! the same database file, set `LIMBO_DISABLE_FILE_LOCK=1` in the
//! environment before building.

use crate::traits::r#async::{AsyncMigrate, AsyncQuery, AsyncTransaction};
use crate::Migration;
use async_trait::async_trait;
use std::fmt;
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;
use turso::{params, Connection, Error as TursoError};

/// Wrapper around [`turso::Error`] that satisfies
/// `AsyncTransaction::Error`'s `std::error::Error + Send + Sync + 'static` bound.
#[derive(Debug)]
pub struct Error {
    source: TursoError,
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "turso error: {}", self.source)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.source)
    }
}

impl From<TursoError> for Error {
    fn from(source: TursoError) -> Self {
        Error { source }
    }
}

async fn exec_simple(conn: &Connection, sql: &str) -> Result<(), TursoError> {
    conn.execute(sql, params!()).await.map(|_| ())
}

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

#[async_trait]
impl AsyncTransaction for Connection {
    type Error = Error;

    // Turso exposes no `Connection::transaction()` helper in the 0.6.0-pre.X
    // line; drive the transaction via plain SQL and use `execute_batch` so
    // multi-statement migrations (and comments with embedded `;`) are parsed
    // by Turso's own tokenizer instead of a hand-rolled splitter.
    async fn execute<'a, T: Iterator<Item = &'a str> + Send>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        exec_simple(self, "BEGIN IMMEDIATE").await?;
        let mut count = 0;
        for query in queries {
            if let Err(err) = self.execute_batch(query).await {
                let _ = exec_simple(self, "ROLLBACK").await;
                return Err(err.into());
            }
            count += 1;
        }
        exec_simple(self, "COMMIT").await?;
        Ok(count)
    }
}

#[async_trait]
impl AsyncQuery<Vec<Migration>> for Connection {
    async fn query(
        &mut self,
        query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        Ok(query_applied_migrations(self, query).await?)
    }
}

impl AsyncMigrate for Connection {}
