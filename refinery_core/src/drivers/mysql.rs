use mysql::{
    error::Error as MError, prelude::Queryable, Conn, IsolationLevel, PooledConn,
    Transaction as MTransaction, TxOpts,
};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

use crate::executor::{Executor, QuerySchemaHistory};
use crate::{Migrate, Migration, MigrationContent};

fn get_tx_opts() -> TxOpts {
    TxOpts::default()
        .set_with_consistent_snapshot(true)
        .set_access_mode(None)
        .set_isolation_level(Some(IsolationLevel::RepeatableRead))
}

fn query_applied_migrations(
    transaction: &mut MTransaction,
    query: &str,
) -> Result<Vec<Migration>, MError> {
    let rows = transaction.query_iter(query)?;
    let mut applied = Vec::new();
    for row in rows {
        let row = row?;
        let version = row.get(0).unwrap();
        let applied_on: String = row.get(2).unwrap();
        // Safe to call unwrap, as we stored it in RFC3339 format on the database
        let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339).unwrap();
        let checksum: String = row.get(3).unwrap();

        applied.push(Migration::applied(
            version,
            row.get(1).unwrap(),
            applied_on,
            checksum
                .parse::<u64>()
                .expect("checksum must be a valid u64"),
        ))
    }
    Ok(applied)
}

impl Executor for Conn {
    type Error = MError;

    fn execute_grouped<'a, T: Iterator<Item = &'a str>>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let mut tx = self.start_transaction(get_tx_opts())?;
        let mut count: usize = 0;
        for query in queries {
            tx.query_iter(query)?;
            count += 1;
        }
        tx.commit()?;

        Ok(count)
    }

    fn execute<'a, T>(&mut self, queries: T) -> Result<usize, Self::Error>
    where
        T: Iterator<Item = (&'a MigrationContent, &'a str)>,
    {
        let mut count: usize = 0;
        for (content, update) in queries {
            if content.no_transaction() {
                self.query_iter(content.sql())?;
                if let Err(e) = self.query_iter(update) {
                    log::error!("applied migration but schema history table update failed");
                    return Err(e);
                }
                count += 2;
            } else {
                let mut tx = self.start_transaction(get_tx_opts())?;
                tx.query_iter(content.sql())?;
                tx.query_iter(update)?;
                tx.commit()?;
                count += 2;
            }
        }

        Ok(count)
    }
}

impl Executor for PooledConn {
    type Error = MError;

    fn execute_grouped<'a, T: Iterator<Item = &'a str>>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let mut tx = self.start_transaction(get_tx_opts())?;
        let mut count: usize = 0;
        for query in queries {
            tx.query_iter(query)?;
            count += 1;
        }
        tx.commit()?;

        Ok(count)
    }

    fn execute<'a, T>(&mut self, queries: T) -> Result<usize, Self::Error>
    where
        T: Iterator<Item = (&'a MigrationContent, &'a str)>,
    {
        let mut count: usize = 0;
        for (content, update) in queries {
            if content.no_transaction() {
                self.query_iter(content.sql())?;
                if let Err(e) = self.query_iter(update) {
                    log::error!("applied migration but schema history table update failed");
                    return Err(e);
                }
                count += 2;
            } else {
                let mut tx = self.start_transaction(get_tx_opts())?;
                tx.query_iter(content.sql())?;
                tx.query_iter(update)?;
                tx.commit()?;
                count += 2;
            }
        }

        Ok(count)
    }
}

impl QuerySchemaHistory<Vec<Migration>> for Conn {
    fn query_schema_history(&mut self, query: &str) -> Result<Vec<Migration>, Self::Error> {
        let mut transaction = self.start_transaction(get_tx_opts())?;
        let applied = query_applied_migrations(&mut transaction, query)?;
        transaction.commit()?;
        Ok(applied)
    }
}

impl QuerySchemaHistory<Vec<Migration>> for PooledConn {
    fn query_schema_history(&mut self, query: &str) -> Result<Vec<Migration>, Self::Error> {
        let mut transaction = self.start_transaction(get_tx_opts())?;
        let applied = query_applied_migrations(&mut transaction, query)?;
        transaction.commit()?;
        Ok(applied)
    }
}

impl Migrate for Conn {}
impl Migrate for PooledConn {}
