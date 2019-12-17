use crate::{AppliedMigration, Query, Transaction};
use chrono::{DateTime, Local};
use mysql::{
    error::Error as MError, params::Params, Conn, IsolationLevel, PooledConn,
    Transaction as MTransaction,
};

fn query_applied_migrations(
    transaction: &mut MTransaction,
    query: &str,
) -> Result<Vec<AppliedMigration>, MError> {
    let rows = transaction.query(query)?;
    let mut applied = Vec::new();
    for row in rows {
        let row = row?;
        let version: i64 = row.get(0).unwrap();
        let applied_on: String = row.get(2).unwrap();
        let applied_on = DateTime::parse_from_rfc3339(&applied_on)
            .unwrap()
            .with_timezone(&Local);
        applied.push(AppliedMigration {
            version: version as usize,
            name: row.get(1).unwrap(),
            applied_on,
            checksum: row.get(3).unwrap(),
        })
    }
    Ok(applied)
}

impl Transaction for Conn {
    type Error = MError;

    fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let mut transaction =
            self.start_transaction(true, Some(IsolationLevel::RepeatableRead), None)?;
        let mut count = 0;
        for query in queries.iter() {
            count += transaction.first_exec(query, Params::Empty)?.unwrap_or(0);
        }
        transaction.commit()?;
        Ok(count as usize)
    }
}

impl Transaction for PooledConn {
    type Error = MError;

    fn execute(&mut self, queries: &[&str]) -> Result<usize, Self::Error> {
        let mut transaction =
            self.start_transaction(true, Some(IsolationLevel::RepeatableRead), None)?;
        let mut count = 0;

        for query in queries.iter() {
            let result = transaction.first_exec(query, Params::Empty);
            if result.is_err() {
                transaction.rollback()?;
                return result.map(|_| 0);
            }
            count += result?.unwrap_or(0);
        }
        transaction.commit()?;
        Ok(count as usize)
    }
}

impl Query<Vec<AppliedMigration>> for Conn {
    fn query(&mut self, query: &str) -> Result<Option<Vec<AppliedMigration>>, Self::Error> {
        let mut transaction =
            self.start_transaction(true, Some(IsolationLevel::RepeatableRead), None)?;
        let applied = query_applied_migrations(&mut transaction, query)?;
        transaction.commit()?;
        Ok(Some(applied))
    }
}

impl Query<Vec<AppliedMigration>> for PooledConn {
    fn query(&mut self, query: &str) -> Result<Option<Vec<AppliedMigration>>, Self::Error> {
        let mut transaction =
            self.start_transaction(true, Some(IsolationLevel::RepeatableRead), None)?;
        let applied = query_applied_migrations(&mut transaction, query)?;
        transaction.commit()?;
        Ok(Some(applied))
    }
}
