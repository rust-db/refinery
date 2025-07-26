use crate::traits::sync::{Migrate, Query, Transaction};
use crate::Migration;
use postgres::{Client as PgClient, Error as PgError, Transaction as PgTransaction};
use time::format_description::well_known::Rfc3339;
use time::OffsetDateTime;

fn query_applied_migrations(
    transaction: &mut PgTransaction,
    query: &str,
) -> Result<Vec<Migration>, PgError> {
    let rows = transaction.query(query, &[])?;
    let mut applied = Vec::new();
    for row in rows.into_iter() {
        let version = row.get(0);
        let applied_on: String = row.get(2);
        // Safe to call unwrap, as we stored it in RFC3339 format on the database
        let applied_on = OffsetDateTime::parse(&applied_on, &Rfc3339).unwrap();

        let checksum: String = row.get(3);

        applied.push(Migration::applied(
            version,
            row.get(1),
            applied_on,
            checksum
                .parse::<u64>()
                .expect("checksum must be a valid u64"),
        ));
    }
    Ok(applied)
}

impl Transaction for PgClient {
    type Error = PgError;

    fn execute<'a, T: Iterator<Item = &'a str>>(
        &mut self,
        queries: T,
    ) -> Result<usize, Self::Error> {
        let mut transaction = PgClient::transaction(self)?;
        let mut count = 0;
        for query in queries {
            PgTransaction::batch_execute(&mut transaction, query)?;
            count += 1;
        }
        transaction.commit()?;
        Ok(count as usize)
    }
}

impl Query<Vec<Migration>> for PgClient {
    fn query(&mut self, query: &str) -> Result<Vec<Migration>, Self::Error> {
        let mut transaction = PgClient::transaction(self)?;
        let applied = query_applied_migrations(&mut transaction, query)?;
        transaction.commit()?;
        Ok(applied)
    }
}

impl Migrate for PgClient {}
