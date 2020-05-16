#[cfg(any(
    feature = "mysql",
    feature = "postgres",
    feature = "tokio-postgres",
    feature = "mysql_async"
))]
use crate::config::build_db_url;
use crate::config::{Config, ConfigDbType};
use crate::error::WrapMigrationError;
use crate::traits::r#async::{AsyncQuery, AsyncTransaction};
use crate::traits::sync::{Query, Transaction};
use crate::traits::{GET_APPLIED_MIGRATIONS_QUERY, GET_LAST_APPLIED_MIGRATION_QUERY};
use crate::{Error, Migration, Report, Target};
use async_trait::async_trait;
use std::convert::Infallible;

// we impl all the dependent traits as noop's and then override the methods that call them on Migrate and AsyncMigrate
impl Transaction for Config {
    type Error = Infallible;

    fn execute(&mut self, _queries: &[&str]) -> Result<usize, Self::Error> {
        Ok(0)
    }
}

impl Query<Vec<Migration>> for Config {
    fn query(&mut self, _query: &str) -> Result<Vec<Migration>, Self::Error> {
        Ok(Vec::new())
    }
}

#[async_trait]
impl AsyncTransaction for Config {
    type Error = Infallible;

    async fn execute(&mut self, _queries: &[&str]) -> Result<usize, Self::Error> {
        Ok(0)
    }
}

#[async_trait]
impl AsyncQuery<Vec<Migration>> for Config {
    async fn query(
        &mut self,
        _query: &str,
    ) -> Result<Vec<Migration>, <Self as AsyncTransaction>::Error> {
        Ok(Vec::new())
    }
}
// this is written as macro so that we don't have to deal with type signatures
#[cfg(any(feature = "mysql", feature = "postgres", feature = "rusqlite"))]
macro_rules! with_connection {
    ($config:ident, $op: expr) => {
        match $config.db_type() {
            ConfigDbType::Mysql => {
                cfg_if::cfg_if! {
                    if #[cfg(feature = "mysql")] {
                        let url = build_db_url("mysql", &$config);
                        let conn = mysql::Conn::new(&url).migration_err("could not connect to database", None)?;
                        $op(conn)
                    } else {
                        panic!("tried to migrate from config for a mysql database, but feature mysql not enabled!");
                    }
                }
            }
            ConfigDbType::Sqlite => {
                cfg_if::cfg_if! {
                    if #[cfg(feature = "rusqlite")] {
                        //may have been checked earlier on config parsing, even if not let it fail with a Rusqlite db file not found error
                        let path = $config.db_path().map(|p| p.to_path_buf()).unwrap_or_default();
                        let conn = rusqlite::Connection::open_with_flags(path, rusqlite::OpenFlags::SQLITE_OPEN_READ_WRITE).migration_err("could not open database", None)?;
                        $op(conn)
                    } else {
                        panic!("tried to migrate from config for a sqlite database, but feature rusqlite not enabled!");
                    }
                }
            }
            ConfigDbType::Postgres => {
                cfg_if::cfg_if! {
                    if #[cfg(feature = "postgres")] {
                        let path = build_db_url("postgresql", &$config);
                        let conn = postgres::Client::connect(path.as_str(), postgres::NoTls).migration_err("could not connect to database", None)?;
                        $op(conn)
                    } else {
                        panic!("tried to migrate from config for a postgresql database, but feature postgres not enabled!");
                    }
                }
            }
        };
    }
}

#[cfg(any(feature = "tokio-postgres", feature = "mysql_async"))]
macro_rules! with_connection_async {
    ($config: ident, $op: expr) => {
        match $config.db_type() {
            ConfigDbType::Mysql => {
                cfg_if::cfg_if! {
                    if #[cfg(feature = "mysql_async")] {
                        let url = build_db_url("mysql", $config);
                        let pool = mysql_async::Pool::from_url(&url).migration_err("could not connect to the database", None)?;
                        $op(pool).await
                    } else {
                        panic!("tried to migrate async from config for a mysql database, but feature mysql_async not enabled!");
                    }
                }
            }
            ConfigDbType::Sqlite => {
                panic!("tried to migrate async from config for a sqlite database, but this feature is not implemented yet");
            }
            ConfigDbType::Postgres => {
                cfg_if::cfg_if! {
                    if #[cfg(all(feature = "tokio-postgres", feature = "tokio"))] {
                        let path = build_db_url("postgresql", $config);
                        let (client, connection ) = tokio_postgres::connect(path.as_str(), tokio_postgres::NoTls).await.migration_err("could not connect to database", None)?;
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                eprintln!("connection error: {}", e);
                            }
                        });
                        $op(client).await
                    } else {
                        panic!("tried to migrate async from config for a postgresql database, but either tokio or tokio-postgres was not enabled!");
                    }
                }
            }
        }
    }
}

// rewrite all the default methods as we overrode Transaction and Query
#[cfg(any(feature = "mysql", feature = "postgres", feature = "rusqlite"))]
impl crate::Migrate for Config {
    fn get_last_applied_migration(&mut self) -> Result<Option<Migration>, Error> {
        with_connection!(self, |mut conn| {
            let mut migrations: Vec<Migration> =
                Query::query(&mut conn, GET_LAST_APPLIED_MIGRATION_QUERY)
                    .migration_err("error getting last applied migration", None)?;

            Ok(migrations.pop())
        })
    }

    fn get_applied_migrations(&mut self) -> Result<Vec<Migration>, Error> {
        with_connection!(self, |mut conn| {
            let migrations: Vec<Migration> = Query::query(&mut conn, GET_APPLIED_MIGRATIONS_QUERY)
                .migration_err("error getting applied migrations", None)?;

            Ok(migrations)
        })
    }

    fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        grouped: bool,
        target: Target,
    ) -> Result<Report, Error> {
        with_connection!(self, |mut conn| {
            crate::Migrate::migrate(
                &mut conn,
                migrations,
                abort_divergent,
                abort_missing,
                grouped,
                target,
            )
        })
    }
}

#[cfg(any(feature = "mysql_async", feature = "tokio-postgres",))]
#[async_trait]
impl crate::AsyncMigrate for Config {
    async fn get_last_applied_migration(&mut self) -> Result<Option<Migration>, Error> {
        with_connection_async!(self, move |mut conn| async move {
            let mut migrations: Vec<Migration> =
                AsyncQuery::query(&mut conn, GET_LAST_APPLIED_MIGRATION_QUERY)
                    .await
                    .migration_err("error getting last applied migration", None)?;

            Ok(migrations.pop())
        })
    }

    async fn get_applied_migrations(&mut self) -> Result<Vec<Migration>, Error> {
        with_connection_async!(self, move |mut conn| async move {
            let migrations: Vec<Migration> =
                AsyncQuery::query(&mut conn, GET_APPLIED_MIGRATIONS_QUERY)
                    .await
                    .migration_err("error getting last applied migration", None)?;
            Ok(migrations)
        })
    }

    async fn migrate(
        &mut self,
        migrations: &[Migration],
        abort_divergent: bool,
        abort_missing: bool,
        grouped: bool,
        target: Target,
    ) -> Result<Report, Error> {
        with_connection_async!(self, move |mut conn| async move {
            crate::AsyncMigrate::migrate(
                &mut conn,
                migrations,
                abort_divergent,
                abort_missing,
                grouped,
                target,
            )
            .await
        })
    }
}
