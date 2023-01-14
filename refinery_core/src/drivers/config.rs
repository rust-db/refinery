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
                        let opts = mysql::Opts::from_url(&url).migration_err("could not parse url", None)?;
                        let conn = mysql::Conn::new(opts).migration_err("could not connect to database", None)?;
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

                        let conn;
                        if $config.use_tls().is_some() && $config.use_tls().unwrap() {
                            let builder = openssl::ssl::SslConnector::builder(openssl::ssl::SslMethod::tls()).unwrap();
                            let connector = postgres_openssl::MakeTlsConnector::new(builder.build());
                            conn = postgres::Client::connect(path.as_str(), connector).migration_err("could not connect to database", None)?;
                        } else {
                            conn = postgres::Client::connect(path.as_str(), postgres::NoTls).migration_err("could not connect to database", None)?;
                        }

                        $op(conn)
                    } else {
                        panic!("tried to migrate from config for a postgresql database, but feature postgres not enabled!");
                    }
                }
            }
            ConfigDbType::Mssql => {
                panic!("tried to synchronously migrate from config for a mssql database, but tiberius is an async driver");
            }
        }
    }
}

#[cfg(any(
    feature = "tokio-postgres",
    feature = "mysql_async",
    feature = "tiberius-config"
))]
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
                    if #[cfg(feature = "tokio-postgres")] {
                        let path = build_db_url("postgresql", $config);
                        let (client, connection ) = tokio_postgres::connect(path.as_str(), tokio_postgres::NoTls).await.migration_err("could not connect to database", None)?;
                        tokio::spawn(async move {
                            if let Err(e) = connection.await {
                                eprintln!("connection error: {}", e);
                            }
                        });
                        $op(client).await
                    } else {
                        panic!("tried to migrate async from config for a postgresql database, but tokio-postgres was not enabled!");
                    }
                }
            }
            ConfigDbType::Mssql => {
                cfg_if::cfg_if! {
                    if #[cfg(feature = "tiberius-config")] {
                        use tiberius::{Client, Config};
                        use tokio::net::TcpStream;
                        use tokio_util::compat::TokioAsyncWriteCompatExt;
                        use std::convert::TryInto;

                        let config: Config = (&*$config).try_into()?;
                        let tcp = TcpStream::connect(config.get_addr())
                            .await
                            .migration_err("could not connect to database", None)?;
                        let client = Client::connect(config, tcp.compat_write())
                            .await
                            .migration_err("could not connect to database", None)?;

                        $op(client).await
                    } else {
                        panic!("tried to migrate async from config for a mssql database, but tiberius-config feature was not enabled!");
                    }
                }
            }
        }
    }
}

// rewrite all the default methods as we overrode Transaction and Query
#[cfg(any(feature = "mysql", feature = "postgres", feature = "rusqlite"))]
impl crate::Migrate for Config {
    fn get_last_applied_migration(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Option<Migration>, Error> {
        with_connection!(self, |mut conn| {
            let mut migrations: Vec<Migration> = Query::query(
                &mut conn,
                &GET_LAST_APPLIED_MIGRATION_QUERY
                    .replace("%MIGRATION_TABLE_NAME%", migration_table_name),
            )
            .migration_err("error getting last applied migration", None)?;

            Ok(migrations.pop())
        })
    }

    fn get_applied_migrations(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        with_connection!(self, |mut conn| {
            let migrations: Vec<Migration> = Query::query(
                &mut conn,
                &GET_APPLIED_MIGRATIONS_QUERY
                    .replace("%MIGRATION_TABLE_NAME%", migration_table_name),
            )
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
        migration_table_name: &str,
    ) -> Result<Report, Error> {
        with_connection!(self, |mut conn| {
            crate::Migrate::migrate(
                &mut conn,
                migrations,
                abort_divergent,
                abort_missing,
                grouped,
                target,
                migration_table_name,
            )
        })
    }
}

#[cfg(any(
    feature = "mysql_async",
    feature = "tokio-postgres",
    feature = "tiberius-config"
))]
#[async_trait]
impl crate::AsyncMigrate for Config {
    async fn get_last_applied_migration(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Option<Migration>, Error> {
        with_connection_async!(self, move |mut conn| async move {
            let mut migrations: Vec<Migration> = AsyncQuery::query(
                &mut conn,
                &GET_LAST_APPLIED_MIGRATION_QUERY
                    .replace("%MIGRATION_TABLE_NAME%", migration_table_name),
            )
            .await
            .migration_err("error getting last applied migration", None)?;

            Ok(migrations.pop())
        })
    }

    async fn get_applied_migrations(
        &mut self,
        migration_table_name: &str,
    ) -> Result<Vec<Migration>, Error> {
        with_connection_async!(self, move |mut conn| async move {
            let migrations: Vec<Migration> = AsyncQuery::query(
                &mut conn,
                &GET_APPLIED_MIGRATIONS_QUERY
                    .replace("%MIGRATION_TABLE_NAME%", migration_table_name),
            )
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
        migration_table_name: &str,
    ) -> Result<Report, Error> {
        with_connection_async!(self, move |mut conn| async move {
            crate::AsyncMigrate::migrate(
                &mut conn,
                migrations,
                abort_divergent,
                abort_missing,
                grouped,
                target,
                migration_table_name,
            )
            .await
        })
    }
}
