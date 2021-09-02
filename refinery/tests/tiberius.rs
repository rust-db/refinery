use barrel::backend::MsSql as Sql;

#[cfg(all(feature = "tiberius-config"))]
mod tiberius {
    use assert_cmd::prelude::*;
    use chrono::Local;
    use futures::FutureExt;
    use predicates::str::contains;
    use refinery::{
        config::{Config, ConfigDbType},
        AsyncMigrate, Migration, Runner,
    };
    use refinery_core::tiberius::{self, Config as TConfig};
    use std::convert::TryInto;
    use std::panic::AssertUnwindSafe;
    use std::process::Command;
    use tokio_util::compat::TokioAsyncWriteCompatExt;

    fn get_migrations() -> Vec<Migration> {
        let migration1 = Migration::unapplied(
            "V1__initial.sql",
            include_str!("./migrations_subdir/V1-2/V1__initial.sql"),
        )
        .unwrap();

        let migration2 = Migration::unapplied(
            "V2__add_cars_and_motos_table.sql",
            include_str!("./migrations_subdir/V1-2/V2__add_cars_and_motos_table.sql"),
        )
        .unwrap();

        let migration3 = Migration::unapplied(
            "V3__add_brand_to_cars_table",
            include_str!("./migrations_subdir/V3/V3__add_brand_to_cars_table.sql"),
        )
        .unwrap();

        let migration4 = Migration::unapplied(
            "V4__add_year_to_motos_table.sql",
            include_str!("./migrations_subdir/V4__add_year_to_motos_table.sql"),
        )
        .unwrap();

        let migration5 = Migration::unapplied(
            "V5__add_year_field_to_cars",
            "ALTER TABLE cars ADD year INTEGER;",
        )
        .unwrap();

        vec![migration1, migration2, migration3, migration4, migration5]
    }

    mod embedded {
        use refinery::embed_migrations;
        embed_migrations!("./tests/migrations");
    }

    mod subdir {
        use refinery::embed_migrations;
        embed_migrations!("./tests/migrations_subdir");
    }

    mod broken {
        use refinery::embed_migrations;
        embed_migrations!("./tests/migrations_broken");
    }

    mod missing {
        use refinery::embed_migrations;
        embed_migrations!("./tests/migrations_missing");
    }

    async fn run_test<T: std::future::Future<Output = ()>>(t: T) {
        let config = generate_config("tempdb");
        let tcp = tokio::net::TcpStream::connect(format!(
            "{}:{}",
            config.db_host().unwrap(),
            config.db_port().unwrap()
        ))
        .await
        .unwrap();
        let mut tconfig: TConfig = (&config).try_into().unwrap();
        tconfig.trust_cert();
        let mut client = tiberius::Client::connect(tconfig, tcp.compat_write())
            .await
            .unwrap();

        client
            .simple_query("CREATE DATABASE refinery_test")
            .await
            .unwrap();

        let result = AssertUnwindSafe(t).catch_unwind().await;
        client
            .simple_query("DROP DATABASE refinery_test")
            .await
            .unwrap();

        assert!(result.is_ok());
    }

    fn generate_config(database: &str) -> Config {
        Config::new(ConfigDbType::Mssql)
            .set_db_name(database)
            .set_db_user("SA")
            .set_db_host("localhost")
            .set_db_pass("Passw0rd")
            .set_db_port("1433")
    }

    #[tokio::test]
    async fn embedded_creates_migration_table() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!("{}:{}", config.db_host().unwrap(), config.db_port().unwrap()))
                .await
                .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write()).await.unwrap();

            subdir::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let row = client
                .simple_query("SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'")
                .await
                .unwrap()
                .into_row()
                .await
                .unwrap()
                .unwrap();

            let name: &str = row.get(0).unwrap();
            assert_eq!("refinery_schema_history", name);

        }).await;
    }

    #[tokio::test]
    async fn embedded_creates_migration_table_grouped_transaction() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!("{}:{}", config.db_host().unwrap(), config.db_port().unwrap()))
                .await
                .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write()).await.unwrap();

            subdir::migrations::runner()
                .set_grouped(true)
                .run_async(&mut client)
                .await
                .unwrap();

            let row = client
                .simple_query("SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'")
                .await
                .unwrap()
                .into_row()
                .await
                .unwrap()
                .unwrap();

            let name: &str = row.get(0).unwrap();
            assert_eq!("refinery_schema_history", name);

        }).await;
    }

    #[tokio::test]
    async fn report_contains_applied_migrations() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!(
                "{}:{}",
                config.db_host().unwrap(),
                config.db_port().unwrap()
            ))
            .await
            .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write())
                .await
                .unwrap();

            let report = subdir::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let migrations = get_migrations();
            let applied_migrations = report.applied_migrations();

            assert_eq!(4, applied_migrations.len());

            assert_eq!(migrations[0].version(), applied_migrations[0].version());
            assert_eq!(migrations[1].version(), applied_migrations[1].version());
            assert_eq!(migrations[2].version(), applied_migrations[2].version());
            assert_eq!(migrations[3].version(), applied_migrations[3].version());

            assert_eq!(migrations[0].name(), migrations[0].name());
            assert_eq!(migrations[1].name(), applied_migrations[1].name());
            assert_eq!(migrations[2].name(), applied_migrations[2].name());
            assert_eq!(migrations[3].name(), applied_migrations[3].name());

            assert_eq!(migrations[0].checksum(), applied_migrations[0].checksum());
            assert_eq!(migrations[1].checksum(), applied_migrations[1].checksum());
            assert_eq!(migrations[2].checksum(), applied_migrations[2].checksum());
            assert_eq!(migrations[3].checksum(), applied_migrations[3].checksum());
        })
        .await;
    }

    #[tokio::test]
    async fn embedded_applies_migration() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!(
                "{}:{}",
                config.db_host().unwrap(),
                config.db_port().unwrap()
            ))
            .await
            .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write())
                .await
                .unwrap();

            subdir::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            client
                .simple_query("INSERT INTO persons (name, city) VALUES ('John Legend', 'New York')")
                .await
                .unwrap();

            let row = client
                .simple_query("SELECT name, city FROM persons")
                .await
                .unwrap()
                .into_row()
                .await
                .unwrap()
                .unwrap();

            let name: &str = row.get(0).unwrap();
            let city: &str = row.get(1).unwrap();

            assert_eq!("John Legend", name);
            assert_eq!("New York", city);
        })
        .await
    }

    #[tokio::test]
    async fn embedded_applies_migration_grouped_transaction() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!(
                "{}:{}",
                config.db_host().unwrap(),
                config.db_port().unwrap()
            ))
            .await
            .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write())
                .await
                .unwrap();

            subdir::migrations::runner()
                .set_grouped(true)
                .run_async(&mut client)
                .await
                .unwrap();

            client
                .simple_query("INSERT INTO persons (name, city) VALUES ('John Legend', 'New York')")
                .await
                .unwrap();

            let row = client
                .simple_query("SELECT name, city FROM persons")
                .await
                .unwrap()
                .into_row()
                .await
                .unwrap()
                .unwrap();

            let name: &str = row.get(0).unwrap();
            let city: &str = row.get(1).unwrap();

            assert_eq!("John Legend", name);
            assert_eq!("New York", city);
        })
        .await
    }

    #[tokio::test]
    async fn embedded_updates_schema_history() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!(
                "{}:{}",
                config.db_host().unwrap(),
                config.db_port().unwrap()
            ))
            .await
            .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write())
                .await
                .unwrap();

            subdir::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let current = client.get_last_applied_migration().await.unwrap().unwrap();
            assert_eq!(4, current.version());
            assert_eq!(Local::today(), current.applied_on().unwrap().date());
        })
        .await
    }

    #[tokio::test]
    async fn embedded_updates_schema_history_grouped_transaction() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!(
                "{}:{}",
                config.db_host().unwrap(),
                config.db_port().unwrap()
            ))
            .await
            .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write())
                .await
                .unwrap();

            subdir::migrations::runner()
                .set_grouped(true)
                .run_async(&mut client)
                .await
                .unwrap();

            let current = client.get_last_applied_migration().await.unwrap().unwrap();
            assert_eq!(4, current.version());
            assert_eq!(Local::today(), current.applied_on().unwrap().date());
        })
        .await
    }

    #[tokio::test]
    async fn embedded_updates_to_last_working_if_not_grouped_transaction() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!(
                "{}:{}",
                config.db_host().unwrap(),
                config.db_port().unwrap()
            ))
            .await
            .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write())
                .await
                .unwrap();

            let result = broken::migrations::runner().run_async(&mut client).await;

            let current = client.get_last_applied_migration().await.unwrap().unwrap();

            let err = result.unwrap_err();
            let migrations = get_migrations();
            let applied_migrations = err.report().unwrap().applied_migrations();

            assert_eq!(Local::today(), current.applied_on().unwrap().date());
            assert_eq!(2, current.version());
            assert_eq!(2, applied_migrations.len());

            assert_eq!(1, applied_migrations[0].version());
            assert_eq!(2, applied_migrations[1].version());

            assert_eq!("initial", migrations[0].name());
            assert_eq!("add_cars_table", applied_migrations[1].name());

            assert_eq!(2959965718684201605, applied_migrations[0].checksum());
            assert_eq!(8238603820526370208, applied_migrations[1].checksum());
        })
        .await
    }

    #[tokio::test]
    async fn embedded_doesnt_update_to_last_working_if_grouped() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!(
                "{}:{}",
                config.db_host().unwrap(),
                config.db_port().unwrap()
            ))
            .await
            .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write())
                .await
                .unwrap();

            let result = broken::migrations::runner()
                .set_grouped(true)
                .run_async(&mut client)
                .await;

            let current = client.get_last_applied_migration().await.unwrap();

            dbg!(&current);
            assert!(current.is_none());
            // matches!(current, None);

            result.unwrap_err();

            let row = client
                .simple_query("SELECT version FROM refinery_schema_history")
                .await
                .unwrap()
                .into_row()
                .await
                .unwrap();

            assert!(row.is_none());
        })
        .await
    }

    #[tokio::test]
    async fn mod_creates_migration_table() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!("{}:{}", config.db_host().unwrap(), config.db_port().unwrap()))
                .await
                .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write()).await.unwrap();

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let row = client
                .simple_query("SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'")
                .await
                .unwrap()
                .into_row()
                .await
                .unwrap()
                .unwrap();

            let name: &str = row.get(0).unwrap();
            assert_eq!("refinery_schema_history", name);

        }).await;
    }

    #[tokio::test]
    async fn mod_applies_migration() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!(
                "{}:{}",
                config.db_host().unwrap(),
                config.db_port().unwrap()
            ))
            .await
            .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write())
                .await
                .unwrap();

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            client
                .simple_query("INSERT INTO persons (name, city) VALUES ('John Legend', 'New York')")
                .await
                .unwrap();

            let row = client
                .simple_query("SELECT name, city FROM persons")
                .await
                .unwrap()
                .into_row()
                .await
                .unwrap()
                .unwrap();

            let name: &str = row.get(0).unwrap();
            let city: &str = row.get(1).unwrap();

            assert_eq!("John Legend", name);
            assert_eq!("New York", city);
        })
        .await
    }

    #[tokio::test]
    async fn mod_updates_schema_history() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!(
                "{}:{}",
                config.db_host().unwrap(),
                config.db_port().unwrap()
            ))
            .await
            .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write())
                .await
                .unwrap();

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let current = client.get_last_applied_migration().await.unwrap().unwrap();
            assert_eq!(4, current.version());
            assert_eq!(Local::today(), current.applied_on().unwrap().date());
        })
        .await
    }

    #[tokio::test]
    async fn gets_applied_migrations() {
        run_test(async {
            let config = generate_config("refinery_test");

            let tcp = tokio::net::TcpStream::connect(format!(
                "{}:{}",
                config.db_host().unwrap(),
                config.db_port().unwrap()
            ))
            .await
            .unwrap();
            let mut tconfig: TConfig = (&config).try_into().unwrap();
            tconfig.trust_cert();
            let mut client = tiberius::Client::connect(tconfig, tcp.compat_write())
                .await
                .unwrap();

            subdir::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let applied_migrations = client.get_applied_migrations().await.unwrap();
            let migrations = get_migrations();
            assert_eq!(4, applied_migrations.len());

            assert_eq!(migrations[0].version(), applied_migrations[0].version());
            assert_eq!(migrations[1].version(), applied_migrations[1].version());
            assert_eq!(migrations[2].version(), applied_migrations[2].version());
            assert_eq!(migrations[3].version(), applied_migrations[3].version());

            assert_eq!(migrations[0].name(), migrations[0].name());
            assert_eq!(migrations[1].name(), applied_migrations[1].name());
            assert_eq!(migrations[2].name(), applied_migrations[2].name());
            assert_eq!(migrations[3].name(), applied_migrations[3].name());

            assert_eq!(migrations[0].checksum(), applied_migrations[0].checksum());
            assert_eq!(migrations[1].checksum(), applied_migrations[1].checksum());
            assert_eq!(migrations[2].checksum(), applied_migrations[2].checksum());
            assert_eq!(migrations[3].checksum(), applied_migrations[3].checksum());
        })
        .await;
    }

    #[tokio::test]
    async fn migrates_from_config() {
        run_test(async {
            let mut config = generate_config("refinery_test");
            config.set_trust_cert();

            let migrations = get_migrations();
            let runner = Runner::new(&migrations)
                .set_grouped(false)
                .set_abort_divergent(true)
                .set_abort_missing(true);

            runner.run_async(&mut config).await.unwrap();

            let applied_migrations = runner
                .get_applied_migrations_async(&mut config)
                .await
                .unwrap();
            assert_eq!(5, applied_migrations.len());

            assert_eq!(migrations[0].version(), applied_migrations[0].version());
            assert_eq!(migrations[1].version(), applied_migrations[1].version());
            assert_eq!(migrations[2].version(), applied_migrations[2].version());
            assert_eq!(migrations[3].version(), applied_migrations[3].version());
            assert_eq!(migrations[4].version(), applied_migrations[4].version());

            assert_eq!(migrations[0].name(), migrations[0].name());
            assert_eq!(migrations[1].name(), applied_migrations[1].name());
            assert_eq!(migrations[2].name(), applied_migrations[2].name());
            assert_eq!(migrations[3].name(), applied_migrations[3].name());
            assert_eq!(migrations[4].name(), applied_migrations[4].name());

            assert_eq!(migrations[0].checksum(), applied_migrations[0].checksum());
            assert_eq!(migrations[1].checksum(), applied_migrations[1].checksum());
            assert_eq!(migrations[2].checksum(), applied_migrations[2].checksum());
            assert_eq!(migrations[3].checksum(), applied_migrations[3].checksum());
            assert_eq!(migrations[4].checksum(), applied_migrations[4].checksum());
        })
        .await;
    }

    #[tokio::test]
    async fn migrate_from_config_report_contains_migrations() {
        run_test(async {
            let mut config = generate_config("refinery_test");
            config.set_trust_cert();

            let migrations = get_migrations();
            let runner = Runner::new(&migrations)
                .set_grouped(false)
                .set_abort_divergent(true)
                .set_abort_missing(true);

            let report = runner.run_async(&mut config).await.unwrap();

            let applied_migrations = report.applied_migrations();
            assert_eq!(5, applied_migrations.len());

            assert_eq!(migrations[0].version(), applied_migrations[0].version());
            assert_eq!(migrations[1].version(), applied_migrations[1].version());
            assert_eq!(migrations[2].version(), applied_migrations[2].version());
            assert_eq!(migrations[3].version(), applied_migrations[3].version());
            assert_eq!(migrations[4].version(), applied_migrations[4].version());

            assert_eq!(migrations[0].name(), migrations[0].name());
            assert_eq!(migrations[1].name(), applied_migrations[1].name());
            assert_eq!(migrations[2].name(), applied_migrations[2].name());
            assert_eq!(migrations[3].name(), applied_migrations[3].name());
            assert_eq!(migrations[4].name(), applied_migrations[4].name());

            assert_eq!(migrations[0].checksum(), applied_migrations[0].checksum());
            assert_eq!(migrations[1].checksum(), applied_migrations[1].checksum());
            assert_eq!(migrations[2].checksum(), applied_migrations[2].checksum());
            assert_eq!(migrations[3].checksum(), applied_migrations[3].checksum());
            assert_eq!(migrations[4].checksum(), applied_migrations[4].checksum());
        })
        .await;
    }

    #[tokio::test]
    async fn migrate_from_config_report_returns_last_applied_migration() {
        run_test(async {
            let mut config = generate_config("refinery_test");
            config.set_trust_cert();

            let migrations = get_migrations();
            let runner = Runner::new(&migrations)
                .set_grouped(false)
                .set_abort_divergent(true)
                .set_abort_missing(true);

            runner.run_async(&mut config).await.unwrap();

            let applied_migration = runner
                .get_last_applied_migration_async(&mut config)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(5, applied_migration.version());

            assert_eq!(migrations[4].version(), applied_migration.version());
            assert_eq!(migrations[4].name(), applied_migration.name());
            assert_eq!(migrations[4].checksum(), applied_migration.checksum());
        })
        .await;
    }

    // this is a blocking test, but shouldn't do arm running it inside tokio's runtime
    #[tokio::test]
    async fn migrates_from_cli() {
        run_test(async {
            Command::new("refinery")
                .args(&[
                    "migrate",
                    "-c",
                    "tests/tiberius_refinery.toml",
                    "files",
                    "-p",
                    "tests/migrations_subdir",
                ])
                .unwrap()
                .assert()
                .stdout(contains("applying migration: V4__add_year_to_motos_table"));
        })
        .await;
    }
}
