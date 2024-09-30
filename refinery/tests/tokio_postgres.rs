use barrel::backend::Pg as Sql;

#[cfg(feature = "tokio-postgres")]
mod tokio_postgres {
    use futures::FutureExt;
    use refinery::{
        config::{Config, ConfigDbType},
        embed_migrations,
        error::Kind,
        AsyncMigrate, Migration, Runner, Target,
    };
    use refinery_core::tokio_postgres;
    use refinery_core::tokio_postgres::NoTls;
    use std::panic::AssertUnwindSafe;
    use time::OffsetDateTime;

    const DEFAULT_TABLE_NAME: &str = "refinery_schema_history";

    fn get_migrations() -> Vec<Migration> {
        embed_migrations!("./tests/migrations");

        let migration1 = Migration::unapplied(
            "V1__initial.rs",
            None,
            &migrations::V1__initial::migration(),
        )
        .unwrap();

        let migration2 = Migration::unapplied(
            "V2__add_cars_and_motos_table.sql",
            None,
            include_str!("./migrations/V1-2/V2__add_cars_and_motos_table.sql"),
        )
        .unwrap();

        let migration3 = Migration::unapplied(
            "V3__add_brand_to_cars_table",
            None,
            include_str!("./migrations/V3/V3__add_brand_to_cars_table.sql"),
        )
        .unwrap();

        let migration4 = Migration::unapplied(
            "V4__add_year_to_motos_table.rs",
            None,
            &migrations::V4__add_year_to_motos_table::migration(),
        )
        .unwrap();

        let migration5 = Migration::unapplied(
            "V5__add_year_field_to_cars",
            None,
            "ALTER TABLE cars ADD year INTEGER;",
        )
        .unwrap();

        vec![migration1, migration2, migration3, migration4, migration5]
    }

    mod embedded {
        use refinery::embed_migrations;
        embed_migrations!("./tests/migrations");
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
        let result = AssertUnwindSafe(t).catch_unwind().await;
        clean_database().await;
        assert!(result.is_ok());
    }

    async fn clean_database() {
        let (client, connection) =
            tokio_postgres::connect("postgres://postgres@localhost:5432/template1", NoTls)
                .await
                .unwrap();

        tokio::spawn(async move {
            connection.await.unwrap();
        });

        client
            .execute(
                "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='postgres'",
                &[],
            )
            .await
            .unwrap();

        client.execute("DROP DATABASE postgres", &[]).await.unwrap();
        client
            .execute("CREATE DATABASE POSTGRES", &[])
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn report_contains_applied_migrations() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            let report = embedded::migrations::runner()
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
    async fn creates_migration_table() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let rows = client
                .query(
                    &format!(
                        "SELECT table_name FROM information_schema.tables WHERE table_name='{}'",
                        DEFAULT_TABLE_NAME
                    ),
                    &[],
                )
                .await
                .unwrap();

            for row in rows {
                let table_name: String = row.get(0);
                assert_eq!(DEFAULT_TABLE_NAME, table_name);
            }
        })
        .await;
    }

    #[tokio::test]
    async fn creates_migration_table_grouped_migrations() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            embedded::migrations::runner()
                .set_grouped(true)
                .run_async(&mut client)
                .await
                .unwrap();

            let rows = client
                .query(
                    &format!(
                        "SELECT table_name FROM information_schema.tables WHERE table_name='{}'",
                        DEFAULT_TABLE_NAME
                    ),
                    &[],
                )
                .await
                .unwrap();

            for row in rows {
                let table_name: String = row.get(0);
                assert_eq!(DEFAULT_TABLE_NAME, table_name);
            }
        })
        .await;
    }

    #[tokio::test]
    async fn applies_migration() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            client
                .execute(
                    "INSERT INTO persons (name, city) VALUES ($1, $2)",
                    &[&"John Legend", &"New York"],
                )
                .await
                .unwrap();

            for row in client
                .query("SELECT name, city FROM persons", &[])
                .await
                .unwrap()
            {
                let name: String = row.get(0);
                let city: String = row.get(1);
                assert_eq!("John Legend", name);
                assert_eq!("New York", city);
            }
        })
        .await
    }

    #[tokio::test]
    async fn applies_migration_grouped() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            embedded::migrations::runner()
                .set_grouped(true)
                .run_async(&mut client)
                .await
                .unwrap();

            client
                .execute(
                    "INSERT INTO persons (name, city) VALUES ($1, $2)",
                    &[&"John Legend", &"New York"],
                )
                .await
                .unwrap();

            for row in client
                .query("SELECT name, city FROM persons", &[])
                .await
                .unwrap()
            {
                let name: String = row.get(0);
                let city: String = row.get(1);
                assert_eq!("John Legend", name);
                assert_eq!("New York", city);
            }
        })
        .await
    }

    #[tokio::test]
    async fn updates_schema_history() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let current = client
                .get_last_applied_migration(DEFAULT_TABLE_NAME)
                .await
                .unwrap()
                .unwrap();

            assert_eq!(4, current.version());
            assert_eq!(
                OffsetDateTime::now_utc().date(),
                current.applied_on().unwrap().date()
            );
        })
        .await
    }

    #[tokio::test]
    async fn updates_schema_history_grouped() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            embedded::migrations::runner()
                .set_grouped(true)
                .run_async(&mut client)
                .await
                .unwrap();

            let current = client
                .get_last_applied_migration(DEFAULT_TABLE_NAME)
                .await
                .unwrap()
                .unwrap();

            assert_eq!(4, current.version());
            assert_eq!(
                OffsetDateTime::now_utc().date(),
                current.applied_on().unwrap().date()
            );
        })
        .await
    }

    #[tokio::test]
    async fn updates_to_last_working_if_not_grouped() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            let result = broken::migrations::runner().run_async(&mut client).await;

            assert!(result.is_err());

            let current = client
                .get_last_applied_migration(DEFAULT_TABLE_NAME)
                .await
                .unwrap()
                .unwrap();
            let err = result.unwrap_err();
            let migrations = get_migrations();
            let applied_migrations = err.report().unwrap().applied_migrations();

            assert_eq!(
                OffsetDateTime::now_utc().date(),
                current.applied_on().unwrap().date()
            );
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
    async fn doesnt_update_to_last_working_if_grouped() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            let result = broken::migrations::runner()
                .set_grouped(true)
                .run_async(&mut client)
                .await;

            assert!(result.is_err());

            let query = client
                .query("SELECT version FROM refinery_schema_history", &[])
                .await
                .unwrap();

            assert!(query.is_empty());
        })
        .await
    }

    #[tokio::test]
    async fn gets_applied_migrations() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let migrations = get_migrations();
            let applied_migrations = client
                .get_applied_migrations(DEFAULT_TABLE_NAME)
                .await
                .unwrap();
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
    async fn applies_new_migration() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let migrations = get_migrations();
            let mchecksum = migrations[4].checksum();

            client
                .migrate(
                    &migrations,
                    true,
                    true,
                    false,
                    Target::Latest,
                    DEFAULT_TABLE_NAME,
                )
                .await
                .unwrap();

            let current = client
                .get_last_applied_migration(DEFAULT_TABLE_NAME)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(5, current.version());
            assert_eq!(mchecksum, current.checksum());
        })
        .await;
    }

    #[tokio::test]
    async fn migrates_to_target_migration() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            let report = embedded::migrations::runner()
                .set_target(Target::Version(3))
                .run_async(&mut client)
                .await
                .unwrap();

            let current = client
                .get_last_applied_migration(DEFAULT_TABLE_NAME)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(3, current.version());

            let migrations = get_migrations();
            let applied_migrations = report.applied_migrations();

            assert_eq!(3, applied_migrations.len());

            assert_eq!(migrations[0].version(), applied_migrations[0].version());
            assert_eq!(migrations[1].version(), applied_migrations[1].version());
            assert_eq!(migrations[2].version(), applied_migrations[2].version());

            assert_eq!(migrations[0].name(), migrations[0].name());
            assert_eq!(migrations[1].name(), applied_migrations[1].name());
            assert_eq!(migrations[2].name(), applied_migrations[2].name());

            assert_eq!(migrations[0].checksum(), applied_migrations[0].checksum());
            assert_eq!(migrations[1].checksum(), applied_migrations[1].checksum());
            assert_eq!(migrations[2].checksum(), applied_migrations[2].checksum());
        })
        .await;
    }

    #[tokio::test]
    async fn migrates_to_target_migration_grouped() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            let report = embedded::migrations::runner()
                .set_target(Target::Version(3))
                .set_grouped(true)
                .run_async(&mut client)
                .await
                .unwrap();

            let current = client
                .get_last_applied_migration(DEFAULT_TABLE_NAME)
                .await
                .unwrap()
                .unwrap();
            assert_eq!(3, current.version());

            let migrations = get_migrations();
            let applied_migrations = report.applied_migrations();

            assert_eq!(3, applied_migrations.len());

            assert_eq!(migrations[0].version(), applied_migrations[0].version());
            assert_eq!(migrations[1].version(), applied_migrations[1].version());
            assert_eq!(migrations[2].version(), applied_migrations[2].version());

            assert_eq!(migrations[0].name(), migrations[0].name());
            assert_eq!(migrations[1].name(), applied_migrations[1].name());
            assert_eq!(migrations[2].name(), applied_migrations[2].name());

            assert_eq!(migrations[0].checksum(), applied_migrations[0].checksum());
            assert_eq!(migrations[1].checksum(), applied_migrations[1].checksum());
            assert_eq!(migrations[2].checksum(), applied_migrations[2].checksum());
        })
        .await;
    }

    #[tokio::test]
    async fn aborts_on_missing_migration_on_filesystem() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let migration = Migration::unapplied(
                "V4__add_year_field_to_cars",
                None,
                "ALTER TABLE cars ADD year INTEGER;",
            )
            .unwrap();
            let err = client
                .migrate(
                    &[migration.clone()],
                    true,
                    true,
                    false,
                    Target::Latest,
                    DEFAULT_TABLE_NAME,
                )
                .await
                .unwrap_err();

            match err.kind() {
                Kind::MissingVersion(missing) => {
                    assert_eq!(1, missing.version());
                    assert_eq!("initial", missing.name());
                }
                _ => panic!("failed test"),
            }
        })
        .await;
    }

    #[tokio::test]
    async fn aborts_on_divergent_migration() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let migration = Migration::unapplied(
                "V2__add_year_field_to_cars",
                None,
                "ALTER TABLE cars ADD year INTEGER;",
            )
            .unwrap();

            let err = client
                .migrate(
                    &[migration.clone()],
                    true,
                    false,
                    false,
                    Target::Latest,
                    DEFAULT_TABLE_NAME,
                )
                .await
                .unwrap_err();

            match err.kind() {
                Kind::DivergentVersion(applied, divergent) => {
                    assert_eq!(&migration, divergent);
                    assert_eq!(2, applied.version());
                    assert_eq!("add_cars_and_motos_table", applied.name());
                }
                _ => panic!("failed test"),
            }
        })
        .await;
    }

    #[tokio::test]
    async fn aborts_on_missing_migration_on_database() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            missing::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let migration1 = Migration::unapplied(
                "V1__initial",
                None,
                concat!(
                    "CREATE TABLE persons (",
                    "id int,",
                    "name varchar(255),",
                    "city varchar(255)",
                    ");"
                ),
            )
            .unwrap();

            let migration2 = Migration::unapplied(
                "V2__add_cars_table",
                None,
                include_str!("./migrations_missing/V2__add_cars_table.sql"),
            )
            .unwrap();
            let err = client
                .migrate(
                    &[migration1, migration2],
                    true,
                    true,
                    false,
                    Target::Latest,
                    DEFAULT_TABLE_NAME,
                )
                .await
                .unwrap_err();

            match err.kind() {
                Kind::MissingVersion(missing) => {
                    assert_eq!(1, missing.version());
                    assert_eq!("initial", missing.name());
                }
                _ => panic!("failed test"),
            }
        })
        .await;
    }

    #[tokio::test]
    async fn migrates_from_config() {
        run_test(async {
            let mut config = Config::new(ConfigDbType::Postgres)
                .set_db_name("postgres")
                .set_db_user("postgres")
                .set_db_host("localhost")
                .set_db_port("5432");

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
            let mut config = Config::new(ConfigDbType::Postgres)
                .set_db_name("postgres")
                .set_db_user("postgres")
                .set_db_host("localhost")
                .set_db_port("5432");

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
            let mut config = Config::new(ConfigDbType::Postgres)
                .set_db_name("postgres")
                .set_db_user("postgres")
                .set_db_host("localhost")
                .set_db_port("5432");

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

    #[tokio::test]
    async fn doesnt_run_migrations_if_fake() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            let report = embedded::migrations::runner()
                .set_target(Target::Fake)
                .run_async(&mut client)
                .await
                .unwrap();

            let applied_migrations = report.applied_migrations();
            assert!(applied_migrations.is_empty());

            let current = client
                .get_last_applied_migration(DEFAULT_TABLE_NAME)
                .await
                .unwrap()
                .unwrap();
            let migrations = get_migrations();
            let mchecksum = migrations[3].checksum();

            assert_eq!(4, current.version());
            assert_eq!(mchecksum, current.checksum());

            let row = client
                .query(
                    "SELECT table_name FROM information_schema.tables WHERE table_name='persons'",
                    &[],
                )
                .await
                .unwrap();
            assert!(row.is_empty());
        })
        .await;
    }

    #[tokio::test]
    async fn doesnt_run_migrations_if_fake_version() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            let report = embedded::migrations::runner()
                .set_target(Target::FakeVersion(2))
                .run_async(&mut client)
                .await
                .unwrap();

            let applied_migrations = report.applied_migrations();
            assert!(applied_migrations.is_empty());

            let current = client
                .get_last_applied_migration(DEFAULT_TABLE_NAME)
                .await
                .unwrap()
                .unwrap();
            let migrations = get_migrations();
            let mchecksum = migrations[1].checksum();

            assert_eq!(2, current.version());
            assert_eq!(mchecksum, current.checksum());

            let row = client
                .query(
                    "SELECT table_name FROM information_schema.tables WHERE table_name='persons'",
                    &[],
                )
                .await
                .unwrap();
            assert!(row.is_empty());
        })
        .await;
    }
}
