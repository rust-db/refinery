#[cfg(feature = "clickhouse")]
mod clickhouse {
    use futures::FutureExt;
    use refinery::{
        config::{Config, ConfigDbType},
        embed_migrations,
        error::Kind,
        AsyncMigrate, Migration, Runner, Target,
    };
    use refinery_core::klickhouse::{Client, ClientOptions, RawRow, UnitValue};
    use std::panic::AssertUnwindSafe;
    use time::OffsetDateTime;

    const DEFAULT_TABLE_NAME: &str = "refinery_schema_history";

    fn get_migrations() -> Vec<Migration> {
        embed_migrations!("./tests/migrations_clickhouse");

        let migration1 = Migration::unapplied(
            "V1__initial",
            include_str!("./migrations_clickhouse/V1-2/V1__initial.sql"),
        )
        .unwrap();

        let migration2 = Migration::unapplied(
            "V2__add_cars_and_motos_table",
            include_str!("./migrations_clickhouse/V1-2/V2__add_cars_and_motos_table.sql"),
        )
        .unwrap();

        let migration3 = Migration::unapplied(
            "V3__add_brand_to_cars_table",
            include_str!("./migrations_clickhouse/V3/V3__add_brand_to_cars_table.sql"),
        )
        .unwrap();

        let migration4 = Migration::unapplied(
            "V4__add_year_to_motos_table",
            include_str!("./migrations_clickhouse/V4__add_year_to_motos_table.sql"),
        )
        .unwrap();

        let migration5 = Migration::unapplied(
            "V5__add_year_field_to_cars",
            "ALTER TABLE cars ADD COLUMN year Int32;",
        )
        .unwrap();

        vec![migration1, migration2, migration3, migration4, migration5]
    }

    mod embedded {
        use refinery::embed_migrations;
        embed_migrations!("./tests/migrations_clickhouse");
    }

    mod broken {
        use refinery::embed_migrations;
        embed_migrations!("./tests/migrations_broken");
    }

    mod missing {
        use refinery::embed_migrations;
        embed_migrations!("./tests/migrations_missing_clickhouse");
    }

    async fn run_test<T: std::future::Future<Output = ()>>(t: T) {
        let result = AssertUnwindSafe(t).catch_unwind().await;
        assert!(result.is_ok());
    }

    async fn connect() -> Client {
        let client = Client::connect(
            "localhost:9000",
            ClientOptions {
                username: "default".to_string(),
                password: "".to_string(),
                default_database: "".to_string(),
            },
        )
        .await
        .unwrap();

        client
            .execute("DROP DATABASE IF EXISTS refinery_test")
            .await
            .unwrap();
        client
            .execute("CREATE DATABASE refinery_test")
            .await
            .unwrap();
        drop(client);
        let client = Client::connect(
            "localhost:9000",
            ClientOptions {
                username: "default".to_string(),
                password: "".to_string(),
                default_database: "refinery_test".to_string(),
            },
        )
        .await
        .unwrap();
        client
    }

    #[tokio::test]
    async fn creates_migration_table() {
        run_test(async {
            let mut client = connect().await;

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            client
                .query_collect::<UnitValue<String>>(&format!(
                    "SELECT table_name FROM information_schema.tables WHERE table_name='{}'",
                    DEFAULT_TABLE_NAME
                ))
                .await
                .unwrap()
                .into_iter()
                .for_each(|name| {
                    assert_eq!(DEFAULT_TABLE_NAME, name.0);
                });
        })
        .await;
    }

    #[tokio::test]
    async fn report_contains_applied_migrations() {
        run_test(async {
            let mut client = connect().await;

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
    async fn applies_migration() {
        run_test(async {
            let mut client = connect().await;

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            client
                .execute("INSERT INTO persons (name, city) VALUES ('John Legend', 'New York')")
                .await
                .unwrap();

            let mut rows: Vec<RawRow> = client
                .query_collect("SELECT name, city FROM persons")
                .await
                .unwrap();

            {
                assert_eq!("John Legend", rows[0].get::<_, String>(0));
                assert_eq!("New York", rows[0].get::<_, String>(1));
            }
        })
        .await
    }

    #[tokio::test]
    async fn updates_schema_history() {
        run_test(async {
            let mut client = connect().await;

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
    async fn gets_applied_migrations() {
        run_test(async {
            let mut client = connect().await;

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let applied_migrations = client
                .get_applied_migrations(DEFAULT_TABLE_NAME)
                .await
                .unwrap();
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
    async fn applies_new_migration() {
        run_test(async {
            let mut client = connect().await;

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
            let mut client = connect().await;

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
            let applied_migrations = report.applied_migrations();
            let migrations = get_migrations();

            assert_eq!(3, current.version());

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
            let mut client = connect().await;

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let migration = Migration::unapplied(
                "V4__add_year_field_to_cars",
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
            let mut client = connect().await;

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            embedded::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let migration = Migration::unapplied(
                "V2__add_year_field_to_cars",
                "ALTER TABLE cars ADD COLUMN year INTEGER;",
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
            };
        })
        .await;
    }

    #[tokio::test]
    async fn aborts_on_missing_migration_on_database() {
        run_test(async {
            let mut client = connect().await;

            missing::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let migration1 = Migration::unapplied(
                "V1__initial",
                r#"
                CREATE TABLE persons (
                    id Int32,
                    name String,
                    city String
                )
                Engine=MergeTree() ORDER BY id;
                "#,
            )
            .unwrap();

            let migration2 = Migration::unapplied(
                "V2__add_cars_table",
                include_str!("./migrations_missing_clickhouse/V2__add_cars_table.sql"),
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
            // clear database
            connect().await;

            let mut config = Config::new(ConfigDbType::Clickhouse)
                .set_db_name("refinery_test")
                .set_db_host("localhost")
                .set_db_port("9000");

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
            // clear database
            connect().await;

            let mut config = Config::new(ConfigDbType::Clickhouse)
                .set_db_name("refinery_test")
                .set_db_host("localhost")
                .set_db_port("9000");

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
            // clear database
            connect().await;

            let mut config = Config::new(ConfigDbType::Clickhouse)
                .set_db_name("refinery_test")
                .set_db_host("localhost")
                .set_db_port("9000");

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
            let mut client = connect().await;

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
                .query_collect::<UnitValue<String>>(
                    "SELECT table_name FROM information_schema.tables WHERE table_name='persons'",
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
            let mut client = connect().await;

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
                .query_collect::<UnitValue<String>>(
                    "SELECT table_name FROM information_schema.tables WHERE table_name='persons'",
                )
                .await
                .unwrap();

            assert!(row.is_empty());
        })
        .await;
    }
}
