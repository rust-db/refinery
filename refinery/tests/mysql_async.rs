use barrel::backend::MySql as Sql;
mod mod_migrations;

#[cfg(all(feature = "tokio", feature = "mysql_async"))]
mod mysql_async {
    use super::mod_migrations;
    use chrono::Local;
    use futures::FutureExt;
    use refinery::{
        config::{Config, ConfigDbType},
        error::Kind,
        AsyncMigrate, Migration, Runner, Target,
    };
    use refinery_core::mysql_async::prelude::Queryable;
    use refinery_core::{mysql_async, tokio};
    use std::panic::AssertUnwindSafe;

    fn get_migrations() -> Vec<Migration> {
        let migration1 = Migration::unapplied(
            "V1__initial.sql",
            include_str!("./sql_migrations/V1-2/V1__initial.sql"),
        )
        .unwrap();

        let migration2 = Migration::unapplied(
            "V2__add_cars_and_motos_table.sql",
            include_str!("./sql_migrations/V1-2/V2__add_cars_and_motos_table.sql"),
        )
        .unwrap();

        let migration3 = Migration::unapplied(
            "V3__add_brand_to_cars_table",
            include_str!("./sql_migrations/V3/V3__add_brand_to_cars_table.sql"),
        )
        .unwrap();

        let migration4 = Migration::unapplied(
            "V4__add_year_to_motos_table.sql",
            include_str!("./sql_migrations/V4__add_year_to_motos_table.sql"),
        )
        .unwrap();

        let migration5 = Migration::unapplied(
            "V5__add_year_field_to_cars",
            &"ALTER TABLE cars ADD year INTEGER;",
        )
        .unwrap();

        vec![migration1, migration2, migration3, migration4, migration5]
    }

    mod embedded {
        use refinery::embed_migrations;
        embed_migrations!("./tests/sql_migrations");
    }

    mod broken {
        use refinery::embed_migrations;
        embed_migrations!("./tests/sql_migrations_broken");
    }

    mod missing {
        use refinery::embed_migrations;
        embed_migrations!("./tests/sql_migrations_missing");
    }

    async fn run_test<T: std::future::Future<Output = ()>>(t: T) {
        let result = AssertUnwindSafe(t).catch_unwind().await;
        clean_database().await;
        assert!(result.is_ok());
    }

    async fn clean_database() {
        let pool = mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");
        let mut conn = pool.get_conn().await.unwrap();

        conn = conn
            .drop_query("DROP DATABASE refinery_test")
            .await
            .unwrap();
        conn.drop_query("CREATE DATABASE refinery_test")
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn embedded_creates_migration_table() {
        run_test(async {
            let mut pool = mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");
            let conn = pool.get_conn().await.unwrap();

            embedded::migrations::runner()
                .run_async(&mut pool)
                .await
                .unwrap();

            conn
                .query("SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'")
                .await
                .unwrap()
                .for_each(|row| {
                   let name = row.get::<Option<String>, _>(0).unwrap().unwrap();
                   assert_eq!("refinery_schema_history", name);

            })
            .await
            .unwrap();

        }).await;
    }

    #[tokio::test]
    async fn embedded_creates_migration_table_grouped_transaction() {
        run_test(async {
            let mut pool = mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");
            let conn = pool.get_conn().await.unwrap();

            embedded::migrations::runner()
                .set_grouped(true)
                .run_async(&mut pool)
                .await
                .unwrap();

            let result = conn
                .query("SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'")
                .await
                .unwrap();


            result.for_each(|row| {
                let table_name: String = row.get(0).unwrap();
                assert_eq!("refinery_schema_history", table_name);

            })
            .await
            .unwrap();

        }).await;
    }

    #[tokio::test]
    async fn report_contains_applied_migrations() {
        run_test(async {
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");

            let report = embedded::migrations::runner()
                .run_async(&mut pool)
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
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");
            let mut conn = pool.get_conn().await.unwrap();

            embedded::migrations::runner()
                .run_async(&mut pool)
                .await
                .unwrap();

            conn = conn
                .query("INSERT INTO persons (name, city) VALUES ('John Legend', 'New York')")
                .await
                .unwrap()
                .drop_result()
                .await
                .unwrap();

            let result = conn.query("SELECT name, city FROM persons").await.unwrap();

            let (_, rows) = result
                .map_and_drop(|row| {
                    let name: String = row.get(0).unwrap();
                    let city: String = row.get(1).unwrap();
                    (name, city)
                })
                .await
                .unwrap();

            {
                let (name, city) = &rows[0];
                assert_eq!("John Legend", name);
                assert_eq!("New York", city);
            }
        })
        .await
    }

    #[tokio::test]
    async fn embedded_applies_migration_grouped_transaction() {
        run_test(async {
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");
            let mut conn = pool.get_conn().await.unwrap();

            embedded::migrations::runner()
                .set_grouped(true)
                .run_async(&mut pool)
                .await
                .unwrap();

            conn = conn
                .query("INSERT INTO persons (name, city) VALUES ('John Legend', 'New York')")
                .await
                .unwrap()
                .drop_result()
                .await
                .unwrap();

            let result = conn.query("SELECT name, city FROM persons").await.unwrap();

            let (_, rows) = result
                .map_and_drop(|row| {
                    let name: String = row.get(0).unwrap();
                    let city: String = row.get(1).unwrap();
                    (name, city)
                })
                .await
                .unwrap();

            {
                let (name, city) = &rows[0];
                assert_eq!("John Legend", name);
                assert_eq!("New York", city);
            }
        })
        .await
    }

    #[tokio::test]
    async fn embedded_updates_schema_history() {
        run_test(async {
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");

            embedded::migrations::runner()
                .run_async(&mut pool)
                .await
                .unwrap();

            let current = pool.get_last_applied_migration().await.unwrap().unwrap();

            assert_eq!(4, current.version());
            assert_eq!(Local::today(), current.applied_on().unwrap().date());
        })
        .await
    }

    #[tokio::test]
    async fn embedded_updates_schema_history_grouped_transaction() {
        run_test(async {
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");

            embedded::migrations::runner()
                .set_grouped(true)
                .run_async(&mut pool)
                .await
                .unwrap();

            let current = pool.get_last_applied_migration().await.unwrap().unwrap();

            assert_eq!(4, current.version());
            assert_eq!(Local::today(), current.applied_on().unwrap().date());
        })
        .await
    }

    #[tokio::test]
    async fn embedded_updates_to_last_working_if_not_grouped_transaction() {
        run_test(async {
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");

            let result = broken::migrations::runner().run_async(&mut pool).await;

            let current = pool.get_last_applied_migration().await.unwrap().unwrap();

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
    async fn mod_creates_migration_table() {
        run_test(async {
            let mut pool = mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");
            let conn = pool.get_conn().await.unwrap();

            mod_migrations::runner()
                .run_async(&mut pool)
                .await
                .unwrap();

            conn
                .query("SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'")
                .await
                .unwrap()
                .for_each(|row| {
                   let name = row.get::<Option<String>, _>(0).unwrap().unwrap();
                   assert_eq!("refinery_schema_history", name);

            })
            .await
            .unwrap();

        }).await;
    }

    #[tokio::test]
    async fn mod_applies_migration() {
        run_test(async {
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");
            let mut conn = pool.get_conn().await.unwrap();

            mod_migrations::runner().run_async(&mut pool).await.unwrap();

            conn = conn
                .query("INSERT INTO persons (name, city) VALUES ('John Legend', 'New York')")
                .await
                .unwrap()
                .drop_result()
                .await
                .unwrap();

            let result = conn.query("SELECT name, city FROM persons").await.unwrap();

            let (_, rows) = result
                .map_and_drop(|row| {
                    let name: String = row.get(0).unwrap();
                    let city: String = row.get(1).unwrap();
                    (name, city)
                })
                .await
                .unwrap();

            {
                let (name, city) = &rows[0];
                assert_eq!("John Legend", name);
                assert_eq!("New York", city);
            }
        })
        .await
    }

    #[tokio::test]
    async fn mod_updates_schema_history() {
        run_test(async {
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");

            mod_migrations::runner().run_async(&mut pool).await.unwrap();

            let current = pool.get_last_applied_migration().await.unwrap().unwrap();

            assert_eq!(4, current.version());
            assert_eq!(Local::today(), current.applied_on().unwrap().date());
        })
        .await
    }

    #[tokio::test]
    async fn gets_applied_migrations() {
        run_test(async {
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");

            embedded::migrations::runner()
                .run_async(&mut pool)
                .await
                .unwrap();

            let applied_migrations = pool.get_applied_migrations().await.unwrap();
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
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");

            embedded::migrations::runner()
                .run_async(&mut pool)
                .await
                .unwrap();

            let migrations = get_migrations();

            let mchecksum = migrations[4].checksum();
            pool.migrate(&migrations, true, true, false, Target::Latest)
                .await
                .unwrap();

            let current = pool.get_last_applied_migration().await.unwrap().unwrap();

            assert_eq!(5, current.version());
            assert_eq!(mchecksum, current.checksum());
        })
        .await;
    }

    #[tokio::test]
    async fn migrates_to_target_migration() {
        run_test(async {
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");

            let report = embedded::migrations::runner()
                .set_grouped(true)
                .set_target(Target::Version(3))
                .run_async(&mut pool)
                .await
                .unwrap();

            let current = pool.get_last_applied_migration().await.unwrap().unwrap();
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
    async fn migrates_to_target_migration_grouped() {
        run_test(async {
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");

            let report = embedded::migrations::runner()
                .set_target(Target::Version(3))
                .run_async(&mut pool)
                .await
                .unwrap();

            let current = pool.get_last_applied_migration().await.unwrap().unwrap();
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
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");

            mod_migrations::runner().run_async(&mut pool).await.unwrap();

            let migration = Migration::unapplied(
                "V4__add_year_field_to_cars",
                &"ALTER TABLE cars ADD year INTEGER;",
            )
            .unwrap();

            let err = pool
                .migrate(&[migration.clone()], true, true, false, Target::Latest)
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
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");

            mod_migrations::runner().run_async(&mut pool).await.unwrap();

            mod_migrations::runner().run_async(&mut pool).await.unwrap();

            let migration = Migration::unapplied(
                "V2__add_year_field_to_cars",
                &"ALTER TABLE cars ADD year INTEGER;",
            )
            .unwrap();

            let err = pool
                .migrate(&[migration.clone()], true, false, false, Target::Latest)
                .await
                .unwrap_err();

            match err.kind() {
                Kind::DivergentVersion(applied, divergent) => {
                    assert_eq!(&migration, divergent);
                    assert_eq!(2, applied.version());
                    assert_eq!("add_cars_table", applied.name());
                }
                _ => panic!("failed test"),
            };
        })
        .await;
    }

    #[tokio::test]
    async fn aborts_on_missing_migration_on_database() {
        run_test(async {
            let mut pool =
                mysql_async::Pool::new("mysql://refinery:root@localhost:3306/refinery_test");

            missing::migrations::runner()
                .run_async(&mut pool)
                .await
                .unwrap();

            let migration1 = Migration::unapplied(
                "V1__initial",
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
                include_str!("./sql_migrations_missing/V2__add_cars_table.sql"),
            )
            .unwrap();
            let err = pool
                .migrate(&[migration1, migration2], true, true, false, Target::Latest)
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
            let mut config = Config::new(ConfigDbType::Mysql)
                .set_db_name("refinery_test")
                .set_db_user("refinery")
                .set_db_pass("root")
                .set_db_host("localhost")
                .set_db_port("3306");

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
            let mut config = Config::new(ConfigDbType::Mysql)
                .set_db_name("refinery_test")
                .set_db_user("refinery")
                .set_db_pass("root")
                .set_db_host("localhost")
                .set_db_port("3306");

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
            let mut config = Config::new(ConfigDbType::Mysql)
                .set_db_name("refinery_test")
                .set_db_user("refinery")
                .set_db_pass("root")
                .set_db_host("localhost")
                .set_db_port("3306");

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
}
