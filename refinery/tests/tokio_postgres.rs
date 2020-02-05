use barrel::backend::Pg as Sql;
mod mod_migrations;

#[cfg(all(feature = "tokio", feature = "tokio-postgres"))]
mod tokio_postgres {
    use super::mod_migrations;
    use chrono::{DateTime, Local};
    use futures::FutureExt;
    use refinery::{
        config::{migrate_from_config_async, Config, ConfigDbType},
        AsyncMigrate, Error, Migration,
    };
    use std::panic::AssertUnwindSafe;
    use tokio_postgres::NoTls;

    fn get_migrations() -> Vec<Migration> {
        let migration1 = Migration::from_filename(
            "V1__initial.sql",
            include_str!("./sql_migrations/V1__initial.sql"),
        )
        .unwrap();

        let migration2 = Migration::from_filename(
            "V2__add_cars_and_motos_table.sql",
            include_str!("./sql_migrations/V2__add_cars_and_motos_table.sql"),
        )
        .unwrap();

        let migration3 = Migration::from_filename(
            "V3__add_brand_to_cars_table",
            include_str!("./sql_migrations/V3__add_brand_to_cars_table.sql"),
        )
        .unwrap();

        let migration4 = Migration::from_filename(
            "V4__add_year_to_motos_table.sql",
            include_str!("./sql_migrations/V4__add_year_to_motos_table.sql"),
        )
        .unwrap();

        let migration5 = Migration::from_filename(
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
    async fn embedded_creates_migration_table() {
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
                .query("SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'", &[])
                .await
                .unwrap();


            for row in rows {
                let table_name: String = row.get(0);
                assert_eq!("refinery_schema_history", table_name);
            }
        }).await;
    }

    #[tokio::test]
    async fn embedded_creates_migration_table_grouped_migrations() {
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
                .query("SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'", &[])
                .await
                .unwrap();

            for row in rows {
                let table_name: String = row.get(0);
                assert_eq!("refinery_schema_history", table_name);
            }
        }).await;
    }

    #[tokio::test]
    async fn embedded_applies_migration() {
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
    async fn embedded_applies_migration_grouped() {
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
    async fn embedded_updates_schema_history() {
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

        for row in client
            .query("SELECT MAX(version) FROM refinery_schema_history", &[])
            .await
            .unwrap()
        {
            let current: i32 = row.get(0);
            assert_eq!(4, current);
        }

        for row in client
            .query("SELECT applied_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)", &[])
                .await
                .unwrap()
                {
                    let applied_on: String = row.get(0);
                    let applied_on = DateTime::parse_from_rfc3339(&applied_on).unwrap().with_timezone(&Local);
                    assert_eq!(Local::today(), applied_on.date());
                }
        }).await
    }

    #[tokio::test]
    async fn embedded_updates_schema_history_grouped() {
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

            for row in client
                .query("SELECT MAX(version) FROM refinery_schema_history", &[])
                    .await
                    .unwrap()
                    {
                        let current: i32 = row.get(0);
                        assert_eq!(4, current);
                    }

            for row in client
                .query("SELECT applied_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)", &[])
                    .await
                    .unwrap()
                    {
                        let applied_on: String = row.get(0);
                        let applied_on = DateTime::parse_from_rfc3339(&applied_on).unwrap().with_timezone(&Local);
                        assert_eq!(Local::today(), applied_on.date());
                    }
        }).await
    }

    #[tokio::test]
    async fn embedded_updates_to_last_working_if_not_grouped() {
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

            for row in client
                .query("SELECT MAX(version) FROM refinery_schema_history", &[])
                .await
                .unwrap()
            {
                let current: i32 = row.get(0);
                assert_eq!(2, current);
            }
        })
        .await
    }

    #[tokio::test]
    async fn embedded_doesnt_update_to_last_working_if_grouped() {
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
    async fn mod_creates_migration_table() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                .await
                .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            mod_migrations::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let rows = client
                .query("SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'", &[])
                .await
                .unwrap();

            for row in rows {
                let table_name: String = row.get(0);
                assert_eq!("refinery_schema_history", table_name);
            }
        }).await
    }

    #[tokio::test]
    async fn mod_applies_migration() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                    .await
                    .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            mod_migrations::migrations::runner()
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
    async fn mod_updates_schema_history() {
        run_test(async {
            let (mut client, connection) =
                tokio_postgres::connect("postgres://postgres@localhost:5432/postgres", NoTls)
                .await
                .unwrap();

            tokio::spawn(async move {
                connection.await.unwrap();
            });

            mod_migrations::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            for row in client
                .query("SELECT MAX(version) FROM refinery_schema_history", &[])
                    .await
                    .unwrap()
                    {
                        let current: i32 = row.get(0);
                        assert_eq!(4, current);
                    }

            for row in client
                .query("SELECT applied_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)", &[])
                    .await
                    .unwrap()
                    {
                        let applied_on: String = row.get(0);
                        let applied_on = DateTime::parse_from_rfc3339(&applied_on).unwrap().with_timezone(&Local);
                        assert_eq!(Local::today(), applied_on.date());
                    }
        }).await
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
                )
                .await
                .unwrap();

            for row in client
                .query("SELECT version, checksum FROM refinery_schema_history where version = (SELECT MAX(version) from refinery_schema_history)", &[])
                    .await
                    .unwrap()
                    {
                        let current: i32 = row.get(0);
                        let checksum: String = row.get(1);
                        assert_eq!(5, current);
                        assert_eq!(mchecksum.to_string(), checksum);
                    }
        }).await;
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

            mod_migrations::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let migration = Migration::from_filename(
                "V4__add_year_field_to_cars",
                &"ALTER TABLE cars ADD year INTEGER;",
            )
            .unwrap();
            let err = client
                .migrate(&[migration.clone()], true, true, false)
                .await
                .unwrap_err();

            match err {
                Error::MissingVersion(missing) => {
                    assert_eq!(1, missing.version);
                    assert_eq!("initial", missing.name);
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

            mod_migrations::migrations::runner()
                .run_async(&mut client)
                .await
                .unwrap();

            let migration = Migration::from_filename(
                "V2__add_year_field_to_cars",
                &"ALTER TABLE cars ADD year INTEGER;",
            )
            .unwrap();

            let err = client
                .migrate(&[migration.clone()], true, false, false)
                .await
                .unwrap_err();

            match err {
                Error::DivergentVersion(applied, divergent) => {
                    assert_eq!(migration, divergent);
                    assert_eq!(2, applied.version);
                    assert_eq!("add_cars_table", applied.name);
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

            let migration1 = Migration::from_filename(
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

            let migration2 = Migration::from_filename(
                "V2__add_cars_table",
                include_str!("./sql_migrations_missing/V2__add_cars_table.sql"),
            )
            .unwrap();
            let err = client
                .migrate(&[migration1, migration2], true, true, false)
                .await
                .unwrap_err();

            match err {
                Error::MissingVersion(missing) => {
                    assert_eq!(1, missing.version);
                    assert_eq!("initial", missing.name);
                }
                _ => panic!("failed test"),
            }
        })
        .await;
    }

    #[tokio::test]
    async fn migrates_from_config() {
        run_test(async {
            let config = Config::new(ConfigDbType::Postgres)
                .set_db_name("postgres")
                .set_db_user("postgres")
                .set_db_host("localhost")
                .set_db_port("5432");

            let migrations = get_migrations();
            migrate_from_config_async(&config, false, true, true, &migrations)
                .await
                .unwrap();
        })
        .await;
    }
}
