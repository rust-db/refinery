mod postgres_previous {
    use assert_cmd::prelude::*;
    use chrono::{DateTime, Local};
    use predicates::str::contains;
    use refinery::{migrate_from_config, Config, ConfigDbType, Error, Migrate as _, Migration};
    use std::process::Command;
    use tpppostgres::{Connection, TlsMode};

    mod embedded {
        use refinery::embed_migrations;
        embed_migrations!("refinery/tests/sql_migrations");
    }

    mod broken {
        use refinery::embed_migrations;
        embed_migrations!("refinery/tests/sql_migrations_broken");
    }

    mod missing {
        use refinery::embed_migrations;
        embed_migrations!("refinery/tests/sql_migrations_missing");
    }

    fn get_migrations() -> Vec<Migration> {
        let migration1 = Migration::from_filename(
            "V1__initial.sql",
            include_str!("./sql_migrations/V1__initial.sql"),
        )
        .unwrap();

        let migration2 = Migration::from_filename(
            "V2__add_cars_table",
            include_str!("./sql_migrations/V2__add_cars_table.sql"),
        )
        .unwrap();

        let migration3 = Migration::from_filename(
            "V3__add_brand_to_cars_table",
            include_str!("./sql_migrations/V3__add_brand_to_cars_table.sql"),
        )
        .unwrap();

        let migration4 = Migration::from_filename(
            "V4__add_year_field_to_cars",
            &"ALTER TABLE cars ADD year INTEGER;",
        )
        .unwrap();

        vec![migration1, migration2, migration3, migration4]
    }

    fn clean_database() {
        let conn = Connection::connect(
            "postgres://postgres@localhost:5432/template1",
            TlsMode::None,
        )
        .unwrap();

        conn.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname='postgres'",
            &[],
        )
        .unwrap();
        conn.execute("DROP DATABASE postgres", &[]).unwrap();
        conn.execute("CREATE DATABASE POSTGRES", &[]).unwrap();
    }

    fn run_test<T>(test: T) -> ()
    where
        T: FnOnce() -> () + std::panic::UnwindSafe,
    {
        let result = std::panic::catch_unwind(|| test());

        clean_database();

        assert!(result.is_ok())
    }

    #[test]
    fn embedded_creates_migration_table() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();
            embedded::migrations::runner().run(&mut conn).unwrap();
            for row in &conn
                .query(
                    "SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'", &[]
                )
                .unwrap()
            {
                let table_name: String = row.get(0);
                assert_eq!("refinery_schema_history", table_name);
            }
        });
    }

    #[test]
    fn embedded_creates_migration_table_single_transaction() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();

            embedded::migrations::runner()
                .set_grouped(true)
                .run(&mut conn)
                .unwrap();

            for row in &conn
                .query(
                    "SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'", &[]
                )
                .unwrap()
            {
                let table_name: String = row.get(0);
                assert_eq!("refinery_schema_history", table_name);
            }
        });
    }

    #[test]
    fn embedded_applies_migration() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();
            embedded::migrations::runner().run(&mut conn).unwrap();
            conn.execute(
                "INSERT INTO persons (name, city) VALUES ($1, $2)",
                &[&"John Legend", &"New York"],
            )
            .unwrap();
            for row in &conn.query("SELECT name, city FROM persons", &[]).unwrap() {
                let name: String = row.get(0);
                let city: String = row.get(1);
                assert_eq!("John Legend", name);
                assert_eq!("New York", city);
            }
        });
    }

    #[test]
    fn embedded_applies_migration_single_transaction() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();

            embedded::migrations::runner()
                .set_grouped(false)
                .run(&mut conn)
                .unwrap();

            conn.execute(
                "INSERT INTO persons (name, city) VALUES ($1, $2)",
                &[&"John Legend", &"New York"],
            )
            .unwrap();
            for row in &conn.query("SELECT name, city FROM persons", &[]).unwrap() {
                let name: String = row.get(0);
                let city: String = row.get(1);
                assert_eq!("John Legend", name);
                assert_eq!("New York", city);
            }
        });
    }

    #[test]
    fn embedded_updates_schema_history() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();

            embedded::migrations::runner().run(&mut conn).unwrap();

            for row in &conn
                .query("SELECT MAX(version) FROM refinery_schema_history", &[])
                .unwrap()
            {
                let current: i32 = row.get(0);
                assert_eq!(3, current);
            }

            for row in &conn
                .query("SELECT applied_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)", &[])
                .unwrap()
            {
                let applied_on: String = row.get(0);
                let applied_on = DateTime::parse_from_rfc3339(&applied_on).unwrap().with_timezone(&Local);
                assert_eq!(Local::today(), applied_on.date());
            }
        });
    }

    #[test]
    fn embedded_updates_schema_history_single_transaction() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();

            embedded::migrations::runner()
                .set_grouped(false)
                .run(&mut conn)
                .unwrap();

            for row in &conn
                .query("SELECT MAX(version) FROM refinery_schema_history", &[])
                .unwrap()
            {
                let current: i32 = row.get(0);
                assert_eq!(3, current);
            }

            for row in &conn
                .query("SELECT applied_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)", &[])
                .unwrap()
            {
                let applied_on: String = row.get(0);
                let applied_on = DateTime::parse_from_rfc3339(&applied_on).unwrap().with_timezone(&Local);
                assert_eq!(Local::today(), applied_on.date());
            }
        });
    }

    #[test]
    fn embedded_updates_to_last_working_if_not_grouped() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();

            let result = broken::migrations::runner().run(&mut conn);

            assert!(result.is_err());
            println!("CURRENT: {:?}", result);

            for row in &conn
                .query("SELECT MAX(version) FROM refinery_schema_history", &[])
                .unwrap()
            {
                let current: i32 = row.get(0);
                assert_eq!(2, current);
            }
        });
    }

    #[test]
    fn embedded_doesnt_update_to_last_working_if_grouped() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();

            let result = broken::migrations::runner()
                .set_grouped(true)
                .run(&mut conn);

            assert!(result.is_err());
            println!("CURRENT: {:?}", result);

            let query = &conn
                .query("SELECT version FROM refinery_schema_history", &[])
                .unwrap();
            assert!(query.is_empty());
        });
    }

    #[test]
    fn mod_creates_migration_table() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();
            mod_migrations::migrations::runner().run(&mut conn).unwrap();
            for row in &conn
                .query(
                    "SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'", &[]
                )
                .unwrap()
            {
                let table_name: String = row.get(0);
                assert_eq!("refinery_schema_history", table_name);
            }
        });
    }

    #[test]
    fn mod_applies_migration() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();

            mod_migrations::migrations::runner().run(&mut conn).unwrap();
            conn.execute(
                "INSERT INTO persons (name, city) VALUES ($1, $2)",
                &[&"John Legend", &"New York"],
            )
            .unwrap();
            for row in &conn.query("SELECT name, city FROM persons", &[]).unwrap() {
                let name: String = row.get(0);
                let city: String = row.get(1);
                assert_eq!("John Legend", name);
                assert_eq!("New York", city);
            }
        });
    }

    #[test]
    fn mod_updates_schema_history() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();

            mod_migrations::migrations::runner().run(&mut conn).unwrap();
            for row in &conn
                .query("SELECT MAX(version) FROM refinery_schema_history", &[])
                .unwrap()
            {
                let current: i32 = row.get(0);
                assert_eq!(3, current);
            }

            for row in &conn
                .query("SELECT applied_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)", &[])
                .unwrap()
            {
                let applied_on: String = row.get(0);
                let applied_on = DateTime::parse_from_rfc3339(&applied_on).unwrap().with_timezone(&Local);
                assert_eq!(Local::today(), applied_on.date());
            }
        });
    }

    #[test]
    fn applies_new_migration() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();

            embedded::migrations::runner().run(&mut conn).unwrap();
            let migration1 = Migration::from_filename(
                "V1__initial.sql",
                include_str!("./sql_migrations/V1__initial.sql"),
            )
            .unwrap();

            let migration2 = Migration::from_filename(
                "V2__add_cars_table",
                include_str!("./sql_migrations/V2__add_cars_table.sql"),
            )
            .unwrap();

            let migration3 = Migration::from_filename(
                "V3__add_brand_to_cars_table",
                include_str!("./sql_migrations/V3__add_brand_to_cars_table.sql"),
            )
            .unwrap();

            let migration4 = Migration::from_filename(
                "V4__add_year_field_to_cars",
                &"ALTER TABLE cars ADD year INTEGER;",
            )
            .unwrap();
            let mchecksum = migration4.checksum();
            conn.migrate(
                &[migration1, migration2, migration3, migration4],
                true,
                true,
                false,
            )
            .unwrap();

            for row in &conn
                .query("SELECT version, checksum FROM refinery_schema_history where version = (SELECT MAX(version) from refinery_schema_history)", &[])
                .unwrap()
            {
                let current: i32 = row.get(0);
                let checksum: String = row.get(1);
                assert_eq!(4, current);
                assert_eq!(mchecksum.to_string(), checksum);
            }
        });
    }

    #[test]
    fn aborts_on_missing_migration_on_filesystem() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();

            mod_migrations::migrations::runner().run(&mut conn).unwrap();

            let migration = Migration::from_filename(
                "V4__add_year_field_to_cars",
                &"ALTER TABLE cars ADD year INTEGER;",
            )
            .unwrap();
            let err = conn
                .migrate(&[migration.clone()], true, true, false)
                .unwrap_err();

            match err {
                Error::MissingVersion(missing) => {
                    assert_eq!(1, missing.version);
                    assert_eq!("initial", missing.name);
                }
                _ => panic!("failed test"),
            }
        });
    }

    #[test]
    fn aborts_on_divergent_migration() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();

            mod_migrations::migrations::runner().run(&mut conn).unwrap();

            let migration = Migration::from_filename(
                "V2__add_year_field_to_cars",
                &"ALTER TABLE cars ADD year INTEGER;",
            )
            .unwrap();
            let err = conn
                .migrate(&[migration.clone()], true, false, false)
                .unwrap_err();

            match err {
                Error::DivergentVersion(applied, divergent) => {
                    assert_eq!(migration, divergent);
                    assert_eq!(2, applied.version);
                    assert_eq!("add_cars_table", applied.name);
                }
                _ => panic!("failed test"),
            }
        });
    }

    #[test]
    fn aborts_on_missing_migration_on_database() {
        run_test(|| {
            let mut conn =
                Connection::connect("postgres://postgres@localhost:5432/postgres", TlsMode::None)
                    .unwrap();

            missing::migrations::runner().run(&mut conn).unwrap();

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
            let err = conn
                .migrate(&[migration1, migration2], true, true, false)
                .unwrap_err();
            match err {
                Error::MissingVersion(missing) => {
                    assert_eq!(1, missing.version);
                    assert_eq!("initial", missing.name);
                }
                _ => panic!("failed test"),
            }
        });
    }

    #[test]
    fn migrates_from_config() {
        run_test(|| {
            let config = Config::new(ConfigDbType::Postgres)
                .set_db_name("postgres")
                .set_db_user("postgres")
                .set_db_host("localhost")
                .set_db_port("5432");

            let migrations = get_migrations();
            migrate_from_config(&config, false, true, true, &migrations).unwrap();
        })
    }

    #[test]
    fn migrates_from_cli() {
        run_test(|| {
            Command::new("refinery")
                .args(&[
                    "migrate",
                    "-c",
                    "tests/postgres_refinery.toml",
                    "files",
                    "-p",
                    "tests/sql_migrations",
                ])
                .unwrap()
                .assert()
                .stdout(contains("applying migration: V3__add_brand_to_cars_table"));
        })
    }
}
