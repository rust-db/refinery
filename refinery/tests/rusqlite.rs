mod rusqlite {
    use assert_cmd::prelude::*;
    use chrono::{DateTime, Local};
    use predicates::str::contains;
    use refinery::{migrate_from_config, Config, ConfigDbType, Error, Migrate as _, Migration};
    use std::fs::{self, File};
    use std::process::Command;
    use ttrusqlite::{Connection, NO_PARAMS};

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

    fn run_test<T>(test: T) -> ()
    where
        T: FnOnce() -> () + std::panic::UnwindSafe,
    {
        let filepath = "tests/db.sql";
        File::create(filepath).unwrap();

        let result = std::panic::catch_unwind(|| test());

        fs::remove_file(filepath).unwrap();

        assert!(result.is_ok())
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

    #[test]
    fn embedded_creates_migration_table() {
        let mut conn = Connection::open_in_memory().unwrap();
        embedded::migrations::runner().run(&mut conn).unwrap();
        let table_name: String = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='refinery_schema_history'",
                NO_PARAMS,
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!("refinery_schema_history", table_name);
    }

    #[test]
    fn embedded_creates_migration_table_grouped_transaction() {
        let mut conn = Connection::open_in_memory().unwrap();
        embedded::migrations::runner()
            .set_grouped(true)
            .run(&mut conn)
            .unwrap();
        let table_name: String = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='refinery_schema_history'",
                NO_PARAMS,
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!("refinery_schema_history", table_name);
    }

    #[test]
    fn embedded_applies_migration() {
        let mut conn = Connection::open_in_memory().unwrap();

        embedded::migrations::runner().run(&mut conn).unwrap();

        conn.execute(
            "INSERT INTO persons (name, city) VALUES (?, ?)",
            &[&"John Legend", &"New York"],
        )
        .unwrap();
        let (name, city): (String, String) = conn
            .query_row("SELECT name, city FROM persons", NO_PARAMS, |row| {
                Ok((row.get(0).unwrap(), row.get(1).unwrap()))
            })
            .unwrap();
        assert_eq!("John Legend", name);
        assert_eq!("New York", city);
    }

    #[test]
    fn embedded_applies_migration_grouped_transaction() {
        let mut conn = Connection::open_in_memory().unwrap();

        embedded::migrations::runner()
            .set_grouped(true)
            .run(&mut conn)
            .unwrap();

        conn.execute(
            "INSERT INTO persons (name, city) VALUES (?, ?)",
            &[&"John Legend", &"New York"],
        )
        .unwrap();
        let (name, city): (String, String) = conn
            .query_row("SELECT name, city FROM persons", NO_PARAMS, |row| {
                Ok((row.get(0).unwrap(), row.get(1).unwrap()))
            })
            .unwrap();
        assert_eq!("John Legend", name);
        assert_eq!("New York", city);
    }

    #[test]
    fn embedded_updates_schema_history() {
        let mut conn = Connection::open_in_memory().unwrap();

        embedded::migrations::runner().run(&mut conn).unwrap();

        let current: u32 = conn
            .query_row(
                "SELECT MAX(version) FROM refinery_schema_history",
                NO_PARAMS,
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(3, current);

        let applied_on: DateTime<Local> = conn
            .query_row(
                "SELECT applied_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)",
                NO_PARAMS,
                |row| {
                    let applied_on: String = row.get(0).unwrap();
                    Ok(DateTime::parse_from_rfc3339(&applied_on).unwrap().with_timezone(&Local))
                }
            )
            .unwrap();
        assert_eq!(Local::today(), applied_on.date());
    }

    #[test]
    fn embedded_updates_schema_history_grouped_transaction() {
        let mut conn = Connection::open_in_memory().unwrap();

        embedded::migrations::runner()
            .set_grouped(true)
            .run(&mut conn)
            .unwrap();

        let current: u32 = conn
            .query_row(
                "SELECT MAX(version) FROM refinery_schema_history",
                NO_PARAMS,
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(3, current);

        let applied_on: DateTime<Local> = conn
            .query_row(
                "SELECT applied_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)",
                NO_PARAMS,
                |row| {
                    let applied_on: String = row.get(0).unwrap();
                    Ok(DateTime::parse_from_rfc3339(&applied_on).unwrap().with_timezone(&Local))
                }
            )
            .unwrap();
        assert_eq!(Local::today(), applied_on.date());
    }

    #[test]
    fn embedded_updates_to_last_working_in_multiple_transaction() {
        let mut conn = Connection::open_in_memory().unwrap();

        let result = broken::migrations::runner().run(&mut conn);

        assert!(result.is_err());
        let current: u32 = conn
            .query_row(
                "SELECT MAX(version) FROM refinery_schema_history",
                NO_PARAMS,
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(2, current);
    }

    #[test]
    fn mod_creates_migration_table() {
        let mut conn = Connection::open_in_memory().unwrap();
        mod_migrations::migrations::runner().run(&mut conn).unwrap();
        let table_name: String = conn
            .query_row(
                "SELECT name FROM sqlite_master WHERE type='table' AND name='refinery_schema_history'",
                NO_PARAMS,
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!("refinery_schema_history", table_name);
    }

    #[test]
    fn mod_applies_migration() {
        let mut conn = Connection::open_in_memory().unwrap();

        mod_migrations::migrations::runner().run(&mut conn).unwrap();

        conn.execute(
            "INSERT INTO persons (name, city) VALUES (?, ?)",
            &[&"John Legend", &"New York"],
        )
        .unwrap();
        let (name, city): (String, String) = conn
            .query_row("SELECT name, city FROM persons", NO_PARAMS, |row| {
                Ok((row.get(0).unwrap(), row.get(1).unwrap()))
            })
            .unwrap();
        assert_eq!("John Legend", name);
        assert_eq!("New York", city);
    }

    #[test]
    fn mod_updates_schema_history() {
        let mut conn = Connection::open_in_memory().unwrap();

        mod_migrations::migrations::runner().run(&mut conn).unwrap();

        let current: u32 = conn
            .query_row(
                "SELECT MAX(version) FROM refinery_schema_history",
                NO_PARAMS,
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(3, current);

        let applied_on: DateTime<Local> = conn
            .query_row(
                "SELECT applied_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)",
                NO_PARAMS,
                |row| {
                    let applied_on: String = row.get(0).unwrap();
                    Ok(DateTime::parse_from_rfc3339(&applied_on).unwrap().with_timezone(&Local))
                }
            )
            .unwrap();
        assert_eq!(Local::today(), applied_on.date());
    }

    #[test]
    fn applies_new_migration() {
        let mut conn = Connection::open_in_memory().unwrap();

        embedded::migrations::runner().run(&mut conn).unwrap();

        let migrations = get_migrations();

        let mchecksum = migrations[3].checksum();
        conn.migrate(&migrations, true, true).unwrap();

        let (current, checksum): (u32, String) = conn
            .query_row(
                "SELECT version, checksum FROM refinery_schema_history where version = (SELECT MAX(version) from refinery_schema_history)",
                NO_PARAMS,
                |row| Ok((row.get(0).unwrap(), row.get(1).unwrap())),
            )
            .unwrap();
        assert_eq!(4, current);
        assert_eq!(mchecksum.to_string(), checksum);
    }

    #[test]
    fn aborts_on_missing_migration_on_filesystem() {
        let mut conn = Connection::open_in_memory().unwrap();

        mod_migrations::migrations::runner().run(&mut conn).unwrap();

        let migration = Migration::from_filename(
            "V4__add_year_field_to_cars",
            &"ALTER TABLE cars ADD year INTEGER;",
        )
        .unwrap();
        let err = conn.migrate(&[migration.clone()], true, true).unwrap_err();

        match err {
            Error::MissingVersion(missing) => {
                assert_eq!(1, missing.version);
                assert_eq!("initial", missing.name);
            }
            _ => panic!("failed test"),
        }
    }

    #[test]
    fn aborts_on_divergent_migration() {
        let mut conn = Connection::open_in_memory().unwrap();

        mod_migrations::migrations::runner().run(&mut conn).unwrap();

        let migration = Migration::from_filename(
            "V2__add_year_field_to_cars",
            &"ALTER TABLE cars ADD year INTEGER;",
        )
        .unwrap();
        let err = conn.migrate(&[migration.clone()], true, false).unwrap_err();

        match err {
            Error::DivergentVersion(applied, divergent) => {
                assert_eq!(migration, divergent);
                assert_eq!(2, applied.version);
                assert_eq!("add_cars_table", applied.name);
            }
            _ => panic!("failed test"),
        }
    }

    #[test]
    fn aborts_on_missing_migration_on_database() {
        let mut conn = Connection::open_in_memory().unwrap();

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
            .migrate(&[migration1, migration2], true, true)
            .unwrap_err();
        match err {
            Error::MissingVersion(missing) => {
                assert_eq!(1, missing.version);
                assert_eq!("initial", missing.name);
            }
            _ => panic!("failed test"),
        }
    }

    #[test]
    fn migrates_from_config() {
        let _db = File::create("db.sql");
        let config = Config::new(ConfigDbType::Sqlite).set_db_path("db.sql");
        let migrations = get_migrations();
        migrate_from_config(&config, false, true, true, &migrations).unwrap();
        std::fs::remove_file("db.sql").unwrap();
    }

    #[test]
    fn migrates_from_cli() {
        run_test(|| {
            Command::cargo_bin("refinery")
                .unwrap()
                .args(&[
                    "migrate",
                    "-c",
                    "tests/sqlite_refinery.toml",
                    "files",
                    "-p",
                    "tests/sql_migrations",
                ])
                .assert()
                .stdout(contains("applying migration: V3__add_brand_to_cars_table"));
        })
    }
}
