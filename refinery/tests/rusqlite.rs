use barrel::backend::Sqlite as Sql;
mod mod_migrations;

#[cfg(feature = "rusqlite")]
mod rusqlite {
    use super::mod_migrations;
    use assert_cmd::prelude::*;
    use chrono::Local;
    use predicates::str::contains;
    use refinery::{
        config::{Config, ConfigDbType},
        error::Kind,
        Migrate, Migration, Runner, Target,
    };
    use refinery_core::rusqlite::{Connection, OptionalExtension, NO_PARAMS};
    use std::fs::{self, File};
    use std::process::Command;

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

    fn run_test<T>(test: T)
    where
        T: FnOnce() + std::panic::UnwindSafe,
    {
        let filepath = "tests/db.sql";
        File::create(filepath).unwrap();

        let result = std::panic::catch_unwind(|| test());

        fs::remove_file(filepath).unwrap();

        assert!(result.is_ok())
    }

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

    #[test]
    fn report_contains_applied_migrations() {
        let mut conn = Connection::open_in_memory().unwrap();
        let report = embedded::migrations::runner().run(&mut conn).unwrap();

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

        let current = conn.get_last_applied_migration().unwrap().unwrap();

        assert_eq!(4, current.version());

        assert_eq!(Local::today(), current.applied_on().unwrap().date());
    }

    #[test]
    fn embedded_updates_schema_history_grouped_transaction() {
        let mut conn = Connection::open_in_memory().unwrap();

        embedded::migrations::runner()
            .set_grouped(true)
            .run(&mut conn)
            .unwrap();

        let current = conn.get_last_applied_migration().unwrap().unwrap();

        assert_eq!(4, current.version());

        assert_eq!(Local::today(), current.applied_on().unwrap().date());
    }

    #[test]
    fn embedded_updates_to_last_working_if_not_grouped() {
        let mut conn = Connection::open_in_memory().unwrap();

        let result = broken::migrations::runner().run(&mut conn);

        assert!(result.is_err());
        let current = conn.get_last_applied_migration().unwrap().unwrap();

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
    }

    #[test]
    fn embedded_doesnt_update_to_last_working_if_grouped() {
        let mut conn = Connection::open_in_memory().unwrap();

        let result = broken::migrations::runner()
            .set_grouped(true)
            .run(&mut conn);

        assert!(result.is_err());
        let query: Option<u32> = conn
            .query_row(
                "SELECT version FROM refinery_schema_history",
                NO_PARAMS,
                |row| row.get(0),
            )
            .optional()
            .unwrap();
        assert!(query.is_none());
    }

    #[test]
    fn mod_creates_migration_table() {
        let mut conn = Connection::open_in_memory().unwrap();
        mod_migrations::runner().run(&mut conn).unwrap();
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

        mod_migrations::runner().run(&mut conn).unwrap();

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

        mod_migrations::runner().run(&mut conn).unwrap();

        let current = conn.get_last_applied_migration().unwrap().unwrap();

        assert_eq!(4, current.version());

        assert_eq!(Local::today(), current.applied_on().unwrap().date());
    }

    #[test]
    fn gets_applied_migrations() {
        let mut conn = Connection::open_in_memory().unwrap();

        embedded::migrations::runner().run(&mut conn).unwrap();

        let migrations = get_migrations();
        let applied_migrations = conn.get_applied_migrations().unwrap();
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
    }

    #[test]
    fn applies_new_migration() {
        let mut conn = Connection::open_in_memory().unwrap();

        embedded::migrations::runner().run(&mut conn).unwrap();

        let migrations = get_migrations();

        let mchecksum = migrations[4].checksum();
        conn.migrate(&migrations, true, true, false, Target::Latest)
            .unwrap();

        let current = conn.get_last_applied_migration().unwrap().unwrap();

        assert_eq!(5, current.version());
        assert_eq!(mchecksum, current.checksum());
    }

    #[test]
    fn migrates_to_target_migration() {
        let mut conn = Connection::open_in_memory().unwrap();

        let report = embedded::migrations::runner()
            .set_target(Target::Version(3))
            .run(&mut conn)
            .unwrap();

        let current = conn.get_last_applied_migration().unwrap().unwrap();

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
    }

    #[test]
    fn migrates_to_target_migration_grouped() {
        let mut conn = Connection::open_in_memory().unwrap();

        let report = embedded::migrations::runner()
            .set_target(Target::Version(3))
            .set_grouped(true)
            .run(&mut conn)
            .unwrap();

        let current = conn.get_last_applied_migration().unwrap().unwrap();

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
    }

    #[test]
    fn aborts_on_missing_migration_on_filesystem() {
        let mut conn = Connection::open_in_memory().unwrap();

        mod_migrations::runner().run(&mut conn).unwrap();

        let migration = Migration::unapplied(
            "V4__add_year_field_to_cars",
            &"ALTER TABLE cars ADD year INTEGER;",
        )
        .unwrap();
        let err = conn
            .migrate(&[migration], true, true, false, Target::Latest)
            .unwrap_err();

        match err.kind() {
            Kind::MissingVersion(missing) => {
                assert_eq!(1, missing.version());
                assert_eq!("initial", missing.name());
            }
            _ => panic!("failed test"),
        }
    }

    #[test]
    fn aborts_on_divergent_migration() {
        let mut conn = Connection::open_in_memory().unwrap();

        mod_migrations::runner().run(&mut conn).unwrap();

        let migration = Migration::unapplied(
            "V2__add_year_field_to_cars",
            &"ALTER TABLE cars ADD year INTEGER;",
        )
        .unwrap();
        let err = conn
            .migrate(&[migration.clone()], true, false, false, Target::Latest)
            .unwrap_err();

        match err.kind() {
            Kind::DivergentVersion(applied, divergent) => {
                assert_eq!(&migration, divergent);
                assert_eq!(2, applied.version());
                assert_eq!("add_cars_table", applied.name());
            }
            _ => panic!("failed test"),
        }
    }

    #[test]
    fn aborts_on_missing_migration_on_database() {
        let mut conn = Connection::open_in_memory().unwrap();

        missing::migrations::runner().run(&mut conn).unwrap();

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
        let err = conn
            .migrate(&[migration1, migration2], true, true, false, Target::Latest)
            .unwrap_err();
        match err.kind() {
            Kind::MissingVersion(missing) => {
                assert_eq!(1, missing.version());
                assert_eq!("initial", missing.name());
            }
            _ => panic!("failed test"),
        }
    }

    #[test]
    fn migrates_from_config() {
        let db = tempfile::NamedTempFile::new_in(".").unwrap();
        let mut config = Config::new(ConfigDbType::Sqlite).set_db_path(db.path().to_str().unwrap());

        let migrations = get_migrations();
        let runner = Runner::new(&migrations)
            .set_grouped(false)
            .set_abort_divergent(true)
            .set_abort_missing(true);

        runner.run(&mut config).unwrap();

        let applied_migrations = runner.get_applied_migrations(&mut config).unwrap();
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
    }

    #[test]
    fn migrate_from_config_report_contains_migrations() {
        let db = tempfile::NamedTempFile::new_in(".").unwrap();
        let mut config = Config::new(ConfigDbType::Sqlite).set_db_path(db.path().to_str().unwrap());

        let migrations = get_migrations();
        let runner = Runner::new(&migrations)
            .set_grouped(false)
            .set_abort_divergent(true)
            .set_abort_missing(true);

        let report = runner.run(&mut config).unwrap();

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
    }

    #[test]
    fn migrate_from_config_report_returns_last_applied_migration() {
        let db = tempfile::NamedTempFile::new_in(".").unwrap();
        let mut config = Config::new(ConfigDbType::Sqlite).set_db_path(db.path().to_str().unwrap());

        let migrations = get_migrations();
        let runner = Runner::new(&migrations)
            .set_grouped(false)
            .set_abort_divergent(true)
            .set_abort_missing(true);

        runner.run(&mut config).unwrap();

        let applied_migration = runner
            .get_last_applied_migration(&mut config)
            .unwrap()
            .unwrap();
        assert_eq!(5, applied_migration.version());

        assert_eq!(migrations[4].version(), applied_migration.version());
        assert_eq!(migrations[4].name(), applied_migration.name());
        assert_eq!(migrations[4].checksum(), applied_migration.checksum());
    }

    #[test]
    fn migrates_from_cli() {
        run_test(|| {
            Command::new("refinery")
                .args(&[
                    "migrate",
                    "-c",
                    "tests/sqlite_refinery.toml",
                    "files",
                    "-p",
                    "tests/sql_migrations",
                ])
                .unwrap()
                .assert()
                .stdout(contains("applying migration: V4__add_year_to_motos_table"));
        })
    }
}
