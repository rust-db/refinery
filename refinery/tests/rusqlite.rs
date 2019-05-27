mod rusqlite {
    use chrono::{DateTime, Local};
    use refinery::{Migrate as _, Migration};
    use ttrusqlite::{Connection, NO_PARAMS};

    mod embedded {
        use refinery::embed_migrations;
        embed_migrations!("refinery/tests/sql_migrations");
    }

    mod broken {
        use refinery::embed_migrations;
        embed_migrations!("refinery/tests/sql_migrations_broken");
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
    fn embedded_creates_migration_table_single_transaction() {
        let mut conn = Connection::open_in_memory().unwrap();
        embedded::migrations::runner()
            .set_grouped(false)
            .run(&mut conn).unwrap();
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
    fn embedded_applies_migration_single_transaction() {
        let mut conn = Connection::open_in_memory().unwrap();

        embedded::migrations::runner()
            .set_grouped(false)
            .run(&mut conn).unwrap();

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

        let installed_on: DateTime<Local> = conn
            .query_row(
                "SELECT installed_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)",
                NO_PARAMS,
                |row| {
                    let _installed_on: String = row.get(0).unwrap();
                    Ok(DateTime::parse_from_rfc3339(&_installed_on).unwrap().with_timezone(&Local))
                }
            )
            .unwrap();
        assert_eq!(Local::today(), installed_on.date());
    }

    #[test]
    fn embedded_updates_schema_history_single_transaction() {
        let mut conn = Connection::open_in_memory().unwrap();

        embedded::migrations::runner()
            .set_grouped(false)
            .run(&mut conn).unwrap();

        let current: u32 = conn
            .query_row(
                "SELECT MAX(version) FROM refinery_schema_history",
                NO_PARAMS,
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(3, current);

        let installed_on: DateTime<Local> = conn
            .query_row(
                "SELECT installed_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)",
                NO_PARAMS,
                |row| {
                    let _installed_on: String = row.get(0).unwrap();
                    Ok(DateTime::parse_from_rfc3339(&_installed_on).unwrap().with_timezone(&Local))
                }
            )
            .unwrap();
        assert_eq!(Local::today(), installed_on.date());
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

        let installed_on: DateTime<Local> = conn
            .query_row(
                "SELECT installed_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)",
                NO_PARAMS,
                |row| {
                    let _installed_on: String = row.get(0).unwrap();
                    Ok(DateTime::parse_from_rfc3339(&_installed_on).unwrap().with_timezone(&Local))
                }
            )
            .unwrap();
        assert_eq!(Local::today(), installed_on.date());
    }

    #[test]
    fn applies_new_migration() {
        let mut conn = Connection::open_in_memory().unwrap();

        mod_migrations::migrations::runner().run(&mut conn).unwrap();

        let migration = Migration::from_filename(
            "V4__add_year_field_to_cars",
            &"ALTER TABLE cars ADD year INTEGER;",
        )
        .unwrap();
        let mchecksum = migration.checksum();
        conn.migrate(&[migration]).unwrap();

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
}
