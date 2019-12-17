mod mysql {
    use assert_cmd::prelude::*;
    use chrono::{DateTime, Local};
    use predicates::str::contains;
    use refinery::{migrate_from_config, Error, Migrate as _, Migration};
    use std::io::Write;
    use std::process::Command;
    use ttmysql as my;

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
        let mut conn = my::Conn::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();

        conn.prep_exec("DROP DATABASE refinery_test", ()).unwrap();
        conn.prep_exec("CREATE DATABASE refinery_test", ()).unwrap();
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
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();
            embedded::migrations::runner().run(&mut conn).unwrap();
            for row in conn
                .query(
                    "SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'"
                )
                .unwrap()
            {
                let table_name: String = row.unwrap().get(0).unwrap();
                assert_eq!("refinery_schema_history", table_name);
            }
        });
    }

    #[test]
    fn embedded_creates_migration_table_grouped_transaction() {
        run_test(|| {
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();
            embedded::migrations::runner()
                .set_grouped(false)
                .run(&mut conn)
                .unwrap();

            for row in conn
                .query(
                    "SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'"
                )
                .unwrap()
            {
                let table_name: String = row.unwrap().get(0).unwrap();
                assert_eq!("refinery_schema_history", table_name);
            }
        });
    }

    #[test]
    fn embedded_applies_migration() {
        run_test(|| {
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            embedded::migrations::runner().run(&mut conn).unwrap();
            conn.prep_exec(
                "INSERT INTO persons (name, city) VALUES (:a, :b)",
                (&"John Legend", &"New York"),
            )
            .unwrap();
            for _row in conn.query("SELECT name, city FROM persons").unwrap() {
                let row = _row.unwrap();
                let name: String = row.get(0).unwrap();
                let city: String = row.get(1).unwrap();
                assert_eq!("John Legend", name);
                assert_eq!("New York", city);
            }
        });
    }

    #[test]
    fn embedded_applies_migration_grouped_transaction() {
        run_test(|| {
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            embedded::migrations::runner()
                .set_grouped(false)
                .run(&mut conn)
                .unwrap();

            conn.prep_exec(
                "INSERT INTO persons (name, city) VALUES (:a, :b)",
                (&"John Legend", &"New York"),
            )
            .unwrap();
            for _row in conn.query("SELECT name, city FROM persons").unwrap() {
                let row = _row.unwrap();
                let name: String = row.get(0).unwrap();
                let city: String = row.get(1).unwrap();
                assert_eq!("John Legend", name);
                assert_eq!("New York", city);
            }
        });
    }

    #[test]
    fn embedded_updates_schema_history() {
        run_test(|| {
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            embedded::migrations::runner().run(&mut conn).unwrap();

            for _row in conn
                .query("SELECT MAX(version) FROM refinery_schema_history")
                .unwrap()
            {
                let row = _row.unwrap();
                let current: i32 = row.get(0).unwrap();
                assert_eq!(3, current);
            }

            for _row in conn
                .query("SELECT applied_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)")
                .unwrap()
            {
                let row = _row.unwrap();
                let applied_on: String = row.get(0).unwrap();
                let applied_on = DateTime::parse_from_rfc3339(&applied_on).unwrap().with_timezone(&Local);
                assert_eq!(Local::today(), applied_on.date());
            }
        });
    }

    #[test]
    fn embedded_updates_schema_history_grouped_transaction() {
        run_test(|| {
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            embedded::migrations::runner()
                .set_grouped(false)
                .run(&mut conn)
                .unwrap();

            for _row in conn
                .query("SELECT MAX(version) FROM refinery_schema_history")
                .unwrap()
            {
                let row = _row.unwrap();
                let current: i32 = row.get(0).unwrap();
                assert_eq!(3, current);
            }

            for _row in conn
                .query("SELECT applied_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)")
                .unwrap()
            {
                let row = _row.unwrap();
                let applied_on: String = row.get(0).unwrap();
                let applied_on = DateTime::parse_from_rfc3339(&applied_on).unwrap().with_timezone(&Local);
                assert_eq!(Local::today(), applied_on.date());
            }
        });
    }

    #[test]
    fn embedded_updates_to_last_working_if_not_grouped() {
        run_test(|| {
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            let result = broken::migrations::runner().run(&mut conn);

            assert!(result.is_err());

            for _row in conn
                .query("SELECT MAX(version) FROM refinery_schema_history")
                .unwrap()
            {
                let row = _row.unwrap();
                let current: i32 = row.get(0).unwrap();
                assert_eq!(2, current);
            }
        });
    }

    /// maintain this test still here for self referencing purposes, Mysql doesn't support well transactions
    /// TODO: maybe uncomment it one day when MySQL does :D
    // #[test]
    // fn embedded_doesnt_update_to_last_working_if_grouped_transaction() {
    //     // run_test(|| {
    //         let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
    //         let mut conn = pool.get_conn().unwrap();

    //         let result = broken::migrations::runner().set_grouped(true).run(&mut conn);

    //         assert!(result.is_err());

    //         let mut query = conn
    //             .query("SELECT version FROM refinery_schema_history")
    //             .unwrap();
    //         let row = query.next();
    //         dbg!(&row);
    //         assert!(row.is_none());
    //         // let value: Option<i32> = row.get(0);
    //         // assert_eq!(0, value.unwrap());
    //     // });
    // }

    #[test]
    fn mod_creates_migration_table() {
        run_test(|| {
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();
            mod_migrations::migrations::runner().run(&mut conn).unwrap();
            for row in conn
                .query(
                    "SELECT table_name FROM information_schema.tables WHERE table_name='refinery_schema_history'"
                )
                .unwrap()
            {
                let table_name: String = row.unwrap().get(0).unwrap();
                assert_eq!("refinery_schema_history", table_name);
            }
        });
    }

    #[test]
    fn mod_applies_migration() {
        run_test(|| {
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            mod_migrations::migrations::runner().run(&mut conn).unwrap();
            conn.prep_exec(
                "INSERT INTO persons (name, city) VALUES (:a, :b)",
                (&"John Legend", &"New York"),
            )
            .unwrap();
            for _row in conn.query("SELECT name, city FROM persons").unwrap() {
                let row = _row.unwrap();
                let name: String = row.get(0).unwrap();
                let city: String = row.get(1).unwrap();
                assert_eq!("John Legend", name);
                assert_eq!("New York", city);
            }
        });
    }

    #[test]
    fn mod_updates_schema_history() {
        run_test(|| {
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            mod_migrations::migrations::runner().run(&mut conn).unwrap();

            for _row in conn
                .query("SELECT MAX(version) FROM refinery_schema_history")
                .unwrap()
            {
                let row = _row.unwrap();
                let current: i32 = row.get(0).unwrap();
                assert_eq!(3, current);
            }

            for _row in conn
                .query("SELECT applied_on FROM refinery_schema_history where version=(SELECT MAX(version) from refinery_schema_history)")
                .unwrap()
            {
                let row = _row.unwrap();
                let applied_on: String = row.get(0).unwrap();
                let applied_on = DateTime::parse_from_rfc3339(&applied_on).unwrap().with_timezone(&Local);
                assert_eq!(Local::today(), applied_on.date());
            }
        });
    }

    #[test]
    fn applies_new_migration() {
        run_test(|| {
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            embedded::migrations::runner().run(&mut conn).unwrap();
            let migrations = get_migrations();

            let mchecksum = migrations[3].checksum();
            conn.migrate(&migrations, true, true, false).unwrap();

            for _row in conn
                .query("SELECT version, checksum FROM refinery_schema_history where version = (SELECT MAX(version) from refinery_schema_history)")
                .unwrap()
            {
                let row = _row.unwrap();
                let current: i32 = row.get(0).unwrap();
                let checksum: String = row.get(1).unwrap();
                assert_eq!(4, current);
                assert_eq!(mchecksum.to_string(), checksum);
            }
        });
    }

    #[test]
    fn aborts_on_missing_migration_on_filesystem() {
        run_test(|| {
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

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
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

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
            let pool = my::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

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
            let config = "[main] \n
                     db_type = \"Mysql\" \n
                     db_name = \"refinery_test\" \n
                     db_user = \"refinery\" \n
                     db_pass= \"root\" \n
                     db_host = \"localhost\" \n
                     db_port = \"3306\" ";

            let mut config_file = tempfile::NamedTempFile::new_in(".").unwrap();
            config_file.write_all(config.as_bytes()).unwrap();

            let migrations = get_migrations();
            migrate_from_config(config_file.path(), false, true, true, &migrations).unwrap();
        })
    }

    #[test]
    fn migrates_from_cli() {
        run_test(|| {
            Command::cargo_bin("refinery")
                .unwrap()
                .args(&[
                    "migrate",
                    "-c",
                    "tests/mysql_refinery.toml",
                    "files",
                    "-p",
                    "tests/sql_migrations",
                ])
                .assert()
                .stdout(contains("applying migration: V3__add_brand_to_cars_table"));
        })
    }
}
