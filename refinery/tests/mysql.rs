use barrel::backend::MySql as Sql;
mod mod_migrations;

#[cfg(feature = "mysql")]
mod mysql {
    use super::mod_migrations;
    use assert_cmd::prelude::*;
    use chrono::Local;
    use predicates::str::contains;
    use refinery::{
        config::{Config, ConfigDbType},
        error::Kind,
        Migrate, Migration, Runner, Target,
    };
    use refinery_core::mysql;
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

    fn clean_database() {
        let mut conn =
            mysql::Conn::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();

        conn.prep_exec("DROP DATABASE refinery_test", ()).unwrap();
        conn.prep_exec("CREATE DATABASE refinery_test", ()).unwrap();
    }

    fn run_test<T>(test: T)
    where
        T: FnOnce() + std::panic::UnwindSafe,
    {
        let result = std::panic::catch_unwind(|| test());

        clean_database();

        assert!(result.is_ok())
    }

    #[test]
    fn report_contains_applied_migrations() {
        run_test(|| {
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();
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
        });
    }

    #[test]
    fn embedded_creates_migration_table() {
        run_test(|| {
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
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
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
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
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
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
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
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
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            embedded::migrations::runner().run(&mut conn).unwrap();
            let current = conn.get_last_applied_migration().unwrap().unwrap();

            assert_eq!(4, current.version());
            assert_eq!(Local::today(), current.applied_on().unwrap().date());
        });
    }

    #[test]
    fn embedded_updates_schema_history_grouped_transaction() {
        run_test(|| {
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            embedded::migrations::runner()
                .set_grouped(false)
                .run(&mut conn)
                .unwrap();

            embedded::migrations::runner().run(&mut conn).unwrap();
            let current = conn.get_last_applied_migration().unwrap().unwrap();

            assert_eq!(4, current.version());
            assert_eq!(Local::today(), current.applied_on().unwrap().date());
        });
    }

    #[test]
    fn embedded_updates_to_last_working_if_not_grouped() {
        run_test(|| {
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

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
        });
    }

    /// maintain this test still here for self referencing purposes, Mysql doesn't support well transactions
    /// TODO: maybe uncomment it one day when MySQL does :D
    // #[test]
    // fn embedded_doesnt_update_to_last_working_if_grouped_transaction() {
    //     // run_test(|| {
    //         let pool = mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
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
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();
            mod_migrations::runner().run(&mut conn).unwrap();
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
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            mod_migrations::runner().run(&mut conn).unwrap();
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
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            mod_migrations::runner().run(&mut conn).unwrap();

            let current = conn.get_last_applied_migration().unwrap().unwrap();

            assert_eq!(4, current.version());
            assert_eq!(Local::today(), current.applied_on().unwrap().date());
        });
    }

    #[test]
    fn gets_applied_migrations() {
        run_test(|| {
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

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
        });
    }

    #[test]
    fn applies_new_migration() {
        run_test(|| {
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

            embedded::migrations::runner().run(&mut conn).unwrap();
            let migrations = get_migrations();

            let mchecksum = migrations[4].checksum();
            conn.migrate(&migrations, true, true, false, Target::Latest)
                .unwrap();

            let current = conn.get_last_applied_migration().unwrap().unwrap();

            assert_eq!(5, current.version());
            assert_eq!(mchecksum, current.checksum());
        });
    }

    #[test]
    fn migrates_to_target_migration() {
        run_test(|| {
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

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
        });
    }

    #[test]
    fn migrates_to_target_migration_grouped() {
        run_test(|| {
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

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
        });
    }

    #[test]
    fn aborts_on_missing_migration_on_filesystem() {
        run_test(|| {
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

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
        });
    }

    #[test]
    fn aborts_on_divergent_migration() {
        run_test(|| {
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

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
        });
    }

    #[test]
    fn aborts_on_missing_migration_on_database() {
        run_test(|| {
            let pool =
                mysql::Pool::new("mysql://refinery:root@localhost:3306/refinery_test").unwrap();
            let mut conn = pool.get_conn().unwrap();

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
        });
    }

    #[test]
    fn migrates_from_config() {
        run_test(|| {
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
        })
    }

    #[test]
    fn migrate_from_config_report_contains_migrations() {
        run_test(|| {
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
        })
    }

    #[test]
    fn migrate_from_config_report_returns_last_applied_migration() {
        run_test(|| {
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

            runner.run(&mut config).unwrap();

            let applied_migration = runner
                .get_last_applied_migration(&mut config)
                .unwrap()
                .unwrap();
            assert_eq!(5, applied_migration.version());

            assert_eq!(migrations[4].version(), applied_migration.version());
            assert_eq!(migrations[4].name(), applied_migration.name());
            assert_eq!(migrations[4].checksum(), applied_migration.checksum());
        })
    }

    #[test]
    fn migrates_from_cli() {
        run_test(|| {
            Command::new("refinery")
                .args(&[
                    "migrate",
                    "-c",
                    "tests/mysql_refinery.toml",
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
