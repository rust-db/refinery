use crate::Error;
use cfg_if::cfg_if;
use regex::Regex;
use std::ffi::OsStr;
use std::path::Path;
use std::{env, fs};
use walkdir::{DirEntry, WalkDir};

// regex used to match file names
pub fn file_match_re() -> Regex {
    Regex::new(r"^([V])([\d|\.]+)__(\w+)").unwrap()
}

lazy_static::lazy_static! {
    pub static ref RE: regex::Regex = file_match_re();
}

pub enum MigrationType {
    Mod,
    Sql,
}

// finds migrations file names given a type and optional parent location, use by refinery-macros and refinery-cli
pub fn find_migrations_filenames(
    location: Option<&Path>,
    mtype: MigrationType,
    full: bool,
) -> Result<Vec<String>, Error> {
    //if parent dir was provided start from it, if not start from current dir
    let start = match location {
        Some(location) => fs::canonicalize(location)
            .map_err(|err| Error::InvalidMigrationPath(location.to_path_buf(), err))?,
        None => env::current_dir().map_err(|err| {
            Error::InvalidMigrationPath(fs::canonicalize("./").unwrap_or_default(), err)
        })?,
    };

    let file_paths = WalkDir::new(start.as_path())
        .into_iter()
        .filter_map(Result::ok)
        .map(DirEntry::into_path)
        .filter(|entry| {
            entry
                .parent()
                .filter(|parent| {
                    //if parent_dir was not provided check if file is on a migrations dir
                    location.is_some() || parent.ends_with("migrations")
                })
                .is_some()
        })
        //filter entries which match the valid migration regex
        .filter(|entry| {
            entry
                .file_name()
                .and_then(OsStr::to_str)
                .filter(|path| RE.is_match(path))
                .is_some()
        })
        //match the right extension
        .filter(|entry| {
            entry
                .extension()
                .and_then(OsStr::to_str)
                .filter(|ext| match mtype {
                    MigrationType::Mod => ext == &"rs",
                    MigrationType::Sql => ext == &"sql",
                })
                .is_some()
        })
        //if full is false get the name of the file without extension
        .filter_map(|entry| {
            if full {
                entry.into_os_string().into_string().ok()
            } else {
                entry
                    .file_stem()
                    .and_then(|file| file.to_os_string().into_string().ok())
            }
        })
        .collect();
    Ok(file_paths)
}

//migrates given a config file, function is defined here instead of refinery_cli because of rust trait constrains
cfg_if! {
    if #[cfg(all(feature = "mysql", feature = "postgres", feature = "rusqlite"))] {
        use crate::{Config, ConfigDbType, Migration, WrapMigrationError, Runner};
        use rusqlite::{Connection as RqlConnection, OpenFlags};
        use postgres::{Connection as PgConnection, TlsMode};
        use mysql::Conn as MysqlConnection;


        fn build_db_url(name: &str, config: &Config) -> String {
            let mut url: String = name.to_string() + "://";

            if let Some(user) = &config.main.db_user {
                url = url + &user;
            }
            if let Some(pass) = &config.main.db_pass {
                url = url + ":" + &pass;
            }
            if let Some(host) = &config.main.db_host {
                if config.main.db_user.is_some() {
                    url = url + "@" + &host;
                } else {
                    url = url + &host;
                }
            }
            if let Some(port) = &config.main.db_port {
               url = url + ":" + &port;
            }
            if let Some(name) = &config.main.db_name {
                url = url + "/" + name;
            }
            url
        }

        pub fn migrate_from_config(config: &Config, grouped: bool, divergent: bool, missing: bool, migrations: &[Migration]) -> Result<(), Error> {
            match config.main.db_type {
                ConfigDbType::Mysql => {
                    let url = build_db_url("mysql", config);
                    let mut connection = MysqlConnection::new(&url).migration_err("could not connect to database")?;
                    Runner::new(migrations).set_grouped(grouped).set_abort_divergent(divergent).set_abort_missing(missing).run(&mut connection)?;
                }
                ConfigDbType::Sqlite => {
                    //may have been checked earlier on config parsing, even if not let it fail with a Rusqlite db file not found error
                    let path = config.main.db_path.as_ref().cloned().unwrap_or_default();
                    let mut connection = RqlConnection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE).migration_err("could not open database")?;
                    Runner::new(migrations).set_grouped(grouped).set_abort_divergent(divergent).set_abort_missing(missing).run(&mut connection)?;
                }
                ConfigDbType::Postgres => {
                    let path = build_db_url("postgresql", config);
                    let mut connection = PgConnection::connect(path.as_str(), TlsMode::None).migration_err("could not connect to database")?;
                    Runner::new(migrations).set_grouped(grouped).set_abort_divergent(divergent).set_abort_missing(missing).run(&mut connection)?;
                }
            }
            Ok(())
        }
    } else {}
}

#[cfg(test)]
mod tests {
    use super::{find_migrations_filenames, MigrationType};
    use std::fs;
    use std::path::Path;
    use tempdir::TempDir;

    #[test]
    fn finds_mod_migrations() {
        let tmp_dir = TempDir::new_in(".", "refinery").unwrap();
        let _migrations_dir = fs::create_dir(tmp_dir.path().join("migrations")).unwrap();
        let mod1 = tmp_dir.path().join("migrations/V1__first.rs");
        fs::File::create(&mod1).unwrap();
        let mod2 = tmp_dir.path().join("migrations/V2__second.rs");
        fs::File::create(&mod2).unwrap();

        let mut mods = find_migrations_filenames(None, MigrationType::Mod, false).unwrap();
        mods.sort();
        assert_eq!("V1__first", mods[0]);
        assert_eq!("V2__second", mods[1]);
    }

    #[test]
    fn finds_mod_migrations_in_parent_dir() {
        let tmp_dir = TempDir::new_in(".", "refinery").unwrap();
        let mod1 = tmp_dir.path().join("V1__first.rs");
        fs::File::create(&mod1).unwrap();
        let mod2 = tmp_dir.path().join("V2__second.rs");
        fs::File::create(&mod2).unwrap();

        let mut mods =
            find_migrations_filenames(Some(tmp_dir.path()), MigrationType::Mod, false).unwrap();
        mods.sort();
        assert_eq!("V1__first", mods[0]);
        assert_eq!("V2__second", mods[1]);
    }

    #[test]
    fn ignores_mod_files_without_migration_regex_match() {
        let tmp_dir = TempDir::new_in(".", "refinery").unwrap();
        let _migrations_dir = fs::create_dir(tmp_dir.path().join("migrations")).unwrap();
        let mod1 = tmp_dir.path().join("migrations/V1first.rs");
        fs::File::create(&mod1).unwrap();
        let mod2 = tmp_dir.path().join("migrations/V2second.rs");
        fs::File::create(&mod2).unwrap();

        let mods =
            find_migrations_filenames(Some(tmp_dir.path()), MigrationType::Mod, false).unwrap();
        assert!(mods.is_empty());
    }

    #[test]
    fn finds_sql_migrations() {
        let tmp_dir = TempDir::new_in("./", "refinery").unwrap();
        let _migrations_dir = fs::create_dir(tmp_dir.path().join("migrations")).unwrap();
        let sql1 = tmp_dir.path().join("migrations/V1__first.sql");
        fs::File::create(&sql1).unwrap();
        let sql2 = tmp_dir.path().join("migrations/V2__second.sql");
        fs::File::create(&sql2).unwrap();

        let mut mods = find_migrations_filenames(None, MigrationType::Sql, true).unwrap();
        mods.sort();
        assert_eq!(sql1, Path::new(&mods[0]));
        assert_eq!(sql2, Path::new(&mods[1]));
    }

    #[test]
    fn finds_sql_migrations_in_parent_dir() {
        let tmp_dir = TempDir::new_in("./", "refinery").unwrap();
        let sql1 = tmp_dir.path().join("V1__first.sql");
        fs::File::create(&sql1).unwrap();
        let sql2 = tmp_dir.path().join("V2__second.sql");
        fs::File::create(&sql2).unwrap();

        let mut mods =
            find_migrations_filenames(Some(tmp_dir.path()), MigrationType::Sql, true).unwrap();
        mods.sort();
        assert_eq!(sql1, Path::new(&mods[0]));
        assert_eq!(sql2, Path::new(&mods[1]));
    }

    #[test]
    fn ignores_sql_files_without_migration_regex_match() {
        let tmp_dir = TempDir::new("refinery").unwrap();
        let _migrations_dir = fs::create_dir(tmp_dir.path().join("migrations")).unwrap();
        let sql1 = tmp_dir.path().join("migrations/V1first.sql");
        fs::File::create(&sql1).unwrap();
        let sql2 = tmp_dir.path().join("migrations/V2second.sql");
        fs::File::create(&sql2).unwrap();

        let mods =
            find_migrations_filenames(Some(tmp_dir.path()), MigrationType::Sql, true).unwrap();
        assert!(mods.is_empty());
    }
}
