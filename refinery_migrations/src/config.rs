use crate::{Error, Migration, Runner, WrapMigrationError};
#[cfg(feature = "mysql")]
use mysql::Conn as MysqlConnection;
#[cfg(feature = "postgres")]
use postgres::{Connection as PgConnection, TlsMode};
#[cfg(feature = "rusqlite")]
use rusqlite::{Connection as RqlConnection, OpenFlags};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::Path;

//refinery config file used by migrate_from_config
#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    pub main: Main,
}

#[derive(Serialize, Deserialize, PartialEq, Debug)]
pub enum ConfigDbType {
    Mysql,
    Postgres,
    Sqlite,
}

impl Config {
    pub fn new(db_type: ConfigDbType) -> Config {
        Config {
            main: Main {
                db_type,
                db_path: None,
                db_host: None,
                db_port: None,
                db_user: None,
                db_pass: None,
                db_name: None,
            },
        }
    }

    pub fn set_db_user(self, db_user: &str) -> Config {
        Config {
            main: Main {
                db_user: Some(db_user.into()),
                ..self.main
            },
        }
    }

    pub fn set_db_pass(self, db_pass: &str) -> Config {
        Config {
            main: Main {
                db_pass: Some(db_pass.into()),
                ..self.main
            },
        }
    }

    pub fn set_db_path(self, db_path: &str) -> Config {
        Config {
            main: Main {
                db_path: Some(db_path.into()),
                ..self.main
            },
        }
    }

    pub fn set_db_host(self, db_host: &str) -> Config {
        Config {
            main: Main {
                db_host: Some(db_host.into()),
                ..self.main
            },
        }
    }

    pub fn set_db_port(self, db_port: &str) -> Config {
        Config {
            main: Main {
                db_port: Some(db_port.into()),
                ..self.main
            },
        }
    }

    pub fn set_db_name(self, db_name: &str) -> Config {
        Config {
            main: Main {
                db_name: Some(db_name.into()),
                ..self.main
            },
        }
    }
}

#[derive(Serialize, Deserialize, Debug)]
pub struct Main {
    pub db_type: ConfigDbType,
    pub db_path: Option<String>,
    pub db_host: Option<String>,
    pub db_port: Option<String>,
    pub db_user: Option<String>,
    pub db_pass: Option<String>,
    pub db_name: Option<String>,
}

#[cfg(feature = "sync")]
fn parse_config<T: AsRef<Path>>(location: T) -> Result<Config, Error> {
    let file = std::fs::read_to_string(&location)
        .map_err(|err| Error::ConfigError(format!("could not open config file, {}", err)))?;

    let mut config: Config = toml::from_str(&file)
        .map_err(|err| Error::ConfigError(format!("could not parse config file, {}", err)))?;

    //replace relative path with canonical path in case of Sqlite db
    if config.main.db_type == ConfigDbType::Sqlite {
        let config_db_path = config.main.db_path.ok_or_else(|| {
            Error::ConfigError("field path must be present for Sqlite database type".into())
        })?;
        let mut config_db_path = Path::new(&config_db_path).to_path_buf();

        if config_db_path.is_relative() {
            let mut config_db_dir = location
                .as_ref()
                .parent()
                //safe to call unwrap in the below cases as the current dir exists and if config was opened there are permissions on the current dir
                .unwrap_or(&std::env::current_dir().unwrap())
                .to_path_buf();

            config_db_dir = fs::canonicalize(config_db_dir).unwrap();
            config_db_path = config_db_dir.join(&config_db_path)
        }

        let config_db_path = config_db_path.into_os_string().into_string().map_err(|_| {
            Error::ConfigError("sqlite db file location must be a valid utf-8 string".into())
        })?;
        config.main.db_path = Some(config_db_path);
    }

    Ok(config)
}

#[cfg(any(feature = "mysql", feature = "postgres"))]
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

/// migrates from a given config file location
/// use this function if you prefer to generate a config file either from refinery_cli or by hand,
/// and migrate without having to pass a database Connection
/// # Panics
///
/// This function panics if refinery was not built with database driver support for the target database,
/// eg trying to migrate a PostgresSQL without feature postgres enabled.
#[cfg(feature = "sync")]
pub fn migrate_from_config<T: AsRef<Path>>(
    config_location: T,
    grouped: bool,
    divergent: bool,
    missing: bool,
    migrations: &[Migration],
) -> Result<(), Error> {
    let config = parse_config(config_location)?;

    match config.main.db_type {
        ConfigDbType::Mysql => {
            cfg_if::cfg_if! {
                if #[cfg(feature = "mysql")] {
                    let url = build_db_url("mysql", &config);
                    let mut connection = MysqlConnection::new(&url).migration_err("could not connect to database")?;
                    Runner::new(migrations).set_grouped(grouped).set_abort_divergent(divergent).set_abort_missing(missing).run(&mut connection)?;
                } else {
                    panic!("tried to migrate from config for a mysql database, but feature mysql not enabled!");
                }
            }
        }
        ConfigDbType::Sqlite => {
            cfg_if::cfg_if! {
                if #[cfg(feature = "rusqlite")] {
                    //may have been checked earlier on config parsing, even if not let it fail with a Rusqlite db file not found error
                    let path = config.main.db_path.as_ref().cloned().unwrap_or_default();
                    let mut connection = RqlConnection::open_with_flags(path, OpenFlags::SQLITE_OPEN_READ_WRITE).migration_err("could not open database")?;
                    Runner::new(migrations).set_grouped(grouped).set_abort_divergent(divergent).set_abort_missing(missing).run(&mut connection)?;
                } else {
                    panic!("tried to migrate from config for a sqlite database, but feature rusqlite not enabled!");
                }
            }
        }
        ConfigDbType::Postgres => {
            cfg_if::cfg_if! {
                if #[cfg(feature = "postgres")] {
                    let path = build_db_url("postgresql", &config);
                    let mut connection = PgConnection::connect(path.as_str(), TlsMode::None).migration_err("could not connect to database")?;
                    Runner::new(migrations).set_grouped(grouped).set_abort_divergent(divergent).set_abort_missing(missing).run(&mut connection)?;
                } else {
                    panic!("tried to migrate from config for a postgresql database, but feature postgres not enabled!");
                }
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{build_db_url, parse_config, Config, Error};
    use std::io::Write;

    #[test]
    fn returns_config_error_from_invalid_config_location() {
        let config_file = "invalid_path";
        let config = parse_config(config_file).unwrap_err();
        match config {
            Error::ConfigError(msg) => assert!(msg.contains("could not open config file")),
            _ => panic!("test failed"),
        }
    }

    #[test]
    fn returns_config_error_from_invalid_toml_file() {
        let config = "[<$%
                     db_type = \"Sqlite\" \n";

        let mut config_file = tempfile::NamedTempFile::new_in(".").unwrap();
        config_file.write_all(config.as_bytes()).unwrap();
        let config = parse_config(config_file.path()).unwrap_err();
        match config {
            Error::ConfigError(msg) => assert!(msg.contains("could not parse config file")),
            _ => panic!("test failed"),
        }
    }

    #[test]
    fn returns_config_error_from_sqlite_with_missing_path() {
        let config = "[main] \n
                     db_type = \"Sqlite\" \n";

        let mut config_file = tempfile::NamedTempFile::new_in(".").unwrap();
        config_file.write_all(config.as_bytes()).unwrap();
        let config = parse_config(config_file.path()).unwrap_err();
        match config {
            Error::ConfigError(msg) => {
                assert_eq!("field path must be present for Sqlite database type", msg)
            }
            _ => panic!("test failed"),
        }
    }

    #[test]
    fn builds_sqlite_path_from_relative_path() {
        let config = "[main] \n
                     db_type = \"Sqlite\" \n
                     db_path = \"./refinery.db\"";

        let mut config_file = tempfile::NamedTempFile::new_in(".").unwrap();
        config_file.write_all(config.as_bytes()).unwrap();
        let config = parse_config(config_file.path()).unwrap();
        let parent = config_file.path().parent().unwrap();
        assert!(parent.is_dir());
        assert_eq!(
            parent.join("./refinery.db").to_str().unwrap(),
            config.main.db_path.unwrap()
        );
    }

    #[test]
    fn builds_db_url() {
        let config = "[main] \n
                     db_type = \"Postgres\" \n
                     db_host = \"localhost\" \n
                     db_port = \"5432\" \n
                     db_user = \"root\" \n
                     db_pass = \"1234\" \n
                     db_name = \"refinery\"";

        let config: Config = toml::from_str(&config).unwrap();

        assert_eq!(
            "postgres://root:1234@localhost:5432/refinery",
            build_db_url("postgres", &config)
        );
    }
}
