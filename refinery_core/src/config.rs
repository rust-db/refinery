use crate::error::Kind;
use crate::Error;

use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

// refinery config file used by migrate_from_config if migration from a Config struct is prefered instead of using the macros
// Config can either be instanced with [`Config::new`] or retrieved from a config file with [`Config::from_file_location`]
#[derive(Serialize, Deserialize, Debug)]
pub struct Config {
    main: Main,
}

#[derive(Clone, Copy, Serialize, Deserialize, PartialEq, Debug)]
pub enum ConfigDbType {
    Mysql,
    Postgres,
    Sqlite,
}

impl Config {
    /// create a new config instance
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

    /// create a new Config instance from a config file located on the file system
    pub fn from_file_location<T: AsRef<Path>>(location: T) -> Result<Config, Error> {
        let file = std::fs::read_to_string(&location).map_err(|err| {
            Error::new(
                Kind::ConfigError(format!("could not open config file, {}", err)),
                None,
            )
        })?;

        let mut config: Config = toml::from_str(&file).map_err(|err| {
            Error::new(
                Kind::ConfigError(format!("could not parse config file, {}", err)),
                None,
            )
        })?;

        //replace relative path with canonical path in case of Sqlite db
        if config.main.db_type == ConfigDbType::Sqlite {
            let mut config_db_path = config.main.db_path.ok_or_else(|| {
                Error::new(
                    Kind::ConfigError("field path must be present for Sqlite database type".into()),
                    None,
                )
            })?;

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

            let config_db_path = config_db_path.canonicalize().map_err(|err| {
                Error::new(
                    Kind::ConfigError(format!("invalid sqlite db path, {}", err)),
                    None,
                )
            })?;

            config.main.db_path = Some(config_db_path);
        }

        Ok(config)
    }

    #[cfg(feature = "rusqlite")]
    pub(crate) fn db_path(&self) -> Option<&Path> {
        self.main.db_path.as_deref()
    }

    pub fn db_type(&self) -> ConfigDbType {
        self.main.db_type
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
struct Main {
    db_type: ConfigDbType,
    db_path: Option<PathBuf>,
    db_host: Option<String>,
    db_port: Option<String>,
    db_user: Option<String>,
    db_pass: Option<String>,
    db_name: Option<String>,
}

#[cfg(any(
    feature = "mysql",
    feature = "postgres",
    feature = "tokio-postgres",
    feature = "mysql_async"
))]
pub(crate) fn build_db_url(name: &str, config: &Config) -> String {
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

#[cfg(test)]
mod tests {
    use super::{build_db_url, Config, Error, Kind};
    use std::io::Write;
    use std::path::Path;

    #[test]
    fn returns_config_error_from_invalid_config_location() {
        let config = Config::from_file_location("invalid_path").unwrap_err();
        match config.kind() {
            Kind::ConfigError(msg) => assert!(msg.contains("could not open config file")),
            _ => panic!("test failed"),
        }
    }

    #[test]
    fn returns_config_error_from_invalid_toml_file() {
        let config = "[<$%
                     db_type = \"Sqlite\" \n";

        let mut config_file = tempfile::NamedTempFile::new_in(".").unwrap();
        config_file.write_all(config.as_bytes()).unwrap();
        let config = Config::from_file_location(config_file.path()).unwrap_err();
        match config.kind() {
            Kind::ConfigError(msg) => assert!(msg.contains("could not parse config file")),
            _ => panic!("test failed"),
        }
    }

    #[test]
    fn returns_config_error_from_sqlite_with_missing_path() {
        let config = "[main] \n
                     db_type = \"Sqlite\" \n";

        let mut config_file = tempfile::NamedTempFile::new_in(".").unwrap();
        config_file.write_all(config.as_bytes()).unwrap();
        let config = Config::from_file_location(config_file.path()).unwrap_err();
        match config.kind() {
            Kind::ConfigError(msg) => {
                assert_eq!("field path must be present for Sqlite database type", msg)
            }
            _ => panic!("test failed"),
        }
    }

    #[test]
    fn builds_sqlite_path_from_relative_path() {
        let db_file = tempfile::NamedTempFile::new_in(".").unwrap();

        let config = format!(
            "[main] \n
                       db_type = \"Sqlite\" \n
                       db_path = \"{}\"",
            db_file.path().file_name().unwrap().to_str().unwrap()
        );

        let mut config_file = tempfile::NamedTempFile::new_in(".").unwrap();
        config_file.write_all(config.as_bytes()).unwrap();
        let config = Config::from_file_location(config_file.path()).unwrap();

        let parent = config_file.path().parent().unwrap();
        assert!(parent.is_dir());
        assert_eq!(
            db_file.path().canonicalize().unwrap(),
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
