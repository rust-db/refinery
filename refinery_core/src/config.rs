use crate::error::Kind;
use crate::Error;
use std::convert::TryFrom;
use std::path::PathBuf;
use std::str::FromStr;
use url::Url;

// refinery config file used by migrate_from_config if migration from a Config struct is preferred instead of using the macros
// Config can either be instanced with [`Config::new`] or retrieved from a config file with [`Config::from_file_location`]
#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub struct Config {
    main: Main,
}

#[derive(Clone, Copy, PartialEq, Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
pub enum ConfigDbType {
    Mysql,
    Postgres,
    Sqlite,
    Mssql,
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
                #[cfg(feature = "tiberius-config")]
                trust_cert: false,
            },
        }
    }

    /// create a new Config instance from an environment variable that contains a URL
    pub fn from_env_var(name: &str) -> Result<Config, Error> {
        let value = std::env::var(name).map_err(|_| {
            Error::new(
                Kind::ConfigError(format!("Couldn't find {} environment variable", name)),
                None,
            )
        })?;
        Config::from_str(&value)
    }

    /// create a new Config instance from a config file located on the file system
    #[cfg(feature = "toml")]
    pub fn from_file_location<T: AsRef<std::path::Path>>(location: T) -> Result<Config, Error> {
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

                config_db_dir = std::fs::canonicalize(config_db_dir).unwrap();
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

    cfg_if::cfg_if! {
        if #[cfg(feature = "rusqlite")] {
            pub(crate) fn db_path(&self) -> Option<&std::path::Path> {
                self.main.db_path.as_deref()
            }

            pub fn set_db_path(self, db_path: &str) -> Config {
                Config {
                    main: Main {
                        db_path: Some(db_path.into()),
                        ..self.main
                    },
                }
            }
        }
    }

    cfg_if::cfg_if! {
        if #[cfg(feature = "tiberius-config")] {
            pub fn set_trust_cert(&mut self) {
                self.main.trust_cert = true;
            }
        }
    }

    pub fn db_type(&self) -> ConfigDbType {
        self.main.db_type
    }

    pub fn db_host(&self) -> Option<&str> {
        self.main.db_host.as_deref()
    }

    pub fn db_port(&self) -> Option<&str> {
        self.main.db_port.as_deref()
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

impl TryFrom<Url> for Config {
    type Error = Error;

    fn try_from(url: Url) -> Result<Config, Self::Error> {
        let db_type = match url.scheme() {
            "mysql" => ConfigDbType::Mysql,
            "postgres" => ConfigDbType::Postgres,
            "postgresql" => ConfigDbType::Postgres,
            "sqlite" => ConfigDbType::Sqlite,
            "mssql" => ConfigDbType::Mssql,
            _ => {
                return Err(Error::new(
                    Kind::ConfigError("Unsupported database".into()),
                    None,
                ))
            }
        };

        cfg_if::cfg_if! {
            if #[cfg(feature = "tiberius-config")] {
                use std::{borrow::Cow, collections::HashMap};
                let query_params = url
                    .query_pairs()
                    .collect::<HashMap< Cow<'_, str>,  Cow<'_, str>>>();

                let trust_cert = query_params.
                    get("trust_cert")
                    .unwrap_or(&Cow::Borrowed("false"))
                    .parse::<bool>()
                    .map_err(|_| {
                        Error::new(
                            Kind::ConfigError("Invalid trust_cert value, please use true/false".into()),
                            None,
                        )
                    })?;
            }
        }

        Ok(Self {
            main: Main {
                db_type,
                db_path: Some(
                    url.as_str()[url.scheme().len()..]
                        .trim_start_matches(':')
                        .trim_start_matches("//")
                        .to_string()
                        .into(),
                ),
                db_host: url.host_str().map(|r| r.to_string()),
                db_port: url.port().map(|r| r.to_string()),
                db_user: Some(url.username().to_string()),
                db_pass: url.password().map(|r| r.to_string()),
                db_name: Some(url.path().trim_start_matches('/').to_string()),
                #[cfg(feature = "tiberius-config")]
                trust_cert,
            },
        })
    }
}

impl FromStr for Config {
    type Err = Error;

    /// create a new Config instance from a string that contains a URL
    fn from_str(url_str: &str) -> Result<Config, Self::Err> {
        let url = Url::parse(url_str).map_err(|_| {
            Error::new(
                Kind::ConfigError(format!("Couldn't parse the string '{}' as a URL", url_str)),
                None,
            )
        })?;
        Config::try_from(url)
    }
}

#[derive(Debug)]
#[cfg_attr(feature = "serde", derive(serde::Serialize, serde::Deserialize))]
struct Main {
    db_type: ConfigDbType,
    db_path: Option<PathBuf>,
    db_host: Option<String>,
    db_port: Option<String>,
    db_user: Option<String>,
    db_pass: Option<String>,
    db_name: Option<String>,
    #[cfg(feature = "tiberius-config")]
    #[serde(default)]
    trust_cert: bool,
}

#[cfg(any(
    feature = "mysql",
    feature = "postgres",
    feature = "tokio-postgres",
    feature = "sqlx-postgres",
    feature = "mysql_async"
))]
pub(crate) fn build_db_url(name: &str, config: &Config) -> String {
    let mut url: String = name.to_string() + "://";

    if let Some(user) = &config.main.db_user {
        url = url + user;
    }
    if let Some(pass) = &config.main.db_pass {
        url = url + ":" + pass;
    }
    if let Some(host) = &config.main.db_host {
        if config.main.db_user.is_some() {
            url = url + "@" + host;
        } else {
            url = url + host;
        }
    }
    if let Some(port) = &config.main.db_port {
        url = url + ":" + port;
    }
    if let Some(name) = &config.main.db_name {
        url = url + "/" + name;
    }
    url
}

cfg_if::cfg_if! {
    if #[cfg(feature = "tiberius-config")] {
        use tiberius::{AuthMethod, Config as TConfig};

        impl TryFrom<&Config> for TConfig {
            type Error=Error;

            fn try_from(config: &Config) -> Result<Self, Self::Error> {
                let mut tconfig = TConfig::new();
                if let Some(host) = &config.main.db_host {
                    tconfig.host(host);
                }

                if let Some(port) = &config.main.db_port {
                    let port = port.parse().map_err(|_| Error::new(
                            Kind::ConfigError(format!("Couldn't parse value {} as mssql port", port)),
                            None,
                    ))?;
                    tconfig.port(port);
                }

                if let Some(db) = &config.main.db_name {
                    tconfig.database(db);
                }

                let user = config.main.db_user.as_deref().unwrap_or("");
                let pass = config.main.db_pass.as_deref().unwrap_or("");

                if config.main.trust_cert {
                    tconfig.trust_cert();
                }
                tconfig.authentication(AuthMethod::sql_server(user, pass));

                Ok(tconfig)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{build_db_url, Config, Kind};
    use std::io::Write;
    use std::str::FromStr;

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

        let config: Config = toml::from_str(config).unwrap();

        assert_eq!(
            "postgres://root:1234@localhost:5432/refinery",
            build_db_url("postgres", &config)
        );
    }

    #[test]
    fn builds_db_env_var() {
        std::env::set_var(
            "DATABASE_URL",
            "postgres://root:1234@localhost:5432/refinery",
        );
        let config = Config::from_env_var("DATABASE_URL").unwrap();
        assert_eq!(
            "postgres://root:1234@localhost:5432/refinery",
            build_db_url("postgres", &config)
        );
    }

    #[test]
    fn builds_from_str() {
        let config = Config::from_str("postgres://root:1234@localhost:5432/refinery").unwrap();
        assert_eq!(
            "postgres://root:1234@localhost:5432/refinery",
            build_db_url("postgres", &config)
        );
    }

    #[test]
    fn builds_db_env_var_failure() {
        std::env::set_var("DATABASE_URL", "this_is_not_a_url");
        let config = Config::from_env_var("DATABASE_URL");
        assert!(config.is_err());
    }
}
