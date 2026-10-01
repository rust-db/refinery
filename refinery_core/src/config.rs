use crate::error::Kind;
use crate::Error;
#[cfg(any(
    feature = "postgres",
    feature = "tokio-postgres",
    feature = "tiberius-config"
))]
use std::borrow::Cow;
use std::convert::TryFrom;
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
            main: Main::new(db_type),
        }
    }

    /// create a new Config instance from an environment variable that contains a URL
    pub fn from_env_var(name: &str) -> Result<Config, Error> {
        let value = std::env::var(name).map_err(|_| {
            Error::new(
                Kind::ConfigError(format!("Couldn't find {name} environment variable")),
                None,
            )
        })?;
        Config::from_str(&value)
    }

    pub fn db_type(&self) -> ConfigDbType {
        self.main.db_type
    }

    /// create a new Config instance from a config file located on the file system
    #[cfg(feature = "toml")]
    pub fn from_file_location<T: AsRef<std::path::Path>>(location: T) -> Result<Config, Error> {
        let file = std::fs::read_to_string(&location).map_err(|err| {
            Error::new(
                Kind::ConfigError(format!("could not open config file, {err}")),
                None,
            )
        })?;

        let config: Config = toml::from_str(&file).map_err(|err| {
            Error::new(
                Kind::ConfigError(format!("could not parse config file, {err}")),
                None,
            )
        })?;

        //replace relative path with canonical path in case of Sqlite db
        #[cfg(feature = "rusqlite")]
        if config.main.db_type == ConfigDbType::Sqlite {
            let mut config = config;
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
                    Kind::ConfigError(format!("invalid sqlite db path, {err}")),
                    None,
                )
            })?;
            config.main.db_path = Some(config_db_path);

            return Ok(config);
        }

        Ok(config)
    }

    #[cfg(feature = "tiberius-config")]
    pub fn set_trust_cert(&mut self) {
        self.main.trust_cert = true;
    }
}

#[cfg(any(
    feature = "mysql",
    feature = "postgres",
    feature = "tokio-postgres",
    feature = "mysql_async",
    feature = "tiberius-config"
))]
impl Config {
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

#[cfg(feature = "rusqlite")]
impl Config {
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

#[cfg(any(feature = "postgres", feature = "tokio-postgres"))]
impl Config {
    pub fn use_tls(&self) -> bool {
        self.main.use_tls
    }

    pub fn set_use_tls(self, use_tls: bool) -> Config {
        Config {
            main: Main {
                use_tls,
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

        Ok(Self {
            main: Main {
                db_type,
                #[cfg(feature = "rusqlite")]
                db_path: Some(
                    url.as_str()[url.scheme().len()..]
                        .trim_start_matches(':')
                        .trim_start_matches("//")
                        .to_string()
                        .into(),
                ),
                #[cfg(any(
                    feature = "mysql",
                    feature = "postgres",
                    feature = "tokio-postgres",
                    feature = "mysql_async",
                    feature = "tiberius-config"
                ))]
                db_host: url.host_str().map(|r| r.to_string()),
                #[cfg(any(
                    feature = "mysql",
                    feature = "postgres",
                    feature = "tokio-postgres",
                    feature = "mysql_async",
                    feature = "tiberius-config"
                ))]
                db_port: url.port().map(|r| r.to_string()),
                #[cfg(any(
                    feature = "mysql",
                    feature = "postgres",
                    feature = "tokio-postgres",
                    feature = "mysql_async",
                    feature = "tiberius-config"
                ))]
                db_user: Some(url.username().to_string()),
                #[cfg(any(
                    feature = "mysql",
                    feature = "postgres",
                    feature = "tokio-postgres",
                    feature = "mysql_async",
                    feature = "tiberius-config"
                ))]
                db_pass: url.password().map(|r| r.to_string()),
                #[cfg(any(
                    feature = "mysql",
                    feature = "postgres",
                    feature = "tokio-postgres",
                    feature = "mysql_async",
                    feature = "tiberius-config"
                ))]
                db_name: Some(url.path().trim_start_matches('/').to_string()),
                #[cfg(any(feature = "postgres", feature = "tokio-postgres"))]
                use_tls: match url
                    .query_pairs()
                    .collect::<std::collections::HashMap<Cow<'_, str>, Cow<'_, str>>>()
                    .get("sslmode")
                {
                    Some(Cow::Borrowed("require")) => true,
                    Some(Cow::Borrowed("disable")) | None => false,
                    _ => {
                        return Err(Error::new(
                            Kind::ConfigError(
                                "Invalid sslmode value, please use disable/require".into(),
                            ),
                            None,
                        ))
                    }
                },
                #[cfg(feature = "tiberius-config")]
                trust_cert: url
                    .query_pairs()
                    .collect::<std::collections::HashMap<Cow<'_, str>, Cow<'_, str>>>()
                    .get("trust_cert")
                    .unwrap_or(&Cow::Borrowed("false"))
                    .parse::<bool>()
                    .map_err(|_| {
                        Error::new(
                            Kind::ConfigError(
                                "Invalid trust_cert value, please use true/false".into(),
                            ),
                            None,
                        )
                    })?,
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
                Kind::ConfigError(format!("Couldn't parse the string '{url_str}' as a URL")),
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
    #[cfg(feature = "rusqlite")]
    db_path: Option<std::path::PathBuf>,
    #[cfg(any(
        feature = "mysql",
        feature = "postgres",
        feature = "tokio-postgres",
        feature = "mysql_async",
        feature = "tiberius-config"
    ))]
    db_host: Option<String>,
    #[cfg(any(
        feature = "mysql",
        feature = "postgres",
        feature = "tokio-postgres",
        feature = "mysql_async",
        feature = "tiberius-config"
    ))]
    db_port: Option<String>,
    #[cfg(any(
        feature = "mysql",
        feature = "postgres",
        feature = "tokio-postgres",
        feature = "mysql_async",
        feature = "tiberius-config"
    ))]
    db_user: Option<String>,
    #[cfg(any(
        feature = "mysql",
        feature = "postgres",
        feature = "tokio-postgres",
        feature = "mysql_async",
        feature = "tiberius-config"
    ))]
    db_pass: Option<String>,
    #[cfg(any(
        feature = "mysql",
        feature = "postgres",
        feature = "tokio-postgres",
        feature = "mysql_async",
        feature = "tiberius-config"
    ))]
    db_name: Option<String>,
    #[cfg(any(feature = "postgres", feature = "tokio-postgres"))]
    #[cfg_attr(feature = "serde", serde(default))]
    use_tls: bool,
    #[cfg(feature = "tiberius-config")]
    #[cfg_attr(feature = "serde", serde(default))]
    trust_cert: bool,
}

impl Main {
    fn new(db_type: ConfigDbType) -> Self {
        Main {
            db_type,
            #[cfg(feature = "rusqlite")]
            db_path: None,
            #[cfg(any(
                feature = "mysql",
                feature = "postgres",
                feature = "tokio-postgres",
                feature = "mysql_async",
                feature = "tiberius-config"
            ))]
            db_host: None,
            #[cfg(any(
                feature = "mysql",
                feature = "postgres",
                feature = "tokio-postgres",
                feature = "mysql_async",
                feature = "tiberius-config"
            ))]
            db_port: None,
            #[cfg(any(
                feature = "mysql",
                feature = "postgres",
                feature = "tokio-postgres",
                feature = "mysql_async",
                feature = "tiberius-config"
            ))]
            db_user: None,
            #[cfg(any(
                feature = "mysql",
                feature = "postgres",
                feature = "tokio-postgres",
                feature = "mysql_async",
                feature = "tiberius-config"
            ))]
            db_pass: None,
            #[cfg(any(
                feature = "mysql",
                feature = "postgres",
                feature = "tokio-postgres",
                feature = "mysql_async",
                feature = "tiberius-config"
            ))]
            db_name: None,
            #[cfg(any(feature = "postgres", feature = "tokio-postgres"))]
            use_tls: false,
            #[cfg(feature = "tiberius-config")]
            trust_cert: false,
        }
    }
}

#[cfg(any(
    feature = "mysql",
    feature = "postgres",
    feature = "tokio-postgres",
    feature = "mysql_async",
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

#[cfg(feature = "tiberius-config")]
impl TryFrom<&Config> for tiberius::Config {
    type Error = Error;

    fn try_from(config: &Config) -> Result<Self, Self::Error> {
        let mut tconfig = tiberius::Config::new();
        if let Some(host) = &config.main.db_host {
            tconfig.host(host);
        }

        if let Some(port) = &config.main.db_port {
            let port = port.parse().map_err(|_| {
                Error::new(
                    Kind::ConfigError(format!("Couldn't parse value {port} as mssql port")),
                    None,
                )
            })?;
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
        tconfig.authentication(tiberius::AuthMethod::sql_server(user, pass));

        Ok(tconfig)
    }
}

#[cfg(test)]
mod tests {
    use super::{Config, Kind};
    use std::io::Write;
    use std::str::FromStr;

    #[cfg(any(
        feature = "mysql",
        feature = "postgres",
        feature = "tokio-postgres",
        feature = "mysql_async"
    ))]
    use super::build_db_url;

    #[test]
    #[cfg(feature = "toml")]
    fn returns_config_error_from_invalid_config_location() {
        let config = Config::from_file_location("invalid_path").unwrap_err();
        match config.kind() {
            Kind::ConfigError(msg) => assert!(msg.contains("could not open config file")),
            _ => panic!("test failed"),
        }
    }

    #[test]
    #[cfg(feature = "toml")]
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
    #[cfg(all(feature = "toml", feature = "rusqlite"))]
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
    #[cfg(all(feature = "toml", feature = "rusqlite"))]
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
    #[cfg(all(
        feature = "toml",
        any(
            feature = "mysql",
            feature = "postgres",
            feature = "tokio-postgres",
            feature = "mysql_async"
        )
    ))]
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
    #[cfg(any(
        feature = "mysql",
        feature = "postgres",
        feature = "tokio-postgres",
        feature = "mysql_async"
    ))]
    fn builds_db_env_var() {
        std::env::set_var(
            "TEST_DATABASE_URL",
            "postgres://root:1234@localhost:5432/refinery",
        );
        let config = Config::from_env_var("TEST_DATABASE_URL").unwrap();
        assert_eq!(
            "postgres://root:1234@localhost:5432/refinery",
            build_db_url("postgres", &config)
        );
    }

    #[test]
    #[cfg(any(
        feature = "mysql",
        feature = "postgres",
        feature = "tokio-postgres",
        feature = "mysql_async"
    ))]
    fn builds_from_str() {
        let config = Config::from_str("postgres://root:1234@localhost:5432/refinery").unwrap();
        assert_eq!(
            "postgres://root:1234@localhost:5432/refinery",
            build_db_url("postgres", &config)
        );
    }

    #[cfg(any(feature = "postgres", feature = "tokio-postgres"))]
    #[test]
    fn builds_from_sslmode_str() {
        use crate::config::ConfigDbType;

        let config_disable =
            Config::from_str("postgres://root:1234@localhost:5432/refinery?sslmode=disable")
                .unwrap();
        assert!(!config_disable.use_tls());

        let config_require =
            Config::from_str("postgres://root:1234@localhost:5432/refinery?sslmode=require")
                .unwrap();
        assert!(config_require.use_tls());

        // Verify that manually created config matches parsed URL config
        let manual_config_disable = Config::new(ConfigDbType::Postgres)
            .set_db_user("root")
            .set_db_pass("1234")
            .set_db_host("localhost")
            .set_db_port("5432")
            .set_db_name("refinery")
            .set_use_tls(false);
        assert_eq!(config_disable.use_tls(), manual_config_disable.use_tls());

        let manual_config_require = Config::new(ConfigDbType::Postgres)
            .set_db_user("root")
            .set_db_pass("1234")
            .set_db_host("localhost")
            .set_db_port("5432")
            .set_db_name("refinery")
            .set_use_tls(true);
        assert_eq!(config_require.use_tls(), manual_config_require.use_tls());

        let config =
            Config::from_str("postgres://root:1234@localhost:5432/refinery?sslmode=invalidvalue");
        assert!(config.is_err());
    }

    #[test]
    fn builds_db_env_var_failure() {
        std::env::set_var("TEST_DATABASE_URL_INVALID", "this_is_not_a_url");
        let config = Config::from_env_var("TEST_DATABASE_URL_INVALID");
        assert!(config.is_err());
    }
}
