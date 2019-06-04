use serde::{Deserialize, Serialize};

///Config used by refinery-cli
#[derive(Serialize, Deserialize)]
pub struct Config {
    pub main: Main,
}

#[derive(Serialize, Deserialize, PartialEq)]
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

#[derive(Serialize, Deserialize)]
pub struct Main {
    pub db_type: ConfigDbType,
    pub db_path: Option<String>,
    pub db_host: Option<String>,
    pub db_port: Option<String>,
    pub db_user: Option<String>,
    pub db_pass: Option<String>,
    pub db_name: Option<String>,
}
