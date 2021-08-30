//! Submodule for handling setup commands
//!
//! Setting up refinery is fairly straight forward. We
//! create a configuration with default values, a migrations
//! directory and also an initial migration (for the db type
//! chosen) that contains metadata that refinery will use
//! for future migrations.
//!
//! This process can be adjusted by user flags. All of this
//! is mirrored in the refinery configuration file stored
//! in `refinery.toml` in the crate root.
//!
//! When running the setup *again* when a configuration file
//! is already present, the fields from it can be used to
//! override otherwise default values

use anyhow::{anyhow, Result};
use clap::ArgMatches;
use refinery_core::config::{Config, ConfigDbType};
use std::fs::File;
use std::io::{self, Write};

/// Do everything that the module docs promise. And more ✨
pub fn handle_setup(_: &ArgMatches) -> Result<()> {
    let cfg = get_config_from_input()?;
    let s = toml::to_string(&cfg)?;
    let mut file = File::create("./refinery.toml").unwrap();
    file.write_all(s.as_bytes()).ok();
    Ok(())
}

fn get_config_from_input() -> Result<Config> {
    println!("Select database 1) Mysql 2) Postgresql 3) Sqlite 4) Mssql: ");
    print!("Enter a number: ");
    io::stdout().flush()?;

    let mut db_type = String::new();
    io::stdin().read_line(&mut db_type)?;
    let db_type = match db_type.as_str().trim() {
        "1" => ConfigDbType::Mysql,
        "2" => ConfigDbType::Postgres,
        "3" => ConfigDbType::Sqlite,
        "4" => ConfigDbType::Mssql,
        _ => return Err(anyhow!("invalid option")),
    };
    let mut config = Config::new(db_type);

    if config.db_type() == ConfigDbType::Sqlite {
        cfg_if::cfg_if! {
            if #[cfg(feature = "sqlite")] {
                print!("Enter database path: ");
                io::stdout().flush()?;
                let mut db_path = String::new();
                io::stdin().read_line(&mut db_path)?;
                //remove \n
                db_path.pop();
                config = config.set_db_path(db_path.trim());
                return Ok(config);
            } else {
                panic!("tried to migrate async from config for a sqlite database, but sqlite feature was not enabled!");
            }
        }
    }

    print!("Enter database host: ");
    io::stdout().flush()?;
    let mut db_host = String::new();
    io::stdin().read_line(&mut db_host)?;
    db_host.pop();
    config = config.set_db_host(&db_host);

    print!("Enter database port: ");
    io::stdout().flush()?;
    let mut db_port = String::new();
    io::stdin().read_line(&mut db_port)?;
    db_port.pop();
    config = config.set_db_port(&db_port);

    print!("Enter database username: ");
    io::stdout().flush()?;
    let mut db_user = String::new();
    io::stdin().read_line(&mut db_user)?;
    db_user.pop();
    config = config.set_db_user(&db_user);

    print!("Enter database password: ");
    io::stdout().flush()?;
    let mut db_pass = String::new();
    io::stdin().read_line(&mut db_pass)?;
    db_pass.pop();
    config = config.set_db_pass(&db_pass);

    print!("Enter database name: ");
    io::stdout().flush()?;
    let mut db_name = String::new();
    io::stdin().read_line(&mut db_name)?;
    db_name.pop();
    config = config.set_db_name(&db_name);

    Ok(config)
}
