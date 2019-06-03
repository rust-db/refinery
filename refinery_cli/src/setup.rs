//! Submodule for handling setup commands
//!
//! Setting up refinery is fairely straight forward. We
//! create a configuration with default values, a migrations
//! directory and also an initial migration (for the db type
//! chosen) that contains metadata that refinery will use
//! for future migrations.
//!
//! This process can be adjusted by user flags. All of this
//! is mirrored in the refinery configuration file stored
//! in `Refinery.toml` in the crate root.
//!
//! When running the setup *again* when a configuration file
//! is already present, the fields from it can be used to
//! override otherwise default values

use clap::ArgMatches;
use failure::{format_err, Error};
use refinery_migrations::{Config, ConfigDbType, Main};
use std::fs::File;
use std::io::{self, Write};

/// Do everything that the module docs promise. And more ✨
pub fn handle_setup(_: &ArgMatches) -> Result<(), Error> {
    let cfg = get_config_from_input()?;
    let s = toml::to_string(&cfg)?;
    let mut file = File::create("./Refinery.toml").unwrap();
    file.write_all(s.as_bytes()).ok();
    Ok(())
}

fn get_config_from_input() -> Result<Config, Error> {
    println!("Select database 1) Mysql 2) Postgresql 3) Sqlite: ");
    print!("Enter a number: ");
    io::stdout().flush()?;

    let mut db_type = String::new();
    io::stdin().read_line(&mut db_type)?;
    let db_type = match db_type.as_str().trim() {
        "1" => ConfigDbType::Mysql,
        "2" => ConfigDbType::Postgres,
        "3" => ConfigDbType::Sqlite,
        _ => return Err(format_err!("invalid option")),
    };

    print!("Enter database path: ");
    io::stdout().flush()?;
    let mut db_path = String::new();
    io::stdin().read_line(&mut db_path)?;
    //remove \n
    db_path.pop();

    if db_type == ConfigDbType::Sqlite {
        return Ok(Config {
            main: Main {
                db_type: db_type.into(),
                db_path: db_path.trim().into(),
                db_user: None,
                db_pw: None,
                db_name: None,
            },
        });
    }

    print!("Enter database name: ");
    io::stdout().flush()?;
    let mut db_name = String::new();
    io::stdin().read_line(&mut db_name)?;
    db_name.pop();

    print!("Enter database username: ");
    io::stdout().flush()?;
    let mut db_user = String::new();
    io::stdin().read_line(&mut db_user)?;
    db_user.pop();

    print!("Enter database password: ");
    io::stdout().flush()?;
    let mut db_pw = String::new();
    io::stdin().read_line(&mut db_pw)?;
    db_pw.pop();

    Ok(Config {
        main: Main {
            db_type,
            db_path: db_path.into(),
            db_user: Some(db_user),
            db_pw: Some(db_pw),
            db_name: Some(db_name),
        },
    })
}
