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

use crate::config::*;
use clap::ArgMatches;
use std::fs::File;
use std::io::Write;

/// Do everything that the module docs promise. And more ✨
pub fn handle_setup(_: &ArgMatches) {
    let cfg = Config {
        main: Main {
            env: ConfigEnvType::Develop,
            db_type: "postgres".to_owned(),
            db_path: "localhost/testing".to_owned(),
            db_user: "testing".to_owned(),
            db_pw: "testing".to_owned(),
        },
    };

    let s = serialize(&cfg).unwrap();
    let mut file = File::create("Refinery.toml").unwrap();
    file.write_all(s.as_bytes()).ok();
}
