/*!
Powerful SQL migration toolkit for Rust.

`refinery` makes running migrations for different databases as easy as possible.
It works by running your migrations on a provided database connection, either by embedding them on your Rust code, or via `refinery_cli`.\
Currently, [`Postgres`](https://crates.io/crates/postgres), [`Rusqlite`](https://crates.io/crates/rusqlite), and [`Mysql`](https://crates.io/crates/mysql) are supported.\

`refinery` works best with [`Barrel`](https://crates.io/crates/barrel) but you can also have your migrations on .sql files or use any other Rust crate for schema generation.

## Usage

- Migrations can be defined in .sql files or Rust modules that must have a function called `migration()` that returns a [`std::string::String`]
- Migrations, both .sql files and Rust modules must be named in the format `V{1}__{2}.rs ` where `{1}` represents the migration version and `{2}` the name.
- Migrations can be run either by embedding them on your Rust code with [`embed_migrations!`] and [`include_migration_mods!`] macros, or via `refinery_cli`.

[`embed_migrations!`]: macro.embed_migrations.html
[`include_migration_mods!`]: macro.include_migration_mods.html

### Example
```rust,no_run
use rusqlite::Connection;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./tests/sql_migrations");
}

let mut conn = Connection::open_in_memory().unwrap();
embedded::migrations::runner().run(&mut conn).unwrap();
```

for more examples refer to the [examples](https://github.com/rust-db/refinery/tree/master/examples)
*/

pub mod config;
mod drivers;
mod error;
mod runner;
mod traits;
mod util;

pub use crate::error::Error;
pub use crate::runner::{AppliedMigration, Migration, Runner};
pub use crate::traits::r#async::AsyncMigrate;
pub use crate::traits::sync::Migrate;
pub use crate::util::{find_migration_files, MigrationType};

#[cfg(feature = "rusqlite")]
pub use rusqlite;

#[cfg(feature = "postgres")]
pub use postgres;

#[cfg(feature = "mysql")]
pub use mysql;

#[cfg(feature = "tokio-postgres")]
pub use tokio_postgres;

#[cfg(feature = "mysql_async")]
pub use mysql_async;

#[cfg(feature = "tokio")]
pub use tokio;
