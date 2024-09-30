/*!
Powerful SQL migration toolkit for Rust.

`refinery` makes running migrations for different databases as easy as possible.
It works by running your migrations on a provided database connection, either by embedding them on your Rust code, or via `refinery_cli`.\
Currently, [`Postgres`](https://crates.io/crates/postgres), [`Rusqlite`](https://crates.io/crates/rusqlite), and [`Mysql`](https://crates.io/crates/mysql) are supported.\

`refinery` works best with [`Barrel`](https://crates.io/crates/barrel) but you can also have your migrations on .sql files or use any other Rust crate for schema generation.

## Usage

- Migrations can be defined in .sql files or Rust modules that must have a function called `migration()` that returns a [`std::string::String`]
- Migrations, both .sql files and Rust modules must be named in the format `V{1}__{2}.rs ` where `{1}` represents the migration version and `{2}` the name.
- Migrations can be run either by embedding them on your Rust code with [`embed_migrations!`] macro, or via `refinery_cli`.

[`embed_migrations!`]: macro.embed_migrations.html

### Example
```rust,ignore
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

pub use refinery_core::config;
pub use refinery_core::{error, load_sql_migrations, Error, Migration, Report, Runner, Target};
#[doc(hidden)]
pub use refinery_core::{AsyncExecutor, AsyncMigrate, Executor, Migrate};
pub use refinery_macros::embed_migrations;
