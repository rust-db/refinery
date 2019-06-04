/*!
Powerful SQL migration toolkit for Rust.

`refinery` makes running migrations for different databases as easy as possible.
it works by running your migrations on a provided database connection, either by embedding them on your Rust code, or via `refinery_cli`.\
currently [`Postgres`](https://crates.io/crates/postgres), [`Rusqlite`](https://crates.io/crates/rusqlite) and [`Mysql`](https://crates.io/crates/mysql) are supported.\
`refinery` works best with [`Barrel`](https://crates.io/crates/barrel) but you can also have your migrations on .sql files or use any other Rust crate for schema generation.

## Usage

- Migrations can be defined in .sql files or Rust modules that must have a function called `migration` that returns a [`String`](https://doc.rust-lang.org/std/string/struct.String.html)
- Migrations, both .sql files and Rust modules must be named in the format `V{1}__{2}.rs ` where `{1}` represents the migration version and `{2}` the name.
- Migrations can be run either by embedding them on your Rust code with [`embedded_migrations`](../refinery_macros/macro.embed_migrations.html) and [`include_migration_mods`](../refinery_macros/macro.include_migration_mods.html) macros, or via `refinery_cli`.

### Example
```rust,no_run
use ttrusqlite::Connection;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("refinery/tests/sql_migrations");
}

fn main() {
    let mut conn = Connection::open_in_memory().unwrap();
    embedded::migrations::runner().run(&mut conn).unwrap();
}
```

for more examples refer to the [`examples`](https://github.com/rust-db/refinery/tree/master/examples)
*/

pub use refinery_macros::{embed_migrations, include_migration_mods};
pub use refinery_migrations::{Config, ConfigDbType, Migration, Runner};
#[doc(hidden)]
pub use refinery_migrations::{Error, Migrate, MigrateGrouped};

#[doc(hidden)]
#[cfg(all(feature = "mysql", feature = "postgres", feature = "rusqlite"))]
pub use refinery_migrations::migrate_from_config;
