![refinery Logo](assets/logo_wide.svg)

Powerful SQL migration toolkit for Rust.

[![Crates.io][crates-badge]][crates-url]
[![docs.rs][docs-badge]][docs-url]
[![MIT licensed][mit-badge]][mit-url]
[![Build Status][circleci-badge]][circleci-url]

[crates-badge]: https://img.shields.io/crates/v/refinery.svg
[crates-url]: https://crates.io/crates/refinery
[docs-badge]: https://docs.rs/refinery/badge.svg
[docs-url]: https://docs.rs/refinery/
[mit-badge]: https://img.shields.io/badge/license-MIT-blue.svg
[mit-url]: LICENSE
[circleci-badge]: https://img.shields.io/circleci/build/github/rust-db/refinery
[circleci-url]: https://circleci.com/gh/rust-db/refinery/tree/master

`refinery` makes running migrations for different databases as easy as possible.
It works by running your migrations on a provided database connection, either by embedding them on your Rust code, or via `refinery_cli`.
Currently [`postgres`](https://crates.io/crates/postgres), [`tokio-postgres`](https://crates.io/crates/tokio-postgres) , [`mysql`](https://crates.io/crates/mysql), [`mysql_async`](https://crates.io/crates/mysql_async) and [`rusqlite`](https://crates.io/crates/rusqlite) are supported.\
If you are using a driver that is not yet supported, namely [`SQLx`](https://github.com/launchbadge/sqlx) you can run migrations providing a [`Config`](https://docs.rs/refinery/0.2.1/refinery/struct.Config.html) instead of the connection type, as `Config` impl's `Migrate`. You will still need to provide the `postgres`/`mysql`/`rusqlite` driver as a feature for [`Runner::run`](https://docs.rs/refinery/latest/refinery/struct.Runner.html#method.run) and `tokio-postgres`/`mysql_async` for [`Runner::run_async`](https://docs.rs/refinery/latest/refinery/struct.Runner.html#method.run_async).\
`refinery` works best with [`Barrel`](https://crates.io/crates/barrel) but you can also have your migrations in .sql files or use any other Rust crate for schema generation.

## Usage

- Add refinery to your Cargo.toml dependencies with the selected driver as feature eg: `refinery = { version = "0.2", features = ["rusqlite"]}`
- Migrations can be defined in .sql files or Rust modules that must have a function called `migration` that returns a [`String`](https://doc.rust-lang.org/std/string/struct.String.html).
- Migrations, both .sql files and Rust modules must be named in the format `V{1}__{2}.sql` or `V{1}__{2}.rs`, where `{1}` represents the migration version and `{2}` the name.
- Migrations can be run either by embedding them in your Rust code with `embed_migrations` and `include_migration_mods` macros, or via `refinery_cli`.

### Example
```rust,no_run
use rusqlite::Connection;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("./tests/sql_migrations");
}

fn main() {
    let mut conn = Connection::open_in_memory().unwrap();
    embedded::migrations::runner().run(&mut conn).unwrap();
}
```

For more examples, refer to the [`examples`](examples).

## Implementation details

refinery works by creating a table that keeps all the applied migrations' versions and their metadata. When you [run](https://docs.rs/refinery/latest/refinery/struct.Runner.html#method.run) the migrations `Runner`, refinery compares the applied migrations with the the ones to be applied, checking for [divergent](https://docs.rs/refinery/latest/refinery/struct.Runner.html#method.set_abort_divergent) and [missing](https://docs.rs/refinery/latest/refinery/struct.Runner.html#method.set_abort_missing) and executing unapplied migrations.\
By default, refinery runs each migration in a single transaction. Alternatively, you can also configure refinery to wrap the entire execution of all migrations in a single transaction by setting [set_grouped](https://docs.rs/refinery/latest/refinery/struct.Runner.html#method.set_grouped) to true.

### Rollback

refinery's design is based on [flyway](https://flywaydb.org/) and so, shares its [perspective](https://flywaydb.org/documentation/command/undo#important-notes) on undo/rollback migrations. To undo/rollback a migration, you have to generate a new one and write specifically what you want to undo.

## Compatibility

refinery aims to support stable Rust, the previous Rust version, and nightly.

## Async

Starting with version 0.2 refinery supports [tokio-postgres](https://crates.io/crates/tokio-postgres) and [`mysql_async`](https://crates.io/crates/mysql_async). To migrate async you have to call `Runner`'s [run_async](https://docs.rs/refinery/latest/refinery/struct.Runner.html).
There are plans to support [Tiberius](https://github.com/steffengy/tiberius) when futures 0.3 support stabilizes.
For Rusqlite, the best way to run migrations in an async context is to run them inside tokio's [`spawn_blocking`](https://docs.rs/tokio/0.2.21/tokio/task/fn.spawn_blocking.html) for example.

## Contributing

:balloon: Thanks for your help improving the project!
No contribution is too small and all contributions are valued, feel free to open Issues and submit Pull Requests

## License

This project is licensed under the [MIT license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in refinery by you, shall be licensed as MIT, without any additional
terms or conditions.
