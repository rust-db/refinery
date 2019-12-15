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
it works by running your migrations on a provided database connection, either by embedding them on your Rust code, or via `refinery_cli`.
currently [`Postgres`](https://crates.io/crates/postgres), [`Rusqlite`](https://crates.io/crates/rusqlite) and [`Mysql`](https://crates.io/crates/mysql) are supported.
`refinery` works best with [`Barrel`](https://crates.io/crates/barrel) but you can also have your migrations on .sql files or use any other Rust crate for schema generation.

## Usage

- Migrations can be defined in .sql files or Rust modules that must have a function called `migration` that returns a [`String`](https://doc.rust-lang.org/std/string/struct.String.html)
- Migrations, both .sql files and Rust modules must be named in the format `V{1}__{2}.rs ` where `{1}` represents the migration version and `{2}` the name.
- Migrations can be run either by embedding them on your Rust code with `embedded_migrations` and `include_migration_mods` macros, or via `refinery_cli`.

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

for more examples refer to the [`examples`](examples)

## Implementation details
refinery works by creating a table that keeps all the applied migrations versions and it's metadata. When you [run](https://docs.rs/refinery/latest/refinery/struct.Runner.html#method.run) the migrations `Runner`, Refinery compares the applied migrations with the the ones to be applied, checking for [divergent](https://docs.rs/refinery/0.1.10/refinery/struct.Runner.html#method.set_abort_divergent) and [missing](https://docs.rs/refinery/0.1.10/refinery/struct.Runner.html#method.set_abort_missing) and executing unapplied migrations.\
By default refinery runs each migration on a single transaction, alternatively you can also configure refinery to wrap the entire execution of all migrations on a single transaction by setting [set_grouped](https://docs.rs/refinery/latest/refinery/struct.Runner.html#method.set_grouped) to true.
### Rollback

refinery's design is based on [flyway](https://flywaydb.org/) and so, shares it's [perspective](https://flywaydb.org/documentation/command/undo#important-notes) on undo/rollback migrations. To undo/rollback a migration you have to generate a new one and write specificaly what you want to undo.

## Compatibility

refinery aims to support stable Rust, the previous Rust version, and nightly


## Async

For the momment only [tokio-postgres](https://crates.io/crates/tokio-postgres) is supported on master branch. To migrate async you have to call `Runner`'s [run_async](https://github.com/rust-db/refinery/blob/master/refinery_migrations/src/lib.rs#L216).
More drivers are going to be supported soon and a new version release with them.\
For drivers that are not supported yet, best way to run migrations on an async context is to run them inside something like tokio's [`spawn_blocking`](https://docs.rs/tokio/0.2.4/tokio/task/fn.spawn_blocking.html), or if with Rusqlite, tokio's [`block_in_place`](https://docs.rs/tokio/0.2.0/tokio/task/fn.block_in_place.html).

## Contributing

:balloon: Thanks for your help improving the project!
No contribution is too small and all contributions are valued, feel free to open Issues and submit Pull Requests

## License

This project is licensed under the [MIT license](LICENSE).

### Contribution

Unless you explicitly state otherwise, any contribution intentionally submitted
for inclusion in refinery by you, shall be licensed as MIT, without any additional
terms or conditions.
