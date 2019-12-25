### v0.2.0 (December 2019)

- **Features**:

  - Add `tokio-postgres` driver support [#10](https://github.com/rust-db/refinery/pull/19).
  - Add `mysql_async` driver suport [#22](https://github.com/rust-db/refinery/pull/19).
  - Add `migrate_from_config` function
  - Add `migrate_from_config_async` function
  - Update postgres to version 0.17 [#32](https://github.com/rust-db/refinery/pull/32)
  - Allow refinery_cli to select driver via features [#32](https://github.com/rust-db/refinery/pull/32)

- **Bugfixes**:
  - allow multiple statements in migration files [#10](https://github.com/rust-db/refinery/issues/21)
  - when building refinery_cli with default features, build with rusqlite bundled libsqlite3 [#33](https://github.com/rust-db/refinery/issues/21)

- **Dependencies**:
  - update rusqlite dependency, 0.18 -> 0.21 [#26](https://github.com/rust-db/refinery/issues/26)

## v0.1.10 (December 10, 2010)

- Intial release.
