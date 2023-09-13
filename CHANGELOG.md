# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.8.11] - 2023-09-13
### Changed
- Improve feature-set, remove non required features from dependencies [#286](https://github.com/rust-db/refinery/pull/286)

## [0.8.10] - 2023-05-20
### Changed
- Update mysql_async to allow 0.32, [#278](https://github.com/rust-db/refinery/pull/278)

## [0.8.9] - 2023-05-05
### Changed
- Add `no-default-features` to tiberius  dependency as tiberius features are non additive [#273](https://github.com/rust-db/refinery/pull/273)
- Increase the range of supported `postgres` versions [#273](https://github.com/rust-db/refinery/pull/274)

## [0.8.8] - 2023-04-28
### Changed
- Update tiberius to allow 0.12 [#271](https://github.com/rust-db/refinery/pull/271)
- Update non driver dependencies to latest available versions to allow 0.12 [#271](https://github.com/rust-db/refinery/pull/271)

## [0.8.7] - 2022-11-26
### Changed
- Update mysql to allow 23 [#229](https://github.com/rust-db/refinery/pull/257)

## [0.8.6] - 2022-08-15
### Changed
- Update mysql_async to allow 0.30, [#229](https://github.com/rust-db/refinery/pull/229)

## [0.8.4] - 2021-01-09
### Changed
- Allow setting a custom migration table name, [#207](https://github.com/rust-db/refinery/pull/207)

## [0.8.2] - 2021-01-05
### Changed
- Update mysql to allow 22, [#202](https://github.com/rust-db/refinery/pull/202)

## [0.8.1] - 2021-12-30
### Changed
- Update mysql to allow 0.29, [#164](https://github.com/rust-db/refinery/pull/199)
- Update rusqlite to allow 0.26, [#159](https://github.com/rust-db/refinery/pull/196)

## [0.7.0] - 2021-10-16
### Added
- Add `Target::Fake` and `Target::FakeVersion` to allow users to only update refinery's schema migration table without actually running the migration files
, [#179](https://github.com/rust-db/refinery/pull/179/)
- Add [tiberius](https://github.com/prisma/tiberius) support to `refinery` and `mssql` support to `refinery_cli` [#169](https://github.com/rust-db/refinery/pull/169)

### Changed
- `include_migration_mods` macro has been removed. Instead of that, use `embed_migrations` macro, and there is no need to have `mod.rs`. [#154](https://github.com/rust-db/refinery/pull/154)

### Removed
- Removal of "files" argument from refinery cli [#174](https://github.com/rust-db/refinery/pull/174)

## [0.6.0] - 2021-07-10
### Changed
- Update mysql to 21, [#164](https://github.com/rust-db/refinery/pull/164)
- Update mysql_async to 0.28, [#164](https://github.com/rust-db/refinery/pull/164)
- Update rusqlite to 0.25, [#159](https://github.com/rust-db/refinery/pull/159)

## [0.5.0] - 2020-12-31
### Added
- Detect repeated migrations on migrations to be applied and return Error on that situation, [#146](https://github.com/rust-db/refinery/pull/146/)

### Changed
- Update assert_cmd to 1.0, [#143](https://github.com/rust-db/refinery/pull/143/)
- Update env_logger to 0.8, [#143](https://github.com/rust-db/refinery/pull/143/)
- Update env_logger to 0.8, [#143](https://github.com/rust-db/refinery/pull/143/)
- Update cfg_if to 1.0, [#143](https://github.com/rust-db/refinery/pull/143/)
- Update postgres to 0.19, [#143](https://github.com/rust-db/refinery/pull/143/)
- Update tokio-postgres to 0.7, [#143](https://github.com/rust-db/refinery/pull/143/)
- Update mysql to 0.20, [#143](https://github.com/rust-db/refinery/pull/143/)

## [0.4.0] - 2020-10-13
### Added
- Warn when migration file name is malformed [#130](https://github.com/rust-db/refinery/pull/130)
- Add `Unversioned` migration type, [#128](https://github.com/rust-db/refinery/pull/128)
- Add `get_migrations` method to runner to allow inspecting gathered migrations, [#120](https://github.com/rust-db/refinery/pull/120)
- Add support for 'postgresql' url schema prefix, [#107](https://github.com/rust-db/refinery/pull/107)
- Add lib option to load config from a string, [1#13](https://github.com/rust-db/refinery/pull/113)
- Add lib and cli option to load config from env var, [#103](https://github.com/rust-db/refinery/pull/103)

### Fixed
- Fix `set_abort_missing`, it was setting the wrong variable, [#127](https://github.com/rust-db/refinery/pull/127)

### Changed
- Update mysql_async to 0.25, [#131](https://github.com/rust-db/refinery/pull/131/)
- Update mysql to 0.18, [#99](https://github.com/rust-db/refinery/pull/99/)

## [0.3.0] - 2020-05-19
### Added
- Rename Config.get_db_type to Config.db_type, [#95](https://github.com/rust-db/refinery/pull/95)
- Deprecate migrate_from_config and migrate_from_config_async, instead impl Migrate for Config, [#94](https://github.com/rust-db/refinery/pull/94)
- Update Runner.run and Runner.run_async return signature, Result<(), Error> -> Result<Report, Error> where report contains applied Migration's, [#92](https://github.com/rust-db/refinery/pull/92)
- Deprecate AppliedMigration, merge its functionality into Migration, [#91](https://github.com/rust-db/refinery/pull/91)
- Add Runner.get_applied_migrations_async method, [#90](https://github.com/rust-db/refinery/pull/90)
- Add Runner.get_applied_migrations method, [#90](https://github.com/rust-db/refinery/pull/90)
- Add Runner.get_last_applied_migration_async method, [#90](https://github.com/rust-db/refinery/pull/90)
- Add Runner.get_last_applied_migration method
- Add allow migrations to run up until a Target version, [#74](https://github.com/rust-db/refinery/pull/74)
- Use SipHasher13 instead of DefaultHasher [#63](https://github.com/rust-db/refinery/pull/63)

### Changed
- Update mysql_async dependency, 0.21 -> 0.23 [#94](https://github.com/rust-db/refinery/pull/94/files#diff-c265757db229c3cac93fd2e32bf4da58)
- Update rusqlite dependency, 0.21 -> 0.23 [#88](https://github.com/rust-db/refinery/pull/88)

## [0.2.1] - 2020-02-19
### Fixed
- Update cfg-if to 0.1.10 to fix backtrace bug [#66](https://github.com/rust-db/refinery/pull/66)

## [0.2.0] - 2019-12-20
### Added
- Add `tokio-postgres` driver support [#10](https://github.com/rust-db/refinery/pull/19).
- Add `mysql_async` driver support [#22](https://github.com/rust-db/refinery/pull/19).
- Add `migrate_from_config` function
- Add `migrate_from_config_async` function
- Update postgres to version 0.17 [#32](https://github.com/rust-db/refinery/pull/32)
- Allow refinery_cli to select driver via features [#32](https://github.com/rust-db/refinery/pull/32)

### Fixed
- allow multiple statements in migration files [#10](https://github.com/rust-db/refinery/issues/21)
- when building refinery_cli with default features, build with rusqlite bundled libsqlite3 [#33](https://github.com/rust-db/refinery/issues/21)
- rename ConnectionError to just Connection as it is a variant for Error enum, and add its source as source [#36](https://github.com/rust-db/refinery/issues/36)

### Changed
- update rusqlite dependency, 0.18 -> 0.21 [#26](https://github.com/rust-db/refinery/issues/26)

## [0.1.10] - 2019-12-10
### Added
- Initial release.
