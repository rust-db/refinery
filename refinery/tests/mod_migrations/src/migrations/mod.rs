#[cfg(any(feature = "sqlite", feature = "postgresql", feature = "mysql"))]
use refinery::include_migration_mods;

#[cfg(any(feature = "sqlite", feature = "postgresql", feature = "mysql"))]
include_migration_mods!("refinery/tests/mod_migrations/src/migrations/");
