#![allow(unused_imports)]
use barrel::backend::Sqlite as Sql;
use log::info;
use refinery::Migration;
use rusqlite::Connection;

refinery::embed_migrations!("migrations");

fn main() {
    env_logger::init();

    let mut conn = Connection::open_in_memory().unwrap();

    let use_iteration = std::env::args().any(|a| a.to_lowercase().eq("--iterate"));

    if use_iteration {
        // create an iterator over migrations as they run
        for migration in migrations::runner().run_iter(&mut conn) {
            process_migration(migration.expect("Migration failed!"));
        }
    } else {
        // or run all migrations in one go
        migrations::runner().run(&mut conn).unwrap();
    }
}

fn process_migration(migration: Migration) {
    #[cfg(not(feature = "enums"))]
    {
        // run something after each migration
        info!("Post-processing a migration: {}", migration)
    }

    #[cfg(feature = "enums")]
    {
        // or with the `enums` feature enabled, match against migrations to run specific post-migration steps
        use migrations::EmbeddedMigration;
        match migration.into() {
            EmbeddedMigration::Initial(m) => info!("V{}: Initialized the database!", m.version()),
            m => info!("Got a migration: {:?}", m),
        }
    }
}
