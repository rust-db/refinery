use rusqlite::Connection;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("refinery/examples/embedded/migrations");
}

fn main() {
    let mut conn = Connection::open_in_memory().unwrap();

    // run all migrations in one go
    embedded::migrations::runner().run(&mut conn).unwrap();

    // or create an iterator over migrations as they run
    for migration in embedded::migrations::runner().run_iter(&mut conn) {
        info!("Got a migration: {}", migration.expect("migration failed!"));
    }
}
