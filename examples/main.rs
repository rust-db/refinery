use rusqlite::Connection;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("refinery/examples/embedded/migrations");
}

fn main() {
    let mut conn = Connection::open_in_memory().unwrap();
    embedded::migrations::runner(&mut conn).run().unwrap();
}

fn iter_main() {
    let mut conn = Connection::open_in_memory().unwrap();
    for migration in embedded::migrations::runner(&mut conn) {
        info!("Got a migration: {}", migration.expect("migration failed!"));
    }
}
