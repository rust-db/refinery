use rusqlite::Connection;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("refinery/examples/embedded/migrations");
}

fn main() {
    let mut conn = Connection::open_in_memory().unwrap();
    embedded::migrations::runner().run(&mut conn).unwrap();
}

fn iter_main() {
    let mut conn = Connection::open_in_memory().unwrap();
    let runner = embedded::migrations::runner();
    for migration in runner.run_stepwise(&mut conn) {
        info!("Got a migration: {}", migration.expect("migration failed!"));
    }
}
