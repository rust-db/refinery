use rusqlite::Connection;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("refinery/examples/embedded/migrations");
}

fn main() {
    let mut conn = Connection::open_in_memory().unwrap();
    embedded::migrations::runner().run(&mut conn).unwrap();
}
