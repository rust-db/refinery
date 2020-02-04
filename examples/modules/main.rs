use rusqlite::Connection;

mod migrations;

fn main() {
    let mut conn = Connection::open_in_memory().unwrap();

    migrations::runner().run(&mut conn).unwrap();
}
