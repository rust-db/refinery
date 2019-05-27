//package renamed to ttrusqlite for tests and examples, due to cargo features limitation
use ttrusqlite::Connection;

mod migrations;


fn main() {
    let mut conn = Connection::open_in_memory().unwrap();

    migrations::runner().run(&mut conn).unwrap();
}