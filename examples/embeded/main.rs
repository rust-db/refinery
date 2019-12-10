//package renamed to ttrusqlite for tests and examples, due to cargo features limitation
use ttrusqlite::Connection;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("refinery/examples/embeded/migrations");
}

fn main() {
    let mut conn = Connection::open_in_memory().unwrap();
    embedded::migrations::runner().run(&mut conn).unwrap();
}
