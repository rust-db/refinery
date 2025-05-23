use barrel::{backend::Sqlite, types, Migration};

use crate::Sql;

pub fn up() -> String {
    let mut m = Migration::new();

    m.create_table("persons", |t| {
        t.add_column("id", types::primary());
        t.add_column("name", types::varchar(255));
        t.add_column("city", types::varchar(255));
    });

    m.make::<Sql>()
}

pub fn down() -> String {
    let mut m = Migration::new();

    m.drop_table("persons");

    m.make::<Sql>()
}
