use barrel::{types, Migration};

use crate::Sql;

pub fn migration() -> String {
    let mut m = Migration::new();

    m.create_table("cars", |t| {
        t.add_column("id", types::integer());
        t.add_column("name", types::varchar(255));
    });

    m.create_table("motos", |t| {
        t.add_column("id", types::integer());
        t.add_column("name", types::varchar(255));
    });

    m.make::<Sql>()
}
