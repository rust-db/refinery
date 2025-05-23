use barrel::{types, Migration};

use crate::Sql;

pub fn up() -> String {
    let mut m = Migration::new();

    m.change_table("motos", |t| {
        t.add_column("year", types::integer().nullable(true));
    });

    m.make::<Sql>()
}

pub fn down() -> String {
    let mut m = Migration::new();

    m.change_table("cars", |t| {
        t.drop_column("year");
    });

    m.make::<Sql>()
}
