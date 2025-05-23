use barrel::{backend::Sqlite, types, Migration};

use crate::Sql;

pub fn up() -> String {
    let mut m = Migration::new();

    m.change_table("cars", |t| {
        t.add_column("brand", types::varchar(255).nullable(true));
    });

    m.make::<Sql>()
}

pub fn down() -> String {
    let mut m = Migration::new();

    m.change_table("cars", |t| {
        t.drop_column("brand");
    });

    m.make::<Sql>()
}
