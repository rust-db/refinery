use barrel::{backend::Sqlite, types, Migration};

pub fn up() -> String {
    let mut m = Migration::new();

    m.create_table("cars", |t| {
        t.add_column("id", types::integer().primary(true));
        t.add_column("name", types::varchar(255).nullable(false));
    });

    m.make::<Sqlite>()
}
