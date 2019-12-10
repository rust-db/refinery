use barrel::{types, Migration, backend::Sqlite};

pub fn migration() -> String {
    let mut m = Migration::new();

    m.create_table("cars", |t| {
        t.add_column("id", types::integer());
        t.add_column("name", types::varchar(255));
    });

    m.make::<Sqlite>()
}