use barrel::{types, Migration, backend::Sqlite};

pub fn migration() -> String {
    let mut m = Migration::new();

    m.change_table("cars", |t| {
        t.add_column("brand", types::varchar(255).nullable(true));
    });

    m.make::<Sqlite>()
}