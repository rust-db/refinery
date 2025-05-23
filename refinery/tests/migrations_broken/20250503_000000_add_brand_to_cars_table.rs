use barrel::{backend::Sqlite, types, Migration};

pub fn up() -> String {
    let mut m = Migration::new();

    m.change_table("non_existent", |t| {
        t.add_column("brand", types::varchar(255).nullable(true));
    });

    m.make::<Sqlite>()
}

pub fn down() -> String {
    "".to_string()
}
