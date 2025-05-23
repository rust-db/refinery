use barrel::{backend::Sqlite, types, Migration};

pub fn down() -> String {
    let mut m = Migration::new();

    m.drop_table("cars");

    m.make::<Sqlite>()
}
