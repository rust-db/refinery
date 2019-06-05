use barrel::{types, Migration};

#[cfg(feature = "sqlite")]
use barrel::backend::Sqlite as Sql;

#[cfg(feature = "postgresql")]
use barrel::backend::Pg as Sql;

#[cfg(feature = "mysql")]
use barrel::backend::MySql as Sql;

#[cfg(any(feature = "sqlite", feature = "postgresql", feature = "mysql"))]
pub fn migration() -> String {
    let mut m = Migration::new();

    m.create_table("cars", |t| {
        t.add_column("id", types::integer());
        t.add_column("name", types::varchar(255));
    });

    m.make::<Sql>()
}