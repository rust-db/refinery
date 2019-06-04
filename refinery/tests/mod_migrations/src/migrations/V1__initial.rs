use barrel::{types, Migration};

#[cfg(feature = "sqlite")]
use barrel::backend::Sqlite as Sql;

#[cfg(feature = "pg")]
use barrel::backend::Pg as Sql;

#[cfg(feature = "mysql")]
use barrel::backend::MySql as Sql;

#[cfg(any(feature = "sqlite", feature = "pg", feature = "mysql"))]
pub fn migration() -> String {
    let mut m = Migration::new();

    m.create_table("persons", |t| {
        t.add_column("id", types::primary());
        t.add_column("name", types::varchar(255));
        t.add_column("city", types::varchar(255));
    });

    m.make::<Sql>()
}