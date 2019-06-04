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

    m.change_table("cars", |t| {
        t.add_column("brand", types::varchar(255).nullable(true));
    });

    m.make::<Sql>()
}