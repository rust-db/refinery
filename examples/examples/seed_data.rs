use std::{error::Error, ffi::OsStr};

use rusqlite::Connection;

refinery::embed_migrations!("seed_migrations");

fn main() -> Result<(), Box<dyn Error>> {
    let usage = "usage: seed_data <database> [--seed]";
    let mut args = std::env::args_os().skip(1);
    let path = args.next().ok_or(usage)?;
    let seed = match args.next().as_deref() {
        None => false,
        Some(flag) if flag == OsStr::new("--seed") && args.next().is_none() => true,
        Some(_) => return Err(usage.into()),
    };

    let mut conn = Connection::open(path)?;
    migrations::runner().run(&mut conn)?;

    if seed {
        conn.execute(
            "INSERT INTO demo_projects (slug, name) VALUES (?1, ?2)
             ON CONFLICT (slug) DO NOTHING",
            ("example", "Example project"),
        )?;
    }

    let count: i64 = conn.query_row("SELECT COUNT(*) FROM demo_projects", [], |row| row.get(0))?;
    println!("demo_projects rows: {count}");
    Ok(())
}
