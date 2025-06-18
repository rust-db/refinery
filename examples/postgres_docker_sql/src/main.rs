use tokio_postgres::NoTls;

mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:password@localhost:5432/postgres".into());
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    let mut client = client;
    let report = embedded::migrations::runner()
        .run_async(&mut client)
        .await?;

    println!("Migrations applied with success:");
    for m in report.applied_migrations() {
        println!("→ {} (version {})", m.name(), m.version());
    }

    Ok(())
}
