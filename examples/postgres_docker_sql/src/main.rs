use tokio_postgres::NoTls;

// macro que gera o módulo `embedded::migrations`
mod embedded {
    use refinery::embed_migrations;
    embed_migrations!("migrations");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 1) URL do banco
    let db_url = std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:password@localhost:5432/postgres".into());

    // 2) conecta com tokio-postgres
    let (client, connection) = tokio_postgres::connect(&db_url, NoTls).await?;

    // 3) roda a parte de I/O do driver em background
    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });

    // 4) precisa de &mut Client para AsyncMigrate
    let mut client = client;

    // 5) executa as migrações
    let report = embedded::migrations::runner()
        .run_async(&mut client)  // agora compila, porque Client: AsyncMigrate
        .await?;

    println!("Migrações aplicadas com sucesso:");
    for m in report.applied_migrations() {
        println!("→ {} (versão {})", m.name(), m.version());
    }

    Ok(())
}
