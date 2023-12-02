// needed to be able to embed the other migrations, the surrealdb migrations don't need that
use barrel::backend::Pg as Sql;

#[cfg(feature = "surrealdb")]
mod surreal {
    use refinery_core::surrealdb::engine::local::{Db, Mem};
    use refinery_core::Migration;
    use refinery_core::{surrealdb, AsyncMigrate};
    use refinery_macros::embed_migrations;
    use serde::{Deserialize, Serialize};
    use serde_json;
    use time::OffsetDateTime;

    const DEFAULT_TABLE_NAME: &str = "refinery_schema_history";

    fn get_migrations() -> Vec<Migration> {
        embed_migrations!("./tests/migrations_surreal");

        let migration1 =
            Migration::unapplied("V1__initial.rs", &migrations::V1__initial::migration()).unwrap();

        let migration2 = Migration::unapplied(
            "V2__add_email.surql",
            include_str!("./migrations_surreal/V2__add_email.surql"),
        )
        .unwrap();

        let migration3 = Migration::unapplied("V3__add_cars_table", "DEFINE TABLE cars;").unwrap();

        vec![migration1, migration2, migration3]
    }

    async fn get_db() -> surrealdb::Surreal<Db> {
        let db = surrealdb::Surreal::new::<Mem>(()).await.unwrap();
        db.use_ns("refinery_test")
            .use_db("refinery_test")
            .await
            .unwrap();

        db
    }

    mod embedded {
        use refinery::embed_migrations;
        embed_migrations!("./tests/migrations_surreal");
    }

    #[tokio::test]
    async fn report_contains_applied_migrations() {
        let mut db = get_db().await;

        let report = embedded::migrations::runner()
            .run_async(&mut db)
            .await
            .unwrap();

        let migrations = get_migrations();
        let applied_migrations = report.applied_migrations();

        assert_eq!(2, applied_migrations.len());

        assert_eq!(migrations[0].version(), applied_migrations[0].version());
        assert_eq!(migrations[1].version(), applied_migrations[1].version());

        assert_eq!(migrations[0].name(), applied_migrations[0].name());
        assert_eq!(migrations[1].name(), applied_migrations[1].name());

        assert_eq!(migrations[0].checksum(), applied_migrations[0].checksum());
        assert_eq!(migrations[1].checksum(), applied_migrations[1].checksum());
    }

    #[tokio::test]
    async fn creates_migration_table() {
        let mut db = get_db().await;

        embedded::migrations::runner()
            .run_async(&mut db)
            .await
            .unwrap();

        #[derive(Deserialize)]
        struct InfoResult {
            fields: serde_json::Value,
        }

        let result: Option<InfoResult> = db
            .query(format!("INFO FOR TABLE {};", DEFAULT_TABLE_NAME))
            .await
            .unwrap()
            .take(0)
            .unwrap();

        // assert that 4 fields exist: applied_on, checksum, name, version
        assert_eq!(4, result.unwrap().fields.as_object().unwrap().len());
    }

    #[tokio::test]
    async fn creates_migration_table_grouped_migrations() {
        let mut db = get_db().await;

        embedded::migrations::runner()
            .set_grouped(true)
            .run_async(&mut db)
            .await
            .unwrap();

        #[derive(Deserialize)]
        struct InfoResult {
            fields: serde_json::Value,
        }

        let result: Option<InfoResult> = db
            .query(format!("INFO FOR TABLE {};", DEFAULT_TABLE_NAME))
            .await
            .unwrap()
            .take(0)
            .unwrap();

        // assert that 4 fields exist: applied_on, checksum, name, version
        assert_eq!(4, result.unwrap().fields.as_object().unwrap().len());
    }

    #[tokio::test]
    async fn applies_migration_grouped() {
        let mut db = get_db().await;

        embedded::migrations::runner()
            .set_grouped(true)
            .run_async(&mut db)
            .await
            .unwrap();

        #[derive(Debug, Serialize, Deserialize)]
        struct User {
            first_name: String,
            last_name: String,
            email: String,
        }

        let result: Option<User> = db
            .create(("user", "john"))
            .content(User {
                first_name: "John".into(),
                last_name: "Doe".into(),
                email: "john@example.com".into(),
            })
            .await
            .unwrap();

        assert_eq!(result.unwrap().email, "john@example.com");
    }

    #[tokio::test]
    async fn updates_schema_history() {
        let mut db = get_db().await;

        embedded::migrations::runner()
            .run_async(&mut db)
            .await
            .unwrap();

        let current = db
            .get_last_applied_migration(DEFAULT_TABLE_NAME)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(2, current.version());
        assert_eq!(
            OffsetDateTime::now_utc().date(),
            current.applied_on().unwrap().date()
        );
    }

    #[tokio::test]
    async fn updates_schema_history_grouped() {
        let mut db = get_db().await;

        embedded::migrations::runner()
            .set_grouped(true)
            .run_async(&mut db)
            .await
            .unwrap();

        let current = db
            .get_last_applied_migration(DEFAULT_TABLE_NAME)
            .await
            .unwrap()
            .unwrap();

        assert_eq!(2, current.version());
        assert_eq!(
            OffsetDateTime::now_utc().date(),
            current.applied_on().unwrap().date()
        );
    }
}
