use refinery::embed_migrations;

//by default there is no need to specify the location
//we need to specify here because there is also another migrations dir in tests
embed_migrations!("refinery/examples/modules/migrations");
