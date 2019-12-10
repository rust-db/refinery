use refinery::include_migration_mods;

//by default there is no need to specify the location
//we need to specify here because there is also another migrations dir in tests
include_migration_mods!("refinery/examples/modules/migrations");
