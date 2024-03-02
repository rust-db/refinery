use crate::error::{Error, Kind};
use crate::Migration;
use regex::Regex;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

/// enum containing the migration types used to search for migrations
/// either just .sql files or both .sql and .rs
pub enum MigrationType {
    All,
    Sql,
}

impl MigrationType {
    fn file_match_re(&self) -> Regex {
        let ext = match self {
            MigrationType::All => "(rs|sql)",
            MigrationType::Sql => "sql",
        };
        let re_str = format!(r"^(U|V)(\d+(?:\.\d+)?)__(\w+)\.{}$", ext);
        Regex::new(re_str.as_str()).unwrap()
    }
}

/// find migrations on file system recursively across directories given a location and [MigrationType]
pub fn find_migration_files(
    location: impl AsRef<Path>,
    migration_type: MigrationType,
) -> Result<impl Iterator<Item = PathBuf>, Error> {
    let location: &Path = location.as_ref();
    let location = location.canonicalize().map_err(|err| {
        Error::new(
            Kind::InvalidMigrationPath(location.to_path_buf(), err),
            None,
        )
    })?;

    let re = migration_type.file_match_re();
    let file_paths = WalkDir::new(location)
        .into_iter()
        .filter_map(Result::ok)
        .map(DirEntry::into_path)
        // filter by migration file regex
        .filter(
            move |entry| match entry.file_name().and_then(OsStr::to_str) {
                Some(file_name) if re.is_match(file_name) => true,
                Some(file_name) => {
                    log::warn!(
                        "File \"{}\" does not adhere to the migration naming convention. Migrations must be named in the format [U|V]{{1}}__{{2}}.sql or [U|V]{{1}}__{{2}}.rs, where {{1}} represents the migration version and {{2}} the name.",
                        file_name
                    );
                    false
                }
                None => false,
            },
        );

    Ok(file_paths)
}

/// Loads SQL migrations from a path. This enables dynamic migration discovery, as opposed to
/// embedding. The resulting collection is ordered by version.
pub fn load_sql_migrations(location: impl AsRef<Path>) -> Result<Vec<Migration>, Error> {
    let migration_files = find_migration_files(location, MigrationType::Sql)?;

    let mut migrations = vec![];

    for path in migration_files {
        let sql = std::fs::read_to_string(path.as_path())
            .map_err(|e| Error::new(Kind::InvalidMigrationFile(path.to_owned(), e), None))?;

        //safe to call unwrap as find_migration_filenames returns canonical paths
        let filename = path
            .file_stem()
            .and_then(|file| file.to_os_string().into_string().ok())
            .unwrap();

        let migration = Migration::unapplied(&filename, &sql)?;
        migrations.push(migration);
    }

    migrations.sort();
    Ok(migrations)
}

#[cfg(test)]
mod tests {
    use super::{find_migration_files, load_sql_migrations, MigrationType};
    use std::fs;
    use std::path::PathBuf;
    use tempfile::TempDir;

    #[test]
    fn finds_mod_migrations() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();
        let sql1 = migrations_dir.join("V1__first.rs");
        fs::File::create(&sql1).unwrap();
        let sql2 = migrations_dir.join("V2__second.rs");
        fs::File::create(&sql2).unwrap();

        let mut mods: Vec<PathBuf> = find_migration_files(migrations_dir, MigrationType::All)
            .unwrap()
            .collect();
        mods.sort();
        assert_eq!(sql1.canonicalize().unwrap(), mods[0]);
        assert_eq!(sql2.canonicalize().unwrap(), mods[1]);
    }

    #[test]
    fn ignores_mod_files_without_migration_regex_match() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();
        let sql1 = migrations_dir.join("V1first.rs");
        fs::File::create(&sql1).unwrap();
        let sql2 = migrations_dir.join("V2second.rs");
        fs::File::create(&sql2).unwrap();

        let mut mods = find_migration_files(migrations_dir, MigrationType::All).unwrap();
        assert!(mods.next().is_none());
    }

    #[test]
    fn finds_sql_migrations() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();
        let sql1 = migrations_dir.join("V1__first.sql");
        fs::File::create(&sql1).unwrap();
        let sql2 = migrations_dir.join("V2__second.sql");
        fs::File::create(&sql2).unwrap();

        let mut mods: Vec<PathBuf> = find_migration_files(migrations_dir, MigrationType::All)
            .unwrap()
            .collect();
        mods.sort();
        assert_eq!(sql1.canonicalize().unwrap(), mods[0]);
        assert_eq!(sql2.canonicalize().unwrap(), mods[1]);
    }

    #[test]
    fn finds_unversioned_migrations() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();
        let sql1 = migrations_dir.join("U1__first.sql");
        fs::File::create(&sql1).unwrap();
        let sql2 = migrations_dir.join("U2__second.sql");
        fs::File::create(&sql2).unwrap();

        let mut mods: Vec<PathBuf> = find_migration_files(migrations_dir, MigrationType::All)
            .unwrap()
            .collect();
        mods.sort();
        assert_eq!(sql1.canonicalize().unwrap(), mods[0]);
        assert_eq!(sql2.canonicalize().unwrap(), mods[1]);
    }

    #[test]
    fn ignores_sql_files_without_migration_regex_match() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();
        let sql1 = migrations_dir.join("V1first.sql");
        fs::File::create(&sql1).unwrap();
        let sql2 = migrations_dir.join("V2second.sql");
        fs::File::create(&sql2).unwrap();

        let mut mods = find_migration_files(migrations_dir, MigrationType::All).unwrap();
        assert!(mods.next().is_none());
    }

    #[test]
    fn loads_migrations_from_path() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();
        let sql1 = migrations_dir.join("V1__first.sql");
        fs::File::create(&sql1).unwrap();
        let sql2 = migrations_dir.join("V2__second.sql");
        fs::File::create(&sql2).unwrap();
        let rs3 = migrations_dir.join("V3__third.rs");
        fs::File::create(&rs3).unwrap();

        let migrations = load_sql_migrations(migrations_dir).unwrap();
        assert_eq!(migrations.len(), 2);
        assert_eq!(&migrations[0].to_string(), "V1__first");
        assert_eq!(&migrations[1].to_string(), "V2__second");
    }
}
