use crate::error::{Error, Kind};
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

use regex::Regex;

/// enum containing the migration types used to search for migrations
/// either Rust Modules or Sql files
pub enum MigrationType {
    Mod,
    Sql,
}

impl MigrationType {
    fn file_match_re(&self) -> Regex {
        let ext = match self {
            MigrationType::Mod => "rs",
            MigrationType::Sql => "sql",
        };
        let re_str = format!(r"^(V)(\d+(?:\.\d+)?)__(\w+)\.{}$", ext);
        Regex::new(re_str.as_str()).unwrap()
    }
}

/// find migrations on file system recursively across directories given a location and [MigrationType]
pub fn find_migration_files(
    location: impl AsRef<Path>,
    migration_type: MigrationType,
) -> Result<impl Iterator<Item = PathBuf>, Error> {
    let re = migration_type.file_match_re();
    let location: &Path = location.as_ref();
    let location = location.canonicalize().map_err(|err| {
        Error::new(
            Kind::InvalidMigrationPath(location.to_path_buf(), err),
            None,
        )
    })?;

    let file_paths = WalkDir::new(location)
        .into_iter()
        .filter_map(Result::ok)
        .map(DirEntry::into_path)
        // filter by migration file regex
        .filter(
            move |entry| match entry.file_name().and_then(OsStr::to_str) {
                Some(file_name) => re.is_match(file_name),
                None => false,
            },
        );

    Ok(file_paths)
}

#[cfg(test)]
mod tests {
    use super::{find_migration_files, MigrationType};
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

        let mut mods: Vec<PathBuf> = find_migration_files(migrations_dir, MigrationType::Mod)
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

        let mut mods = find_migration_files(migrations_dir, MigrationType::Mod).unwrap();
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

        let mut mods: Vec<PathBuf> = find_migration_files(migrations_dir, MigrationType::Sql)
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

        let mut mods = find_migration_files(migrations_dir, MigrationType::Sql).unwrap();
        assert!(mods.next().is_none());
    }
}
