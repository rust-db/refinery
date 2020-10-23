use crate::error::{Error, Kind};
use std::path::{Path, PathBuf};
use std::{collections::HashSet, ffi::OsStr};
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
        let re_str = format!(r"^(U|V)(\d+(?:\.\d+)?)__(\w+)\.{}$", ext);
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

    let file_paths: Vec<PathBuf> = WalkDir::new(location)
        .into_iter()
        .filter_map(Result::ok)
        .map(DirEntry::into_path)
        .collect();

    let mut occurence: HashSet<String> = HashSet::with_capacity(file_paths.len());
    let mut result : Vec<PathBuf> = Vec::with_capacity(file_paths.len());

    for entry in &file_paths {
        if let Some(file_name) = entry.file_name().and_then(OsStr::to_str) {
            match re.captures(file_name) {
                Some(captures) => {
                    // Unwrap should always be safe due to how the regex is defined.
                    // The first and second matching groups are not optional, so it must
                    // at least have those two for the captures to be Some.
                    let version = captures.get(2).unwrap();

                    if occurence.insert(version.as_str().to_owned()) {
                        result.push(entry.to_owned());
                    } else {
                        return Err(Error::new(Kind::DuplicateVersion, None));
                    }
                }
                ,None => {
                    log::warn!(
                            "File \"{}\" does not adhere to the migration naming convention. Migrations must be named in the format [U|V]{{1}}__{{2}}.sql or [U|V]{{1}}__{{2}}.rs, where {{1}} represents the migration version and {{2}} the name.",
                            file_name
                        );
                }
                
            }
        }
    }


    Ok(result.into_iter())
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
    fn finds_unversioned_migrations() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();
        let sql1 = migrations_dir.join("U1__first.sql");
        fs::File::create(&sql1).unwrap();
        let sql2 = migrations_dir.join("U2__second.sql");
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


    #[test]
    fn returns_error_if_two_files_have_same_version() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();
        let sql1 = migrations_dir.join("V1__first.sql");
        fs::File::create(&sql1).unwrap();
        let sql2 = migrations_dir.join("V1__second.sql");
        fs::File::create(&sql2).unwrap();

        assert!(find_migration_files(migrations_dir, MigrationType::Sql).is_err());
    }
}
