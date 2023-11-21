use crate::error::{Error, Kind};
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
        let re_str = format!(r"^.*\.{}$", ext);
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
        // Filter by migration type encoded in file extension.
        .filter(
            move |entry| match entry.file_name().and_then(OsStr::to_str) {
                Some(file_name) if re.is_match(file_name) => true,
                Some(file_name) => {
                    log::warn!("Filename \"{}\" has not supported extension.", file_name);
                    false
                }
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
    fn ignores_files_without_supported_file_extension() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();
        let file1 = migrations_dir.join("V1__first.txt");
        fs::File::create(&file1).unwrap();

        let mut all = find_migration_files(migrations_dir, MigrationType::All).unwrap();
        assert!(all.next().is_none());

        let sql_migrations_dir = tmp_dir.path().join("migrations");
        let mut sqls = find_migration_files(sql_migrations_dir, MigrationType::Sql).unwrap();
        assert!(sqls.next().is_none());
    }

    #[test]
    fn finds_files_with_supported_file_extension() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();
        let file1 = migrations_dir.join("V1__all_good.rs");
        fs::File::create(&file1).unwrap();
        let file2 = migrations_dir.join("V2_invalid_format_but_good_extension.sql");
        fs::File::create(&file2).unwrap();

        let sqls: Vec<PathBuf> = find_migration_files(migrations_dir, MigrationType::Sql)
            .unwrap()
            .collect();
        assert_eq!(file2.canonicalize().unwrap(), sqls[0]);

        let all_migrations_dir = tmp_dir.path().join("migrations");
        let mut all: Vec<PathBuf> = find_migration_files(all_migrations_dir, MigrationType::All)
            .unwrap()
            .collect();
        all.sort();
        assert_eq!(file1.canonicalize().unwrap(), all[0]);
        assert_eq!(file2.canonicalize().unwrap(), all[1]);
    }
}
