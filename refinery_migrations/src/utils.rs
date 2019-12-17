use crate::Error;
use regex::Regex;
use std::ffi::OsStr;
use std::path::Path;
use std::{env, fs};
use walkdir::{DirEntry, WalkDir};

// regex used to match file names
pub fn file_match_re() -> Regex {
    Regex::new(r"^([V])([\d|\.]+)__(\w+)").unwrap()
}

lazy_static::lazy_static! {
    pub static ref RE: regex::Regex = file_match_re();
}

pub enum MigrationType {
    Mod,
    Sql,
}

// finds migrations file names given a type and optional parent location, use by refinery-macros and refinery-cli
pub fn find_migrations_filenames(
    location: Option<&Path>,
    mtype: MigrationType,
    full: bool,
) -> Result<Vec<String>, Error> {
    //if parent dir was provided start from it, if not start from current dir
    let start = match location {
        Some(location) => fs::canonicalize(location)
            .map_err(|err| Error::InvalidMigrationPath(location.to_path_buf(), err))?,
        None => env::current_dir().map_err(|err| {
            Error::InvalidMigrationPath(fs::canonicalize("./").unwrap_or_default(), err)
        })?,
    };

    let file_paths = WalkDir::new(start.as_path())
        .into_iter()
        .filter_map(Result::ok)
        .map(DirEntry::into_path)
        .filter(|entry| {
            entry
                .parent()
                .filter(|parent| {
                    //if parent_dir was not provided check if file is on a migrations dir
                    location.is_some() || parent.ends_with("migrations")
                })
                .is_some()
        })
        //filter entries which match the valid migration regex
        .filter(|entry| {
            entry
                .file_name()
                .and_then(OsStr::to_str)
                .filter(|path| RE.is_match(path))
                .is_some()
        })
        //match the right extension
        .filter(|entry| {
            entry
                .extension()
                .and_then(OsStr::to_str)
                .filter(|ext| match mtype {
                    MigrationType::Mod => ext == &"rs",
                    MigrationType::Sql => ext == &"sql",
                })
                .is_some()
        })
        //if full is false get the name of the file without extension
        .filter_map(|entry| {
            if full {
                entry.into_os_string().into_string().ok()
            } else {
                entry
                    .file_stem()
                    .and_then(|file| file.to_os_string().into_string().ok())
            }
        })
        .collect();
    Ok(file_paths)
}

#[cfg(test)]
mod tests {
    use super::{find_migrations_filenames, MigrationType};
    use std::fs;
    use std::path::Path;
    use tempfile::TempDir;

    #[test]
    fn finds_mod_migrations() {
        let tmp_dir = TempDir::new_in(".").unwrap();
        let _migrations_dir = fs::create_dir(tmp_dir.path().join("migrations")).unwrap();
        let mod1 = tmp_dir.path().join("migrations/V1__first.rs");
        fs::File::create(&mod1).unwrap();
        let mod2 = tmp_dir.path().join("migrations/V2__second.rs");
        fs::File::create(&mod2).unwrap();

        let mut mods = find_migrations_filenames(None, MigrationType::Mod, false).unwrap();
        mods.sort();
        assert_eq!("V1__first", mods[0]);
        assert_eq!("V2__second", mods[1]);
    }

    #[test]
    fn finds_mod_migrations_in_parent_dir() {
        let tmp_dir = TempDir::new_in(".").unwrap();
        let mod1 = tmp_dir.path().join("V1__first.rs");
        fs::File::create(&mod1).unwrap();
        let mod2 = tmp_dir.path().join("V2__second.rs");
        fs::File::create(&mod2).unwrap();

        let mut mods =
            find_migrations_filenames(Some(tmp_dir.path()), MigrationType::Mod, false).unwrap();
        mods.sort();
        assert_eq!("V1__first", mods[0]);
        assert_eq!("V2__second", mods[1]);
    }

    #[test]
    fn ignores_mod_files_without_migration_regex_match() {
        let tmp_dir = TempDir::new_in(".").unwrap();
        let _migrations_dir = fs::create_dir(tmp_dir.path().join("migrations")).unwrap();
        let mod1 = tmp_dir.path().join("migrations/V1first.rs");
        fs::File::create(&mod1).unwrap();
        let mod2 = tmp_dir.path().join("migrations/V2second.rs");
        fs::File::create(&mod2).unwrap();

        let mods =
            find_migrations_filenames(Some(tmp_dir.path()), MigrationType::Mod, false).unwrap();
        assert!(mods.is_empty());
    }

    #[test]
    fn finds_sql_migrations() {
        let tmp_dir = TempDir::new_in("./").unwrap();
        let _migrations_dir = fs::create_dir(tmp_dir.path().join("migrations")).unwrap();
        let sql1 = tmp_dir.path().join("migrations/V1__first.sql");
        fs::File::create(&sql1).unwrap();
        let sql2 = tmp_dir.path().join("migrations/V2__second.sql");
        fs::File::create(&sql2).unwrap();

        let mut mods = find_migrations_filenames(None, MigrationType::Sql, true).unwrap();
        mods.sort();
        assert_eq!(sql1, Path::new(&mods[0]));
        assert_eq!(sql2, Path::new(&mods[1]));
    }

    #[test]
    fn finds_sql_migrations_in_parent_dir() {
        let tmp_dir = TempDir::new_in("./").unwrap();
        let sql1 = tmp_dir.path().join("V1__first.sql");
        fs::File::create(&sql1).unwrap();
        let sql2 = tmp_dir.path().join("V2__second.sql");
        fs::File::create(&sql2).unwrap();

        let mut mods =
            find_migrations_filenames(Some(tmp_dir.path()), MigrationType::Sql, true).unwrap();
        mods.sort();
        assert_eq!(sql1, Path::new(&mods[0]));
        assert_eq!(sql2, Path::new(&mods[1]));
    }

    #[test]
    fn ignores_sql_files_without_migration_regex_match() {
        let tmp_dir = TempDir::new().unwrap();
        let _migrations_dir = fs::create_dir(tmp_dir.path().join("migrations")).unwrap();
        let sql1 = tmp_dir.path().join("migrations/V1first.sql");
        fs::File::create(&sql1).unwrap();
        let sql2 = tmp_dir.path().join("migrations/V2second.sql");
        fs::File::create(&sql2).unwrap();

        let mods =
            find_migrations_filenames(Some(tmp_dir.path()), MigrationType::Sql, true).unwrap();
        assert!(mods.is_empty());
    }
}
