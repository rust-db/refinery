use crate::error::{Error, Kind};
use crate::runner::Type;
use regex::Regex;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use walkdir::{DirEntry, WalkDir};

const STEM_RE: &'static str = r"^([U|V])(\d+(?:\.\d+)?)__(\w+)";

/// Matches the stem of a migration file
fn file_stem_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(STEM_RE).unwrap())
}

/// Matches the stem + extension of a SQL migration file
fn file_re_sql() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new([STEM_RE, r"\.sql$"].concat().as_str()).unwrap())
}

/// Matches the stem + extension of any migration file
fn file_re_all() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new([STEM_RE, r"\.(rs|sql)$"].concat().as_str()).unwrap())
}

/// enum containing the migration types used to search for migrations
/// either just .sql files or both .sql and .rs
pub enum MigrationType {
    All,
    Sql,
}

impl MigrationType {
    fn file_match_re(&self) -> &'static Regex {
        match self {
            MigrationType::All => file_re_all(),
            MigrationType::Sql => file_re_sql(),
        }
    }
}

/// Parse a migration filename stem into a prefix, version, and name
pub fn parse_migration_name(name: &str) -> Result<(Type, i32, String), Error> {
    let captures = file_stem_re()
        .captures(name)
        .filter(|caps| caps.len() == 4)
        .ok_or_else(|| Error::new(Kind::InvalidName, None))?;
    let version: i32 = captures[2]
        .parse()
        .map_err(|_| Error::new(Kind::InvalidVersion, None))?;

    let name: String = (&captures[3]).into();
    let prefix = match &captures[1] {
        "V" => Type::Versioned,
        "U" => Type::Unversioned,
        _ => unreachable!(),
    };

    Ok((prefix, version, name))
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
        fs::File::create(sql1).unwrap();
        let sql2 = migrations_dir.join("V2second.rs");
        fs::File::create(sql2).unwrap();

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
        fs::File::create(sql1).unwrap();
        let sql2 = migrations_dir.join("V2second.sql");
        fs::File::create(sql2).unwrap();

        let mut mods = find_migration_files(migrations_dir, MigrationType::All).unwrap();
        assert!(mods.next().is_none());
    }
}
