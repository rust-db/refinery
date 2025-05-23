use crate::error::{Error, Kind};
use crate::runner::Type;
use crate::Migration;
use regex::Regex;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use walkdir::{DirEntry, WalkDir};

const STEM_RE: &str = r"^([U|V])(\d+(?:\.\d+)?)__(\w+)";
const DIR_RE: &str = r"^\(d{14})_([a-z0-9\_]+)$";
const UPDOWN_RE: &str = r"^(up|down)";

/// Matches the stem of a migration file.
fn file_stem_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(STEM_RE).unwrap())
}

/// Matches the stem + extension of a SQL migration file.
fn file_re_sql() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new([STEM_RE, r"\.sql$"].concat().as_str()).unwrap())
}

/// Matches the stem + extension of any migration file.
fn file_re_all() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new([STEM_RE, r"\.(rs|sql)$"].concat().as_str()).unwrap())
}

/// Matches the stem + extension of a directory migration file.
fn updown_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new([UPDOWN_RE, r"\.(rs|sql)$"].concat().as_str()).unwrap())
}

/// Matches the stem of a directory migration file.
fn dir_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(DIR_RE).unwrap())
}

/// enum containing the migration types used to search for migrations
/// either just .sql files or both .sql and .rs
pub enum MigrationType {
    All,
    Sql,
    Directory,
}

impl MigrationType {
    fn file_match_re(&self) -> &'static Regex {
        match self {
            MigrationType::All => file_re_all(),
            MigrationType::Sql => file_re_sql(),
            MigrationType::Directory => dir_re(),
        }
    }
}

/// Parse a migration filename stem into a prefix, version, and name.
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

#[derive(Debug, PartialEq, Eq)]
pub enum MigrationPath {
    File(PathBuf),
    Directory(PathBuf),
}

impl MigrationPath {
    pub fn as_path(&self) -> &Path {
        match self {
            MigrationPath::File(path) => path,
            MigrationPath::Directory(path) => path,
        }
    }
}

impl PartialOrd for MigrationPath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        match (self, other) {
            (MigrationPath::File(path1), MigrationPath::File(path2)) => path1.partial_cmp(path2),
            (MigrationPath::Directory(path1), MigrationPath::Directory(path2)) => {
                path1.partial_cmp(path2)
            }
            _ => None,
        }
    }
}

impl Ord for MigrationPath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        match (self, other) {
            (MigrationPath::File(path1), MigrationPath::File(path2)) => path1.cmp(path2),
            (MigrationPath::Directory(path1), MigrationPath::Directory(path2)) => path1.cmp(path2),
            (MigrationPath::Directory(_), MigrationPath::File(_)) => std::cmp::Ordering::Less,
            (MigrationPath::File(_), MigrationPath::Directory(_)) => std::cmp::Ordering::Greater,
        }
    }
}

/// find migrations on file system recursively across directories given a location and [MigrationType]
pub fn find_migration_files(
    location: impl AsRef<Path>,
    migration_type: MigrationType,
) -> Result<Box<dyn Iterator<Item = MigrationPath>>, Error> {
    let location: &Path = location.as_ref();
    let location = location.canonicalize().map_err(|err| {
        Error::new(
            Kind::InvalidMigrationPath(location.to_path_buf(), err),
            None,
        )
    })?;

    if let MigrationType::Directory = migration_type {
        let re = dir_re();
        let dir_paths = WalkDir::new(location)
            .max_depth(1)
            .into_iter()
            .filter_map(Result::ok)
            .filter(|entry| entry.file_type().is_dir())
            .map(DirEntry::into_path)
            .filter(move |entry| {
                match entry.file_name().and_then(OsStr::to_str) {
                    Some(file_name) if re.is_match(file_name) => true,
                    Some(file_name) => {
                        log::warn!(
                            "Directory \"{}\" does not adhere to the migration naming convention. Migrations must be named in the format YYYYMMDDHHMMSS_{{name}}.",
                            file_name
                        );
                        false
                    }
                    None => false,
                }
            })
            .map(MigrationPath::Directory);

        return Ok(Box::new(dir_paths));
    }

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
        )
        .map(MigrationPath::File);

    Ok(Box::new(file_paths))
}

/// Loads SQL migrations from a path. This enables dynamic migration discovery, as opposed to
/// embedding. The resulting collection is ordered by version.
pub fn load_sql_migrations(location: impl AsRef<Path>) -> Result<Vec<Migration>, Error> {
    let migration_files = find_migration_files(location, MigrationType::Sql)?;
    let mut migrations = parse_sql_migration_files(migration_files)?;
    migrations.sort();
    Ok(migrations)
}

pub fn parse_sql_migration_files(
    migration_files: Box<dyn Iterator<Item = MigrationPath> + 'static>,
) -> Result<Vec<Migration>, Error> {
    let mut migrations = vec![];
    for path in migration_files {
        match path {
            MigrationPath::File(path) => {
                let sql = std::fs::read_to_string(path.as_path()).map_err(|e| {
                    let path = path.to_owned();
                    let kind = match e.kind() {
                        std::io::ErrorKind::NotFound => Kind::InvalidMigrationPath(path, e),
                        _ => Kind::InvalidMigrationFile(path, e),
                    };

                    Error::new(kind, None)
                })?;

                //safe to call unwrap as find_migration_filenames returns canonical paths
                let filename = path
                    .file_stem()
                    .and_then(|file| file.to_os_string().into_string().ok())
                    .unwrap();

                let migration = Migration::unapplied(&filename, &sql, None)?;
                migrations.push(migration);
            }
            MigrationPath::Directory(path) => {
                let (path_up, path_down) = find_directory_migration_files(&path)?;

                let sql_up = std::fs::read_to_string(path_up.as_path()).map_err(|e| {
                    let path = path_up.to_owned();
                    let kind = match e.kind() {
                        std::io::ErrorKind::NotFound => Kind::InvalidMigrationPath(path, e),
                        _ => Kind::InvalidMigrationFile(path, e),
                    };

                    Error::new(kind, None)
                })?;

                let sql_down = std::fs::read_to_string(path_down.as_path()).map_err(|e| {
                    let path = path_down.to_owned();
                    let kind = match e.kind() {
                        std::io::ErrorKind::NotFound => Kind::InvalidMigrationPath(path, e),
                        _ => Kind::InvalidMigrationFile(path, e),
                    };

                    Error::new(kind, None)
                })?;

                // safe to call unwrap as find_migration_filenames returns canonical paths
                let dirname = path
                    .file_name()
                    .and_then(|file| file.to_os_string().into_string().ok())
                    .unwrap();

                let migration = Migration::unapplied(&dirname, &sql_up, Some(&sql_down))?;
                migrations.push(migration);
            }
        }
    }
    Ok(migrations)
}

pub fn find_directory_migration_files(dir: &Path) -> Result<(PathBuf, PathBuf), Error> {
    let re = updown_re();
    let files = WalkDir::new(dir)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter(|entry| entry.file_type().is_file())
        .map(DirEntry::into_path)
        .filter(move |entry| {
            match entry.file_name().and_then(OsStr::to_str) {
                Some(file_name) if re.is_match(file_name) => true,
                Some(file_name) => {
                    log::warn!(
                        "Directory \"{}\" does not adhere to the migration naming convention. Migrations must be named in the format YYYYMMDDHHMMSS_{{name}}.",
                        file_name
                    );
                    false
                }
                None => false,
            }
        });

    let mut up_file = None;
    let mut down_file = None;

    for file in files {
        let file_stem = file
            .file_stem()
            .and_then(|stem| stem.to_str())
            .ok_or_else(|| {
                Error::new(
                    Kind::InvalidMigrationPath(
                        file.clone(),
                        std::io::Error::new(std::io::ErrorKind::InvalidData, "Invalid file name"),
                    ),
                    None,
                )
            })?;

        match file_stem {
            "up" => up_file = Some(file),
            "down" => down_file = Some(file),
            _ => unreachable!(),
        }
    }

    let up_file = up_file.ok_or_else(|| {
        Error::new(
            Kind::InvalidMigrationPath(
                dir.to_path_buf(),
                std::io::Error::new(std::io::ErrorKind::NotFound, "Up migration file not found"),
            ),
            None,
        )
    })?;

    let down_file = down_file.ok_or_else(|| {
        Error::new(
            Kind::InvalidMigrationPath(
                dir.to_path_buf(),
                std::io::Error::new(
                    std::io::ErrorKind::NotFound,
                    "Down migration file not found",
                ),
            ),
            None,
        )
    })?;

    Ok((up_file, down_file))
}

#[cfg(test)]
mod tests {
    use crate::util::MigrationPath;

    use super::{find_migration_files, load_sql_migrations, MigrationType};
    use std::fs;
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

        let mut mods: Vec<MigrationPath> = find_migration_files(migrations_dir, MigrationType::All)
            .unwrap()
            .collect();
        mods.sort();
        assert_eq!(sql1.canonicalize().unwrap(), mods[0].as_path());
        assert_eq!(sql2.canonicalize().unwrap(), mods[1].as_path());
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

        let mut mods: Vec<MigrationPath> = find_migration_files(migrations_dir, MigrationType::All)
            .unwrap()
            .collect();
        mods.sort();
        assert_eq!(sql1.canonicalize().unwrap(), mods[0].as_path());
        assert_eq!(sql2.canonicalize().unwrap(), mods[1].as_path());
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

        let mut mods: Vec<MigrationPath> = find_migration_files(migrations_dir, MigrationType::All)
            .unwrap()
            .collect();
        mods.sort();
        assert_eq!(sql1.canonicalize().unwrap(), mods[0].as_path());
        assert_eq!(sql2.canonicalize().unwrap(), mods[1].as_path());
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
