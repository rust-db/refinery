use crate::error::{Error, Kind};
use crate::Migration;
use regex::Regex;
use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use walkdir::{DirEntry, WalkDir};

const STEM_RE: &str = r"^(\d{8}_\d{6})_([a-z0-9\_]+)";
const DIR_RE: &str = r"^(\d{8}_\d{6})_([a-z0-9\_]+)$";
const UPDOWN_RE: &str = r"^(up|down)";

/// Matches the stem of a migration file.
fn file_stem_re() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(STEM_RE).unwrap())
}

/// Matches the stem + extension of any migration file.
fn file_re_rs() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new([STEM_RE, r"\.rs$"].concat().as_str()).unwrap())
}

/// Matches the stem + extension of a any directory migration file.
fn updown_re_all() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new([UPDOWN_RE, r"\.(rs|sql)$"].concat().as_str()).unwrap())
}

/// Matches the stem + extension of sql migration file.
fn updown_re_sql() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new([UPDOWN_RE, r"\.sql$"].concat().as_str()).unwrap())
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
}

impl MigrationType {
    fn updown_match_re(&self) -> &'static Regex {
        match self {
            MigrationType::All => updown_re_all(),
            MigrationType::Sql => updown_re_sql(),
        }
    }
}

/// Parse a migration file stem or directory name into a version, and name.
pub fn parse_migration_name(name: &str) -> Result<(i64, String), Error> {
    let captures = file_stem_re()
        .captures(name)
        .filter(|caps| caps.len() == 3)
        .ok_or_else(|| Error::new(Kind::InvalidName, None))?;

    let version: i64 = captures[1]
        .replace("_", "")
        .parse()
        .map_err(|_| Error::new(Kind::InvalidVersion, None))?;

    let name: String = (&captures[2]).into();

    Ok((version, name))
}

#[derive(Debug, PartialEq, Eq)]
pub enum MigrationPath {
    File(PathBuf),
    Directory {
        dir: PathBuf,
        up: PathBuf,
        down: PathBuf,
    },
}

impl MigrationPath {
    pub fn as_path(&self) -> &Path {
        match self {
            MigrationPath::File(path) => path,
            MigrationPath::Directory { dir, .. } => dir,
        }
    }
}

impl PartialOrd for MigrationPath {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MigrationPath {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.as_path().cmp(other.as_path())
    }
}

/// find migrations on file system recursively across directories given a location and [MigrationType]
pub fn find_migration_files(
    location: impl AsRef<Path>,
    migration_type: MigrationType,
) -> Result<impl Iterator<Item = MigrationPath>, Error> {
    let location: &Path = location.as_ref();
    let location = location.canonicalize().map_err(|err| {
        Error::new(
            Kind::InvalidMigrationPath(location.to_path_buf(), err),
            None,
        )
    })?;

    let file_re = file_re_rs();
    let dir_re = dir_re();

    let migration_files = WalkDir::new(location)
        .min_depth(1)
        .max_depth(1)
        .into_iter()
        .filter_map(Result::ok)
        .filter_map(move |entry| {
            let file_name = entry.file_name();
            eprintln!("{}", file_name.display());

            if entry.file_type().is_dir() {
                match file_name.to_str() {
                    Some(file_name) if dir_re.is_match(file_name) => {
                        match find_directory_migration_files(&entry.path(), &migration_type) {
                            Ok((up, down)) => {
                                Some(MigrationPath::Directory { dir: entry.into_path(), up, down })
                            },
                            Err(e) => {
                                log::warn!(
                                    "Directory \"{}\" is missing either up or down migration files: {e}",
                                    file_name
                                );
                                None
                            }
                        }
                    }
                    Some(file_name) => {
                        log::warn!(
                            "Directory \"{}\" does not adhere to the migration naming convention. Migrations must be named in the format YYYYMMDDHHMMSS_{{name}}.",
                            file_name
                        );
                        None
                    }
                    None => None,
                }
            } else if entry.file_type().is_file() {
                // We do not support standalone SQL files in the same directory as the migration files
                if let MigrationType::Sql = migration_type {
                    return None;
                }

                match file_name.to_str() {
                    Some(file_name) if file_re.is_match(file_name) => Some(MigrationPath::File(entry.into_path())),
                    Some(file_name) => {
                        log::warn!(
                            "File \"{}\" does not adhere to the migration naming convention. Migrations must be named in the format YYYYMMDDHHMMSS_{{name}}.",
                            file_name
                        );
                        None
                    }
                    None => None,
                }
            } else {
                None
            }
        });

    Ok(migration_files)
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
    migration_files: impl Iterator<Item = MigrationPath>,
) -> Result<Vec<Migration>, Error> {
    let mut migrations = vec![];
    for path in migration_files {
        match path {
            MigrationPath::File(path) => {
                return Err(Error::new(
                    Kind::InvalidMigrationPath(
                        path,
                        std::io::Error::new(
                            std::io::ErrorKind::InvalidData,
                            "SQL migration files must be in a directory",
                        ),
                    ),
                    None,
                ));
            }
            MigrationPath::Directory { dir, up, down } => {
                let sql_up = std::fs::read_to_string(up.as_path()).map_err(|e| {
                    let path = up.to_owned();
                    let kind = match e.kind() {
                        std::io::ErrorKind::NotFound => Kind::InvalidMigrationPath(path, e),
                        _ => Kind::InvalidMigrationFile(path, e),
                    };

                    Error::new(kind, None)
                })?;

                let sql_down = std::fs::read_to_string(down.as_path()).map_err(|e| {
                    let path = down.to_owned();
                    let kind = match e.kind() {
                        std::io::ErrorKind::NotFound => Kind::InvalidMigrationPath(path, e),
                        _ => Kind::InvalidMigrationFile(path, e),
                    };

                    Error::new(kind, None)
                })?;

                // safe to call unwrap as find_migration_filenames returns canonical paths
                let dirname = dir
                    .file_name()
                    .and_then(|file| file.to_os_string().into_string().ok())
                    .unwrap();

                let migration = Migration::unapplied(&dirname, &sql_up, &sql_down)?;
                migrations.push(migration);
            }
        }
    }
    Ok(migrations)
}

pub fn find_directory_migration_files(
    dir: &Path,
    migration_type: &MigrationType,
) -> Result<(PathBuf, PathBuf), Error> {
    let re = migration_type.updown_match_re();

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
            _ => continue,
        }

        if up_file.is_some() && down_file.is_some() {
            break;
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
        let sql1 = migrations_dir.join("20250501_000000_first.rs");
        fs::File::create(&sql1).unwrap();
        let sql2 = migrations_dir.join("20250502_000000_second.rs");
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
        let sql1 = migrations_dir.join("V1_first.rs");
        fs::File::create(sql1).unwrap();
        let sql2 = migrations_dir.join("V2_second.rs");
        fs::File::create(sql2).unwrap();

        let mut mods = find_migration_files(migrations_dir, MigrationType::All).unwrap();
        assert!(mods.next().is_none());
    }

    #[test]
    fn finds_sql_migrations() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();

        let sql1_dir = migrations_dir.join("20250501_000000_first");
        fs::create_dir(&sql1_dir).unwrap();
        let sql1_up = sql1_dir.join("up.sql");
        fs::File::create(sql1_up).unwrap();
        let sql2_down = sql1_dir.join("down.sql");
        fs::File::create(sql2_down).unwrap();

        let sql2_dir = migrations_dir.join("20250502_000000_second");
        fs::create_dir(&sql2_dir).unwrap();
        let sql2_up = sql2_dir.join("up.sql");
        fs::File::create(sql2_up).unwrap();
        let sql2_down = sql2_dir.join("down.sql");
        fs::File::create(sql2_down).unwrap();

        let mut mods: Vec<MigrationPath> = find_migration_files(migrations_dir, MigrationType::All)
            .unwrap()
            .collect();
        mods.sort();
        assert_eq!(sql1_dir.canonicalize().unwrap(), mods[0].as_path());
        assert_eq!(sql2_dir.canonicalize().unwrap(), mods[1].as_path());
    }

    #[test]
    fn ignores_sql_files_without_migration_regex_match() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();

        let sql1_dir = migrations_dir.join("20250501_000000_first");
        fs::create_dir(&sql1_dir).unwrap();
        let sql1_up = sql1_dir.join("v1.sql");
        fs::File::create(sql1_up).unwrap();
        let sql2_down = sql1_dir.join("rollback.sql");
        fs::File::create(sql2_down).unwrap();

        let sql2_dir = migrations_dir.join("20250502_000000_second");
        fs::create_dir(&sql2_dir).unwrap();
        let sql2_up = sql2_dir.join("upgrade.sql");
        fs::File::create(sql2_up).unwrap();
        let sql2_down = sql2_dir.join("downgrade.sql");
        fs::File::create(sql2_down).unwrap();

        let mut mods = find_migration_files(migrations_dir, MigrationType::All).unwrap();
        assert!(mods.next().is_none());
    }

    #[test]
    fn loads_migrations_from_path() {
        let tmp_dir = TempDir::new().unwrap();
        let migrations_dir = tmp_dir.path().join("migrations");
        fs::create_dir(&migrations_dir).unwrap();

        let sql1_dir = migrations_dir.join("20250501_000000_first");
        fs::create_dir(&sql1_dir).unwrap();
        let sql1_up = sql1_dir.join("up.sql");
        fs::File::create(sql1_up).unwrap();
        let sql2_down = sql1_dir.join("down.sql");
        fs::File::create(sql2_down).unwrap();

        let sql2_dir = migrations_dir.join("20250502_000000_second");
        fs::create_dir(&sql2_dir).unwrap();
        let sql2_up = sql2_dir.join("up.sql");
        fs::File::create(sql2_up).unwrap();
        let sql2_down = sql2_dir.join("down.sql");
        fs::File::create(sql2_down).unwrap();

        let rs3 = migrations_dir.join("V3__third.rs");
        fs::File::create(&rs3).unwrap();

        let migrations = load_sql_migrations(migrations_dir).unwrap();
        assert_eq!(migrations.len(), 2);
        assert_eq!(&migrations[0].to_string(), "20250501000000_first");
        assert_eq!(&migrations[1].to_string(), "20250502000000_second");
    }
}
