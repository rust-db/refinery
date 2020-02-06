use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use walkdir::{DirEntry, WalkDir};

use regex::Regex;

pub(crate) fn find_migration_files(
    location: impl AsRef<Path>,
) -> Result<impl Iterator<Item = PathBuf>, std::io::Error> {
    let re = Regex::new(r"^V\d+(\.\d+)?__\w+\.sql$").unwrap();

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
