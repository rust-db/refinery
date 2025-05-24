use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use anyhow::{Context, Error};
use chrono::Utc;

use crate::cli::GenerateArgs;

pub fn handle_generate_command(args: GenerateArgs) -> anyhow::Result<()> {
    let migration_name = sanitize_name(&args.name);
    let timestamp = Utc::now().format("%Y%m%d_%H%M%S");
    let migration_dir_name = format!("{}_{}", timestamp, migration_name);
    
    create_migration(&args.path, &migration_dir_name)
}

fn sanitize_name(name: &str) -> String {
    let lowercase = name.chars()
        .map(|c| if c.is_alphanumeric() || c == '_' { c } else { '_' })
        .collect::<String>()
        .to_lowercase();
    
    let mut result = String::with_capacity(lowercase.len());
    let mut last_was_underscore = false;
    
    for c in lowercase.chars() {
        if c == '_' {
            if !last_was_underscore {
                result.push(c);
            }
            last_was_underscore = true;
        } else {
            result.push(c);
            last_was_underscore = false;
        }
    }
    
    result.trim_start_matches('_').trim_end_matches('_').to_string()
}

fn create_migration(base_path: &Path, migration_name: &str) -> anyhow::Result<()> {
    if !base_path.exists() {
        fs::create_dir_all(base_path)
            .context(format!("Failed to create migrations directory at {:?}", base_path))?;
    }

    let migration_path = base_path.join(migration_name);
    if migration_path.exists() {
        return Err(Error::msg(format!(
            "Migration directory already exists at {:?}",
            migration_path
        )));
    }

    fs::create_dir(&migration_path).context(format!(
        "Failed to create migration directory at {:?}",
        migration_path
    ))?;

    create_migration_file(
        &migration_path.join("up.sql"),
        "-- Write your UP migration SQL here\n",
    )?;

    create_migration_file(
        &migration_path.join("down.sql"),
        "-- Write your DOWN migration SQL here\n",
    )?;

    println!("Created migration: {}", migration_name);
    println!("Migration directory: {:?}", migration_path);
    println!("Don't forget to write your migration SQL in the up.sql and down.sql files!");

    Ok(())
}

fn create_migration_file(path: &PathBuf, content: &str) -> anyhow::Result<()> {
    let mut file = fs::File::create(path)
        .context(format!("Failed to create migration file at {:?}", path))?;
    
    file.write_all(content.as_bytes())
        .context(format!("Failed to write to migration file at {:?}", path))?;
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sanitize_name() {
        assert_eq!(sanitize_name("create_users_table"), "create_users_table");
        assert_eq!(sanitize_name("CreateUsersTable"), "createuserstable");
        assert_eq!(sanitize_name("create-users-table"), "create_users_table");
        assert_eq!(sanitize_name("create users table"), "create_users_table");
        assert_eq!(sanitize_name("create.users.table"), "create_users_table");
        assert_eq!(sanitize_name("_create_users_table"), "create_users_table");
        assert_eq!(sanitize_name("create_users_table_"), "create_users_table");
        assert_eq!(sanitize_name("_create_users_table_"), "create_users_table");
        assert_eq!(sanitize_name("create__users___table"), "create_users_table");
        assert_eq!(sanitize_name("__Create-Users__Table!@#$%^&*()__"), "create_users_table");
    }
}