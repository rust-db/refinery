//! Contains Refinery macros that are used to import and embed migration files.
#![recursion_limit = "128"]

use proc_macro::TokenStream;
use proc_macro2::{Span as Span2, TokenStream as TokenStream2};
use quote::quote;
use quote::ToTokens;
use refinery_core::{find_migration_files, MigrationType};
use std::path::PathBuf;
use std::{env, fs};
use syn::{parse_macro_input, Ident, LitStr};

pub(crate) fn crate_root() -> PathBuf {
    let crate_root = env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR environment variable not present");
    PathBuf::from(crate_root)
}

fn migration_fn_quoted<T: ToTokens>(_migrations: Vec<T>) -> TokenStream2 {
    let result = quote! {
        use refinery::{Migration, Runner};
        pub fn runner() -> Runner {
            let quoted_migrations: Vec<(&str, String)> = vec![#(#_migrations),*];
            let mut migrations: Vec<Migration> = Vec::new();
            for module in quoted_migrations.into_iter() {
                migrations.push(Migration::unapplied(module.0, &module.1).unwrap());
            }
            Runner::new(&migrations)
        }
    };
    result
}

/// Interpret Rust or SQL migrations and inserts a function called runner that when called returns a [`Runner`] instance with the collected migration modules.
///
/// When called without arguments `embed_migrations` searches for migration files on a directory called `migrations` at the root level of your crate.
/// if you want to specify another directory call `embed_migrations!` with it's location relative to the root level of your crate.
///
/// To be a valid migration module, it has to be named in the format `V{1}__{2}.{3} ` where `{1}` represents the migration version and `{2}` the name and `{3} is "rs" or "sql".
/// For the name alphanumeric characters plus "_" are supported.
/// The Rust migration file must have a function named `migration()` that returns a [`std::string::String`].
/// The SQL migration file must have valid sql instructions for the database you want it to run on.
///
/// [`Runner`]: https://docs.rs/refinery/latest/refinery/struct.Runner.html
#[proc_macro]
pub fn embed_migrations(input: TokenStream) -> TokenStream {
    let location = if input.is_empty() {
        crate_root().join("migrations")
    } else {
        let location: LitStr = parse_macro_input!(input);
        crate_root().join(location.value())
    };

    let migration_files =
        find_migration_files(location, MigrationType::All).expect("error getting migration files");

    let mut migrations_mods = Vec::new();
    let mut _migrations = Vec::new();

    for migration in migration_files {
        // safe to call unwrap as find_migration_filenames returns canonical paths
        let filename = migration
            .file_stem()
            .and_then(|file| file.to_os_string().into_string().ok())
            .unwrap();
        let path = migration.display().to_string();
        let extension = migration.extension().unwrap();

        if extension == "sql" {
            _migrations.push(quote! {(#filename, include_str!(#path).to_string())});
        } else if extension == "rs" {
            let rs_content = fs::read_to_string(&path)
                .unwrap()
                .parse::<TokenStream2>()
                .unwrap();
            let ident = Ident::new(&filename, Span2::call_site());
            let mig_mod = quote! {pub mod #ident {
                #rs_content
                // also include the file as str so we trigger recompilation if it changes
                const _RECOMPILE_IF_CHANGED: &str = include_str!(#path);
            }};
            _migrations.push(quote! {(#filename, #ident::migration())});
            migrations_mods.push(mig_mod);
        }
    }

    let fnq = migration_fn_quoted(_migrations);
    (quote! {
        pub mod migrations {
            #(#migrations_mods)*
            #fnq
        }
    })
    .into()
}

#[cfg(test)]
mod tests {
    use super::{migration_fn_quoted, quote};

    #[test]
    fn test_quote_fn() {
        let migs = vec![quote!("V1__first", "valid_sql_file")];
        let expected = concat! {
            "use refinery :: { Migration , Runner } ; ",
            "pub fn runner () -> Runner { ",
            "let quoted_migrations : Vec < (& str , String) > = vec ! [\"V1__first\" , \"valid_sql_file\"] ; ",
            "let mut migrations : Vec < Migration > = Vec :: new () ; ",
            "for module in quoted_migrations . into_iter () { ",
            "migrations . push (Migration :: unapplied (module . 0 , & module . 1) . unwrap ()) ; ",
            "} ",
            "Runner :: new (& migrations) }"
        };
        assert_eq!(expected, migration_fn_quoted(migs).to_string());
    }
}
