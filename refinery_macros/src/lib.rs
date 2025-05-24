//! Contains Refinery macros that are used to import and embed migration files.
#![recursion_limit = "128"]

use heck::ToUpperCamelCase;
use itertools::Itertools;
use proc_macro::TokenStream;
use proc_macro2::{Span as Span2, TokenStream as TokenStream2};
use quote::ToTokens;
use quote::quote;
use refinery_core::MigrationPath;
use refinery_core::{MigrationType, find_migration_files};
use std::path::Path;
use std::path::PathBuf;
use std::{env, fs};
use syn::{Ident, LitStr, parse_macro_input};

pub(crate) fn crate_root() -> PathBuf {
    let crate_root = env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR environment variable not present");
    PathBuf::from(crate_root)
}

fn migration_fn_quoted<T: ToTokens>(migration_tuples: Vec<T>) -> TokenStream2 {
    let result = quote! {
        use refinery::{Migration, Runner};
        pub fn runner() -> Runner {
            let quoted_migrations: Vec<(&str, String, String)> = vec![#(#migration_tuples),*];
            let mut migrations: Vec<Migration> = Vec::new();
            for module in quoted_migrations.into_iter() {
                migrations.push(Migration::unapplied(module.0, &module.1, &module.2).unwrap());
            }
            Runner::new(&migrations)
        }
    };
    result
}

fn migration_enum_quoted(migration_names: &[impl AsRef<str>]) -> TokenStream2 {
    if cfg!(feature = "enums") {
        let mut variants = Vec::new();
        let mut discriminants = Vec::new();

        for m in migration_names {
            let m = m.as_ref();
            let (version, name) = refinery_core::parse_migration_name(m)
                .unwrap_or_else(|e| panic!("Couldn't parse migration filename '{}': {:?}", m, e));
            let variant = Ident::new(name.to_upper_camel_case().as_str(), Span2::call_site());
            variants.push(quote! { #variant(Migration) = #version });
            discriminants.push(quote! { #version => Self::#variant(migration) });
        }

        discriminants.push(quote! { v => panic!("Invalid migration version '{}'", v) });

        let repr_quoted = if !variants.is_empty() {
            quote! { #[repr(i64)] }
        } else {
            quote! {}
        };

        let result = quote! {
            #repr_quoted
            #[derive(Debug)]
            pub enum EmbeddedMigration {
                #(#variants),*
            }

            impl From<Migration> for EmbeddedMigration {
                fn from(migration: Migration) -> Self {
                    match migration.version() as i64 {
                        #(#discriminants),*
                    }
                }
            }
        };
        result
    } else {
        quote!()
    }
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

    let migration_files = find_migration_files(location, MigrationType::All)
        .expect("error getting migration files")
        .sorted();

    let mut migration_mods = Vec::new();
    let mut migration_tuples = Vec::new();
    let mut migration_filenames = Vec::new();

    for migration in migration_files {
        match migration {
            MigrationPath::File(migration) => {
                // safe to call unwrap as find_migration_filenames returns canonical paths
                let filename = migration
                    .file_stem()
                    .and_then(|file| file.to_os_string().into_string().ok())
                    .unwrap();

                let path = migration.display().to_string();
                let extension = migration.extension().unwrap();
                migration_filenames.push(filename.clone());

                if extension == "sql" {
                    panic!("received unexpected sql file at root directory");
                } else if extension == "rs" {
                    let rs_content = fs::read_to_string(&path)
                        .unwrap()
                        .parse::<TokenStream2>()
                        .unwrap();

                    let ident = Ident::new(&format!("m{filename}"), Span2::call_site());

                    let mig_mod = quote! {pub mod #ident {
                        #rs_content
                        // also include the file as str so we trigger recompilation if it changes
                        const _RECOMPILE_IF_CHANGED: &str = include_str!(#path);
                    }};

                    migration_tuples.push(quote! {(#filename, #ident::up(), #ident::down())});

                    migration_mods.push(mig_mod);
                }
            }
            MigrationPath::Directory { dir, up, down } => {
                // safe to call unwrap as find_migration_filenames returns canonical paths
                let dirname = dir
                    .file_stem()
                    .and_then(|file| file.to_os_string().into_string().ok())
                    .unwrap();

                migration_filenames.push(dirname.clone());

                let (up_content, up_mod) = directory_file_quoted(&dirname, &up, "up");
                if let Some(up_mod) = up_mod {
                    migration_mods.push(up_mod);
                }

                let (down_content, down_mod) = directory_file_quoted(&dirname, &down, "down");
                if let Some(down_mod) = down_mod {
                    migration_mods.push(down_mod);
                }

                migration_tuples.push(quote! {(#dirname, #up_content, #down_content)});
            }
        }
    }

    let fnq = migration_fn_quoted(migration_tuples);
    let enums = migration_enum_quoted(migration_filenames.as_slice());

    (quote! {
        pub mod migrations {
            #(#migration_mods)*
            #fnq
            #enums
        }
    })
    .into()
}

fn directory_file_quoted(
    dirname: &str,
    file_path: &Path,
    direction: &str,
) -> (TokenStream2, Option<TokenStream2>) {
    let path = file_path.display().to_string();
    let extension = file_path.extension().unwrap();

    if extension == "sql" {
        (quote! {include_str!(#path).to_string()}, None)
    } else if extension == "rs" {
        let rs_content = fs::read_to_string(&path)
            .unwrap()
            .parse::<TokenStream2>()
            .unwrap();

        let ident = Ident::new(&format!("m{dirname}_{direction}"), Span2::call_site());

        let mig_mod = quote! {pub mod #ident {
            #rs_content
            // also include the file as str so we trigger recompilation if it changes
            const _RECOMPILE_IF_CHANGED: &str = include_str!(#path);
        }};

        let direction_ident = Ident::new(direction, Span2::call_site());

        (quote! {#ident::#direction_ident()}, Some(mig_mod))
    } else {
        panic!("unexpected file type: {}", extension.to_string_lossy());
    }
}

#[cfg(test)]
mod tests {
    use super::{migration_fn_quoted, quote};

    #[test]
    #[cfg(feature = "enums")]
    fn test_enum_fn() {
        let expected = concat! {
            "# [repr (i32)] # [derive (Debug)] ",
            "pub enum EmbeddedMigration { ",
            "Foo (Migration) = 1i32 , ",
            "BarBaz (Migration) = 3i32 ",
            "} ",
            "impl From < Migration > for EmbeddedMigration { ",
            "fn from (migration : Migration) -> Self { ",
            "match migration . version () as i32 { ",
            "1i32 => Self :: Foo (migration) , ",
            "3i32 => Self :: BarBaz (migration) , ",
            "v => panic ! (\"Invalid migration version '{}'\" , v) ",
            "} } }"
        };
        let enums = super::migration_enum_quoted(&["V1__foo", "U3__barBAZ"]).to_string();
        assert_eq!(expected, enums);
    }

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
