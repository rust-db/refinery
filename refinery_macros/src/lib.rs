//! Contains Refinery macros that are used to import and embed migration files.
#![recursion_limit = "128"]

use heck::ToUpperCamelCase;
use proc_macro::TokenStream;
use proc_macro2::{Span as Span2, TokenStream as TokenStream2};
use quote::quote;
use quote::ToTokens;
use refinery_core::{
    find_migration_files, parse_finalize_migration, parse_no_transaction, MigrationType,
};
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
        pub fn runner() -> Runner {
            use refinery::{Migration, Runner};
            let quoted_migrations: Vec<(&str, Option<bool>, String)> = vec![#(#_migrations),*];
            let mut migrations: Vec<Migration> = Vec::new();
            for module in quoted_migrations.into_iter() {
                migrations.push(Migration::unapplied(module.0, module.1, &module.2).unwrap());
            }
            Runner::new(&migrations)
        }
    };
    result
}

fn finalize_migration_fns_quoted<T: ToTokens>(
    _finalized_migrations: Vec<T>,
    _async_finalized_migrations: Vec<T>,
) -> TokenStream2 {
    let result = quote! {
        pub fn runner_with_finalize<C: Executor>(conn: &mut C) -> Runner {
            let migrations: Vec<Migration> = vec![#(#_finalized_migrations),*];
            Runner::new(&migrations)
        }

        pub async fn runner_with_async_finalize<C: AsyncExecutor>(conn: &mut C) -> Runner {
            let migrations: Vec<Migration> = vec![#(#_async_finalized_migrations),*];
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
            let (_, version, name) = refinery_core::parse_migration_name(m)
                .unwrap_or_else(|e| panic!("Couldn't parse migration filename '{}': {:?}", m, e));
            let variant = Ident::new(name.to_upper_camel_case().as_str(), Span2::call_site());
            variants.push(quote! { #variant(Migration) = #version });
            discriminants.push(quote! { #version => Self::#variant(migration) });
        }
        discriminants.push(quote! { v => panic!("Invalid migration version '{}'", v) });

        let result = quote! {
            #[repr(i32)]
            #[derive(Debug)]
            pub enum EmbeddedMigration {
                #(#variants),*
            }

            impl From<Migration> for EmbeddedMigration {
                fn from(migration: Migration) -> Self {
                    match migration.version() as i32 {
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

/// Return a tuple of tokens that create a `Migration` by using the correct method
/// depending on whether or not the migration needs to be finalized.
/// Returns a tuple of tokens of function calls where the first element synchronously
/// created the `Migration`, and the second element asynchronously did.
fn unapplied_migration_call(
    filename: &str,
    path: String,
    no_transaction: Option<bool>,
    is_rs_finalize: Option<bool>,
) -> (TokenStream2, TokenStream2) {
    let no_transaction_token = match no_transaction {
        Some(val) => quote!(core::option::Option::Some(#val)),
        None => quote!(core::option::Option::None),
    };
    match is_rs_finalize {
        // this is a sql migration so doesn't need to call finalizing unapplied
        None => {
            let sql_mig = quote! {Migration::unapplied(#filename, #no_transaction_token, include_str!(#path)).unwrap()};
            let async_sql_mig = quote! {Migration::unapplied(#filename, #no_transaction_token, include_str!(#path)).unwrap()};
            (sql_mig, async_sql_mig)
        }
        Some(finalize) if finalize => {
            let ident = Ident::new(&filename, Span2::call_site());
            let rs_fin_mig = quote! {Migration::finalize_unapplied::<#ident::Finalizer, _>(conn, #filename, #no_transaction_token).unwrap()};
            let async_rs_fin_mig = quote! {Migration::async_finalize_unapplied::<#ident::Finalizer, _>(conn, #filename, #no_transaction_token).await.unwrap()};
            (rs_fin_mig, async_rs_fin_mig)
        }
        _ => {
            let ident = Ident::new(&filename, Span2::call_site());
            let rs_mig = quote! {Migration::unapplied(#filename, #no_transaction_token, &#ident::migration()).unwrap()};
            let async_rs_mig = quote! {Migration::unapplied(#filename, #no_transaction_token, &#ident::migration()).unwrap()};
            (rs_mig, async_rs_mig)
        }
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

    let migration_files =
        find_migration_files(location, MigrationType::All).expect("error getting migration files");

    let mut migrations_mods = Vec::new();
    let mut _migrations = Vec::new();
    let mut _finalized_migrations = Vec::new();
    let mut _async_finalized_migrations = Vec::new();
    let mut migration_filenames = Vec::new();

    for migration in migration_files {
        // safe to call unwrap as find_migration_filenames returns canonical paths
        let filename = migration
            .file_stem()
            .and_then(|file| file.to_os_string().into_string().ok())
            .unwrap();
        let path = migration.display().to_string();
        let content = fs::read_to_string(&path).unwrap();
        let no_transaction = parse_no_transaction(content, MigrationType::All);
        let no_transaction_token = match no_transaction {
            Some(val) => quote!(core::option::Option::Some(#val)),
            None => quote!(core::option::Option::None),
        };
        let extension = migration.extension().unwrap();
        migration_filenames.push(filename.clone());

        if extension == "sql" {
            _migrations
                .push(quote! {(#filename, #no_transaction_token, include_str!(#path).to_string())});
            let (sql_mig, async_sql_mig) =
                unapplied_migration_call(&filename, path, no_transaction, None);
            _finalized_migrations.push(sql_mig);
            _async_finalized_migrations.push(async_sql_mig);
        } else if extension == "rs" {
            let rs_raw = fs::read_to_string(&path).unwrap();
            let rs_content = rs_raw.parse::<TokenStream2>().unwrap();
            let ident = Ident::new(&filename, Span2::call_site());
            let is_rs_finalize = parse_finalize_migration(rs_raw.to_string()).or(Some(false));
            let mig_mod = quote! {pub mod #ident {
                #rs_content
                // also include the file as str so we trigger recompilation if it changes
                const _RECOMPILE_IF_CHANGED: &str = include_str!(#path);
            }};
            _migrations.push(quote! {(#filename, #no_transaction_token, #ident::migration())});
            let (rs_mig, async_rs_mig) =
                unapplied_migration_call(&filename, path, no_transaction, is_rs_finalize);
            _finalized_migrations.push(rs_mig);
            _async_finalized_migrations.push(async_rs_mig);
            migrations_mods.push(mig_mod);
        }
    }

    let fnq = migration_fn_quoted(_migrations);
    let fnzq = finalize_migration_fns_quoted(_finalized_migrations, _async_finalized_migrations);
    let enums = migration_enum_quoted(migration_filenames.as_slice());
    (quote! {
        pub mod migrations {
            #(#migrations_mods)*
            use refinery::{Migration, Runner, Executor, AsyncExecutor};
            #fnq
            #fnzq
            #enums
        }
    })
    .into()
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
