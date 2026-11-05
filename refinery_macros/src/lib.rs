//! Contains Refinery macros that are used to import and embed migration files.
#![recursion_limit = "128"]

#[cfg(feature = "enums")]
use heck::ToUpperCamelCase;
use proc_macro::TokenStream;
use proc_macro2::{Span as Span2, TokenStream as TokenStream2};
use quote::quote;
use quote::ToTokens;
use refinery_core::{find_migration_files, MigrationType};
use std::path::PathBuf;
use std::{env, fs};
use syn::parse::{Parse, ParseStream};
use syn::{parse_macro_input, Ident, LitStr, Path, Token};

pub(crate) fn crate_root() -> PathBuf {
    let crate_root = env::var("CARGO_MANIFEST_DIR")
        .expect("CARGO_MANIFEST_DIR environment variable not present");
    PathBuf::from(crate_root)
}

/// The arguments of [`embed_migrations!`]: an optional location, optionally followed by a
/// line ending normalization.
struct EmbedMigrationsArgs {
    location: Option<LitStr>,
    /// The `LineEndingNormalization` SQL migrations are normalized with, if any, as written and
    /// as its variant.
    normalization: Option<(Path, Ident)>,
}

impl Parse for EmbedMigrationsArgs {
    fn parse(input: ParseStream) -> syn::Result<Self> {
        let mut args = EmbedMigrationsArgs {
            location: None,
            normalization: None,
        };
        if input.is_empty() {
            return Ok(args);
        }
        args.location = Some(input.parse()?);
        if input.is_empty() {
            return Ok(args);
        }
        input.parse::<Token![,]>()?;
        if input.is_empty() {
            return Ok(args);
        }
        let path: Path = input.parse()?;
        input.parse::<Option<Token![,]>>()?;
        match path.segments.last() {
            Some(segment)
                if segment.arguments.is_none()
                    && (segment.ident == "AsLF" || segment.ident == "AsCRLF") =>
            {
                let variant = segment.ident.clone();
                args.normalization = Some((path, variant));
                Ok(args)
            }
            _ => Err(syn::Error::new_spanned(
                path,
                "expected `refinery::LineEndingNormalization::AsLF` or `refinery::LineEndingNormalization::AsCRLF`",
            )),
        }
    }
}

fn migration_fn_quoted<T: ToTokens>(_migrations: Vec<T>) -> TokenStream2 {
    let result = quote! {
        use refinery::{Migration, Runner, SchemaVersion};
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

#[cfg(feature = "enums")]
fn migration_enum_quoted(migration_names: &[impl AsRef<str>]) -> TokenStream2 {
    use refinery_core::SchemaVersion;

    let mut variants = Vec::new();
    let mut discriminants = Vec::new();

    for m in migration_names {
        let m = m.as_ref();
        let (_, version, name) = refinery_core::parse_migration_name(m)
            .unwrap_or_else(|e| panic!("Couldn't parse migration filename '{}': {:?}", m, e));
        let version: SchemaVersion = version;
        let variant = Ident::new(name.to_upper_camel_case().as_str(), Span2::call_site());
        variants.push(quote! { #variant(Migration) = #version });
        discriminants.push(quote! { #version => Self::#variant(migration) });
    }
    discriminants.push(quote! { v => panic!("Invalid migration version '{}'", v) });

    #[cfg(feature = "int8-versions")]
    let embedded = quote! {
        #[repr(i64)]
        #[derive(Debug)]
        pub enum EmbeddedMigration {
            #(#variants),*
        }
    };

    #[cfg(not(feature = "int8-versions"))]
    let embedded = quote! {
        #[repr(i32)]
        #[derive(Debug)]
        pub enum EmbeddedMigration {
            #(#variants),*
        }
    };

    quote! {

        #embedded

        impl From<Migration> for EmbeddedMigration {
            fn from(migration: Migration) -> Self {
                match migration.version() as SchemaVersion {
                    #(#discriminants),*
                }
            }
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
/// Migrations are embedded sorted by path, so the generated code doesn't depend on the order in
/// which the file system happens to return the directory entries and builds stay reproducible.
/// This is not the order in which they are applied, [`Runner`] always applies them by version.
///
/// The location can be followed by a [`LineEndingNormalization`] to normalize the line endings
/// of SQL migrations before they are embedded. A migration's checksum covers its exact SQL, so
/// without it a migration checked out with CRLF line endings on one machine (for example by git's
/// `core.autocrlf` on Windows) and LF on another has two checksums, and the runner reports it as
/// divergent. Pick the line endings the migrations were first applied with, as switching changes
/// the checksums of migrations already applied. Rust migrations are not affected: the Rust
/// compiler already turns CRLF line endings in string literals into LF.
///
/// ```rust,ignore
/// mod embedded {
///     use refinery::embed_migrations;
///     embed_migrations!("./migrations", refinery::LineEndingNormalization::AsLF);
/// }
/// ```
///
/// [`Runner`]: https://docs.rs/refinery/latest/refinery/struct.Runner.html
/// [`LineEndingNormalization`]: https://docs.rs/refinery/latest/refinery/enum.LineEndingNormalization.html
#[proc_macro]
pub fn embed_migrations(input: TokenStream) -> TokenStream {
    let args = parse_macro_input!(input as EmbedMigrationsArgs);
    let location = match &args.location {
        Some(location) => crate_root().join(location.value()),
        None => crate_root().join("migrations"),
    };

    let migration_files =
        find_migration_files(location, MigrationType::All).expect("error getting migration files");

    let mut migrations_mods = Vec::new();
    let mut _migrations = Vec::new();
    let mut migration_filenames = Vec::new();

    for migration in migration_files {
        // safe to call unwrap as find_migration_filenames returns canonical paths
        let filename = migration
            .file_stem()
            .and_then(|file| file.to_os_string().into_string().ok())
            .unwrap();
        let path = migration.display().to_string();
        let extension = migration.extension().unwrap();
        migration_filenames.push(filename.clone());

        if extension == "sql" {
            let sql = match &args.normalization {
                Some((_, variant)) => quote! {
                    refinery::LineEndingNormalization::#variant.normalize(include_str!(#path))
                },
                None => quote! {include_str!(#path).to_string()},
            };
            _migrations.push(quote! {(#filename, #sql)});
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
    #[cfg(feature = "enums")]
    let enums = migration_enum_quoted(migration_filenames.as_slice());
    #[cfg(not(feature = "enums"))]
    let enums = quote!();

    // The generated module names the variant through `refinery`, as a path written relative to the
    // call site doesn't resolve inside it. The path is still checked where it was written, which
    // also uses an import of `LineEndingNormalization` it relies on.
    let normalization_check = match &args.normalization {
        Some((path, _)) => quote! {
            const _: refinery::LineEndingNormalization = #path;
        },
        None => quote!(),
    };

    (quote! {
        #normalization_check
        pub mod migrations {
            #(#migrations_mods)*
            #fnq
            #enums
        }
    })
    .into()
}

#[cfg(test)]
mod tests {
    use super::{migration_fn_quoted, quote, EmbedMigrationsArgs};

    fn parse_args(tokens: proc_macro2::TokenStream) -> syn::Result<EmbedMigrationsArgs> {
        syn::parse2(tokens)
    }

    #[test]
    fn parses_no_args() {
        let args = parse_args(quote!()).unwrap();
        assert!(args.location.is_none());
        assert!(args.normalization.is_none());
    }

    #[test]
    fn parses_location() {
        for tokens in [quote!("./migrations"), quote!("./migrations",)] {
            let args = parse_args(tokens).unwrap();
            assert_eq!("./migrations", args.location.unwrap().value());
            assert!(args.normalization.is_none());
        }
    }

    #[test]
    fn parses_location_and_normalization() {
        for (tokens, variant) in [
            (
                quote!("./migrations", refinery::LineEndingNormalization::AsLF),
                "AsLF",
            ),
            (
                quote!("./migrations", LineEndingNormalization::AsCRLF,),
                "AsCRLF",
            ),
            (quote!("./migrations", AsLF), "AsLF"),
        ] {
            let args = parse_args(tokens).unwrap();
            assert_eq!("./migrations", args.location.unwrap().value());
            assert_eq!(variant, args.normalization.unwrap().1.to_string());
        }
    }

    #[test]
    fn rejects_unknown_normalization() {
        let err = parse_args(quote!(
            "./migrations",
            refinery::LineEndingNormalization::AsCR
        ))
        .err()
        .unwrap();
        assert!(err
            .to_string()
            .contains("expected `refinery::LineEndingNormalization::AsLF`"));
    }

    #[test]
    fn rejects_extra_args() {
        assert!(parse_args(quote!("./migrations", AsLF, AsCRLF)).is_err());
        assert!(parse_args(quote!(AsLF)).is_err());
    }

    #[cfg(all(feature = "enums", feature = "int8-versions"))]
    #[test]
    fn test_enum_fn_i8() {
        let expected = concat! {
            "# [repr (i64)] ",
            "# [derive (Debug)] ",
            "pub enum EmbeddedMigration { ",
            "Foo (Migration) = 1i64 , ",
            "BarBaz (Migration) = 3i64 ",
            "} ",
            "impl From < Migration > for EmbeddedMigration { ",
            "fn from (migration : Migration) -> Self { ",
            "match migration . version () as SchemaVersion { ",
            "1i64 => Self :: Foo (migration) , ",
            "3i64 => Self :: BarBaz (migration) , ",
            "v => panic ! (\"Invalid migration version '{}'\" , v) ",
            "} } }"
        };
        let enums = super::migration_enum_quoted(&["V1__foo", "U3__barBAZ"]).to_string();
        assert_eq!(expected, enums);
    }

    #[cfg(all(feature = "enums", not(feature = "int8-versions")))]
    #[test]
    fn test_enum_fn() {
        let expected = concat! {
            "# [repr (i32)] ",
            "# [derive (Debug)] ",
            "pub enum EmbeddedMigration { ",
            "Foo (Migration) = 1i32 , ",
            "BarBaz (Migration) = 3i32 ",
            "} ",
            "impl From < Migration > for EmbeddedMigration { ",
            "fn from (migration : Migration) -> Self { ",
            "match migration . version () as SchemaVersion { ",
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
            "use refinery :: { Migration , Runner , SchemaVersion } ; ",
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
