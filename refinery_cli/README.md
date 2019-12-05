# Refinery Cli

Run Refinery migrations via cli.

## Instalation
Install refinery_cli via cargo.

```sh
cargo install refinery_cli
```

## Usage
Setup your database type and access credentials with `setup`.

```sh
refinery_cli setup
```

After that, just run your migrations giving your config file with `-c` flag and migrations dir with `files -p $dir`.

```sh
refinery_cli migrate -c sqlite_refinery.toml files -p ./sql_migrations
```

For more info and migration options run.

```sh
refinery_cli migrate --help
```
