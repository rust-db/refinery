mod cli {
    use assert_cmd::prelude::*;
    use predicates::str::contains;
    use std::process::Command;

    // `refinery` with no args should exit with a non-zero code.
    #[test]
    fn cli_no_args() {
        Command::new(assert_cmd::cargo_bin!("refinery"))
            .assert()
            .failure();
    }

    #[test]
    fn cli_version() {
        Command::new(assert_cmd::cargo_bin!("refinery"))
            .arg("-V")
            .assert()
            .stdout(contains(env!("CARGO_PKG_VERSION")));
    }

    // `refinery migrate` with no args should exit with a non-zero code.
    #[test]
    fn migrate_no_args() {
        Command::new(assert_cmd::cargo_bin!("refinery"))
            .arg("migrate")
            .assert()
            .failure();
    }
}
