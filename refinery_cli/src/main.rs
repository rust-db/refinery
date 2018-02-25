//!

extern crate chrono;
extern crate clap;

mod cli;

fn main() {
    let _matches = cli::create_cli().get_matches();
    // println!("Hello, world!");
}
