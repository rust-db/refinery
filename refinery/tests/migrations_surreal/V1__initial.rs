pub fn migration() -> String {
    r##"
        DEFINE TABLE user SCHEMAFULL;

        DEFINE FIELD first_name ON TABLE user TYPE string;
        DEFINE FIELD last_name ON TABLE user TYPE string;
    "##.to_string()
}
