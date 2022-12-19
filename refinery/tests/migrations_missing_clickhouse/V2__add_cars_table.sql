CREATE TABLE cars (
    id Int32,
    name String
)
Engine=MergeTree() ORDER BY id;