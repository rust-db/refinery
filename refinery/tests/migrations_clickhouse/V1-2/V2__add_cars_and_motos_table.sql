CREATE TABLE cars (
    id UUID,
    name String
)
Engine=MergeTree() ORDER BY id;
CREATE TABLE motos (
    id UUID,
    name String
)
Engine=MergeTree() ORDER BY id;
