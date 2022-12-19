
CREATE TABLE persons (
    id UUID,
    name String,
    city String
) 
Engine=MergeTree() ORDER BY id;