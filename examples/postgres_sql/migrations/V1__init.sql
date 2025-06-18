CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    date DATE NOT NULL,
    kind VARCHAR(6) NOT NULL,
    amount NUMERIC NOT NULL,
    description TEXT NOT NULL,
    tag VARCHAR(50) NOT NULL
);
