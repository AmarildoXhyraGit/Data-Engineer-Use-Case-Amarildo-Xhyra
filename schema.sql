CREATE TABLE cars (
    id SERIAL PRIMARY KEY,
    make TEXT,
    model TEXT,
    production_year INT,
    price FLOAT,
    engine_type TEXT
);

CREATE TABLE consumers (
    id SERIAL PRIMARY KEY,
    country TEXT,
    model TEXT,
    year INT,
    review_score FLOAT,
    sales_volume INT
);
