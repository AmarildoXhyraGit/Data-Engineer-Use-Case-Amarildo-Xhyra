CREATE TABLE cars (
    id SERIAL PRIMARY KEY,
    make TEXT,
    model TEXT,
    production_year INT,
    price FLOAT,
    engine_type TEXT,
    CONSTRAINT unique_car UNIQUE (make, model, production_year)
);

CREATE TABLE consumers (
    id SERIAL PRIMARY KEY,
    country TEXT,
    model TEXT,
    type TEXT, 
    year INT,
    sales_volume INT,
    review_score FLOAT,
    CONSTRAINT unique_consumer UNIQUE (country, model, type, year) 
);