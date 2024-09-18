DROP TABLE IF EXISTS property_transaction;
CREATE TABLE property_transaction (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    area REAL,
    floor_range TEXT,
    no_of_units INTEGER,
    contract_date TEXT,
    type_of_sale INTEGER,
    price REAL,
    property_type TEXT,
    district INTEGER,
    type_of_area TEXT,
    tenure TEXT,
    project TEXT,
    street TEXT,
    x TEXT,
    y TEXT,
    market_segment TEXT,
    contract_datetime TEXT,
    price_psm REAL,
    contract_month INTEGER,
    contract_year INTEGER,
    contract_quarter INTEGER
);