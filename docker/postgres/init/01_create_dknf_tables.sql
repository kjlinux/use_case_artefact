CREATE TYPE gender_enum AS ENUM ('Female', 'Male', 'Other');

CREATE TABLE countries (
    country_id   SERIAL PRIMARY KEY,
    country_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE categories (
    category_id   SERIAL PRIMARY KEY,
    category_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE brands (
    brand_id   SERIAL PRIMARY KEY,
    brand_name VARCHAR(100) NOT NULL UNIQUE
);

CREATE TABLE colors (
    color_id   SERIAL PRIMARY KEY,
    color_name VARCHAR(50) NOT NULL UNIQUE
);

CREATE TABLE sizes (
    size_id    SERIAL PRIMARY KEY,
    size_label VARCHAR(10) NOT NULL UNIQUE
);

CREATE TABLE age_ranges (
    age_range_id    SERIAL PRIMARY KEY,
    age_range_label VARCHAR(20) NOT NULL UNIQUE
);

CREATE TABLE channels (
    channel_id    SERIAL PRIMARY KEY,
    channel_name  VARCHAR(50) NOT NULL UNIQUE,
    campaign_name VARCHAR(100)
);

CREATE TABLE customers (
    customer_id  INTEGER PRIMARY KEY,
    first_name   VARCHAR(100),
    last_name    VARCHAR(100),
    email        VARCHAR(255),
    gender       gender_enum,
    age_range_id INTEGER NOT NULL REFERENCES age_ranges(age_range_id),
    signup_date  DATE,
    country_id   INTEGER NOT NULL REFERENCES countries(country_id)
);

CREATE TABLE products (
    product_id    INTEGER PRIMARY KEY,
    product_name  VARCHAR(255) NOT NULL,
    category_id   INTEGER NOT NULL REFERENCES categories(category_id),
    brand_id      INTEGER NOT NULL REFERENCES brands(brand_id),
    color_id      INTEGER NOT NULL REFERENCES colors(color_id),
    size_id       INTEGER NOT NULL REFERENCES sizes(size_id),
    catalog_price NUMERIC(10, 2) NOT NULL,
    cost_price    NUMERIC(10, 2) NOT NULL
);

CREATE TABLE sales (
    sale_id     INTEGER PRIMARY KEY,
    sale_date   DATE NOT NULL,
    customer_id INTEGER NOT NULL REFERENCES customers(customer_id),
    channel_id  INTEGER NOT NULL REFERENCES channels(channel_id)
);

CREATE TABLE sale_items (
    item_id          INTEGER PRIMARY KEY,
    sale_id          INTEGER NOT NULL REFERENCES sales(sale_id),
    product_id       INTEGER NOT NULL REFERENCES products(product_id),
    quantity         INTEGER NOT NULL CHECK (quantity > 0),
    original_price   NUMERIC(10, 2) NOT NULL,
    discount_applied NUMERIC(10, 2) NOT NULL DEFAULT 0.00
);
