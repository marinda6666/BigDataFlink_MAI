CREATE TABLE IF NOT EXISTS dim_customers (
    customer_id INT PRIMARY KEY,
    customer_first_name VARCHAR,
    customer_last_name VARCHAR,
    customer_email VARCHAR,
    customer_age INT,
    customer_country VARCHAR,
    customer_postal_code VARCHAR,
    customer_pet_type VARCHAR,
    customer_pet_name VARCHAR,
    customer_pet_breed VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_stores (
    store_name VARCHAR PRIMARY KEY,
    store_location VARCHAR,
    store_city VARCHAR,
    store_state VARCHAR,
    store_country VARCHAR,
    store_phone VARCHAR,
    store_email VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_suppliers (
    supplier_name VARCHAR PRIMARY KEY,
    supplier_contact VARCHAR,
    supplier_email VARCHAR,
    supplier_phone VARCHAR,
    supplier_address TEXT,
    supplier_city VARCHAR,
    supplier_country VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_sellers (
    seller_id INT PRIMARY KEY,
    seller_first_name VARCHAR,
    seller_last_name VARCHAR,
    seller_email VARCHAR,
    seller_country VARCHAR,
    seller_postal_code VARCHAR
);

CREATE TABLE IF NOT EXISTS dim_products (
    product_id INT PRIMARY KEY,
    product_name VARCHAR,
    product_category VARCHAR,
    product_brand VARCHAR,
    product_price DECIMAL(10,2),
    product_weight DECIMAL(10,2),
    product_color VARCHAR,
    product_size VARCHAR,
    product_material VARCHAR,
    product_description TEXT,
    product_rating DECIMAL(3,1),
    product_reviews INT,
    product_release_date VARCHAR,
    product_expiry_date VARCHAR,
    pet_category VARCHAR,
    supplier_id INT
);

CREATE TABLE IF NOT EXISTS fact_sales (
    sale_id SERIAL PRIMARY KEY,
    sale_date DATE,
    customer_id INT,
    product_id INT,
    seller_id INT,
    quantity INT,
    total_price DECIMAL(12,2)
);
