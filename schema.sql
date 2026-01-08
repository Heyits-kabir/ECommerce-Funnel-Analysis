-- ==========================================
-- E-COMMERCE FUNNEL ANALYSIS SCHEMA
-- Database: ecommerce_funnel
-- ==========================================

DROP DATABASE IF EXISTS ecommerce_funnel;
CREATE DATABASE ecommerce_funnel;
USE ecommerce_funnel;

-- ==========================================
-- 1. DIMENSION TABLES (Must be created first)
-- ==========================================

-- 1.1 Customers Dimension
CREATE TABLE dim_customers (
    customer_id VARCHAR(50) PRIMARY KEY,
    customer_unique_id VARCHAR(50) NOT NULL,
    customer_zip_code_prefix INT,
    customer_city VARCHAR(100),
    customer_state VARCHAR(5)
);

-- 1.2 Products Dimension
CREATE TABLE dim_products (
    product_id VARCHAR(50) PRIMARY KEY,
    category_name VARCHAR(100),
    product_name_lenght INT,
    product_description_lenght INT,
    product_photos_qty INT,
    product_weight_g DECIMAL(10,2),
    product_length_cm DECIMAL(10,2),
    product_height_cm DECIMAL(10,2),
    product_width_cm DECIMAL(10,2)
);

-- 1.3 Sellers Dimension
CREATE TABLE dim_sellers (
    seller_id VARCHAR(50) PRIMARY KEY,
    seller_zip_code_prefix INT,
    seller_city VARCHAR(100),
    seller_state VARCHAR(5)
);

-- 1.4 Geolocation Dimension (Lookup Table)
-- Note: Zip codes are not unique in raw data (many coords per zip).
-- We will load a deduplicated version or allow duplicates if analyzing granular location.
CREATE TABLE dim_geo (
    geolocation_zip_code_prefix INT,
    geolocation_lat DECIMAL(18,15),
    geolocation_lng DECIMAL(18,15),
    geolocation_city VARCHAR(100),
    geolocation_state VARCHAR(5)
    -- No PK here because zip codes repeat in the raw file. 
    -- We can add an INDEX for performance.
);
CREATE INDEX idx_geo_zip ON dim_geo(geolocation_zip_code_prefix);

-- ==========================================
-- 2. FACT TABLES (Transactional Data)
-- ==========================================

-- 2.1 Orders Fact (The Core Funnel)
CREATE TABLE fact_orders (
    order_id VARCHAR(50) PRIMARY KEY,
    customer_id VARCHAR(50),
    order_status VARCHAR(50),
    order_purchase_timestamp DATETIME,
    order_approved_at DATETIME,
    order_delivered_carrier_date DATETIME,
    order_delivered_customer_date DATETIME,
    order_estimated_delivery_date DATETIME,
    delivery_days INT, -- Derived in Python
    is_late TINYINT,   -- Derived in Python (0 or 1)
    
    FOREIGN KEY (customer_id) REFERENCES dim_customers(customer_id) 
    ON DELETE CASCADE ON UPDATE CASCADE
);

-- 2.2 Order Items (Links Orders, Products, and Sellers)
CREATE TABLE fact_order_items (
    order_id VARCHAR(50),
    order_item_id INT,
    product_id VARCHAR(50),
    seller_id VARCHAR(50),
    shipping_limit_date DATETIME,
    price DECIMAL(10,2),
    freight_value DECIMAL(10,2),
    
    -- Composite Primary Key (Order + Item Number)
    PRIMARY KEY (order_id, order_item_id),
    
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id),
    FOREIGN KEY (product_id) REFERENCES dim_products(product_id),
    FOREIGN KEY (seller_id) REFERENCES dim_sellers(seller_id)
);

-- 2.3 Payments
CREATE TABLE fact_payments (
    order_id VARCHAR(50),
    payment_sequential INT,
    payment_type VARCHAR(50),
    payment_installments INT,
    payment_value DECIMAL(10,2),
    
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);

-- 2.4 Reviews
CREATE TABLE fact_reviews (
    review_id VARCHAR(50),
    order_id VARCHAR(50),
    review_score INT,
    review_comment_title TEXT,
    review_comment_message TEXT,
    review_creation_date DATETIME,
    review_answer_timestamp DATETIME,
    
    -- Note: Review ID is not always unique in Olist (rare edge cases), 
    -- but usually it is. We will index order_id for joining.
    INDEX idx_review_order (order_id),
    FOREIGN KEY (order_id) REFERENCES fact_orders(order_id)
);

-- ==========================================
-- 3. COMPETITOR DATA (Flipkart)
-- ==========================================

CREATE TABLE competitor_flipkart (
    uniq_id VARCHAR(50) PRIMARY KEY,
    crawl_timestamp DATETIME,
    product_name TEXT,
    main_category VARCHAR(255),
    pid VARCHAR(50),
    retail_price DECIMAL(10,2),
    discounted_price DECIMAL(10,2),
    discount_pct DECIMAL(5,2),
    brand VARCHAR(100),
    product_rating VARCHAR(50),
    overall_rating VARCHAR(50)
);