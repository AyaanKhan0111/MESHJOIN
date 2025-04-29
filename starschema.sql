-- Step 1: Create the star schema database (Drop if already exists)
DROP DATABASE IF EXISTS starschema;
CREATE DATABASE starschema;
USE starschema;

-- Step 2: Drop tables if they already exist
DROP TABLE IF EXISTS fact_sales;
DROP TABLE IF EXISTS dim_order;
DROP TABLE IF EXISTS dim_date;
DROP TABLE IF EXISTS dim_store;
DROP TABLE IF EXISTS dim_supplier;
DROP TABLE IF EXISTS dim_customer;
DROP TABLE IF EXISTS dim_product;

-- Step 3: Create the dimension tables

-- Product Dimension
CREATE TABLE dim_product (
    ProductID INT PRIMARY KEY,
    productName VARCHAR(255),
    productPrice DECIMAL(10, 2)
);

-- Customer Dimension
CREATE TABLE dim_customer (
    customer_id INT PRIMARY KEY,
    customer_name VARCHAR(255),
    gender VARCHAR(10)
);

-- Store Dimension
CREATE TABLE dim_store (
    storeID INT PRIMARY KEY,
    storeName VARCHAR(255)
);

-- Supplier Dimension
CREATE TABLE dim_supplier (
    supplierID INT PRIMARY KEY,
    supplierName VARCHAR(255)
);

-- Date Dimension
CREATE TABLE dim_date (
    date_id INT PRIMARY KEY AUTO_INCREMENT,
    full_date DATE NOT NULL,
    year INT,
    quarter INT,
    month INT,
    week INT,
    day INT,
    day_of_week VARCHAR(10)
);

-- Order Dimension
CREATE TABLE dim_order (
    order_id INT PRIMARY KEY,
    time_id INT,
    order_date DATETIME,
    hour INT,
    minute INT,
    second INT
);

-- Fact Sales Table
CREATE TABLE fact_sales (
    Order_ID INT PRIMARY KEY,
    date_id INT,
    ProductID INT,
    customer_id INT,
    storeID INT,
    supplierID INT,
    Quantity INT,
    total_sale DECIMAL(10, 2),
    FOREIGN KEY (Order_ID) REFERENCES dim_order(order_id),
    FOREIGN KEY (date_id) REFERENCES dim_date(date_id),
    FOREIGN KEY (ProductID) REFERENCES dim_product(ProductID),
    FOREIGN KEY (customer_id) REFERENCES dim_customer(customer_id),
    FOREIGN KEY (storeID) REFERENCES dim_store(storeID),
    FOREIGN KEY (supplierID) REFERENCES dim_supplier(supplierID)
);

-- Step 4: Insert data into dimension tables

-- Insert data into dim_product (Removed supplierID and supplierName)
INSERT INTO dim_product (ProductID, productName, productPrice)
SELECT DISTINCT ProductID, productName, productPrice
FROM metro.combined;

-- Insert data into dim_customer
INSERT INTO dim_customer (customer_id, customer_name, gender)
SELECT DISTINCT customer_id, customer_name, gender
FROM metro.combined;

-- Insert data into dim_store
INSERT INTO dim_store (storeID, storeName)
SELECT DISTINCT storeID, storeName
FROM metro.combined;

-- Insert data into dim_supplier
INSERT INTO dim_supplier (supplierID, supplierName)
SELECT DISTINCT supplierID, supplierName
FROM metro.combined
WHERE supplierID IS NOT NULL;

-- Generate data for dim_date
INSERT INTO dim_date (full_date, year, quarter, month, week, day, day_of_week)
SELECT DISTINCT 
    DATE(Order_Date) AS full_date,
    YEAR(Order_Date) AS year,
    QUARTER(Order_Date) AS quarter,
    MONTH(Order_Date) AS month,
    WEEK(Order_Date) AS week,
    DAY(Order_Date) AS day,
    DAYNAME(Order_Date) AS day_of_week
FROM metro.combined;

-- Insert data into dim_order
INSERT INTO dim_order (order_id, time_id, order_date, hour, minute, second)
SELECT DISTINCT 
    Order_ID,
    time_id,
    Order_Date,
    HOUR(Order_Date),
    MINUTE(Order_Date),
    SECOND(Order_Date)
FROM metro.combined;

-- Step 5: Insert data into the fact_sales table
INSERT INTO fact_sales (Order_ID, date_id, ProductID, customer_id, storeID, supplierID, Quantity, total_sale)
SELECT 
    c.Order_ID,
    d.date_id,
    c.ProductID,
    c.customer_id,
    c.storeID,
    sp.supplierID, 
    c.Quantity,
    c.total_sale
FROM metro.combined c
JOIN dim_date d ON DATE(c.Order_Date) = d.full_date
JOIN dim_supplier sp ON c.supplierID = sp.supplierID;
