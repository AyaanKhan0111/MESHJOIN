-- CREATE DATABASE IF NOT EXISTS metro;
USE metro;
-- CREATE TABLE IF NOT EXISTS products (
--     productID INT PRIMARY KEY,
--     productName VARCHAR(255),
--     productPrice DECIMAL(10, 2),
--     supplierID INT,
--     supplierName VARCHAR(255),
--     storeID INT,
--     storeName VARCHAR(255)
-- );

-- CREATE TABLE IF NOT EXISTS customers (
--     customer_id INT PRIMARY KEY,
--     customer_name VARCHAR(255),
--     gender VARCHAR(10)
-- );

-- for combined data--
CREATE TABLE IF NOT EXISTS combined (
    Order_ID INT NOT NULL,
    Order_Date DATETIME NOT NULL,
    ProductID INT NOT NULL,
    Quantity INT NOT NULL,
    customer_id INT NOT NULL,
    time_id INT NOT NULL,
    customer_name VARCHAR(255),
    gender VARCHAR(10),
    productName VARCHAR(255),
    productPrice DECIMAL(10, 2),
    supplierID INT,
    supplierName VARCHAR(255),
    storeID INT,
    storeName VARCHAR(255),
    total_sale DECIMAL(10, 2),
    PRIMARY KEY (Order_ID)
);





