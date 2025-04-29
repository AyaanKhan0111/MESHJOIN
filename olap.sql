use starschema;
-- Q1: Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
-- Find the top 5 products that generated the highest revenue, separated by weekday and weekend sales, with results grouped by month for a specified year.

SELECT 
    p.productName,
    d.month,
    CASE 
        WHEN d.day_of_week IN ('Saturday', 'Sunday') THEN 'Weekend'
        ELSE 'Weekday'
    END AS day_type,
    SUM(f.total_sale) AS total_revenue
FROM fact_sales f
JOIN dim_product p ON f.ProductID = p.ProductID
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2019  
GROUP BY p.productName, d.month, day_type
ORDER BY total_revenue DESC
LIMIT 5;

-- Q2: Trend Analysis of Store Revenue Growth Rate Quarterly for 2019
-- Calculate the revenue growth rate for each store on a quarterly basis for 2019.

SELECT 
    s.storeName,
    d.quarter,
    SUM(f.total_sale) AS total_revenue,
    LAG(SUM(f.total_sale)) OVER (PARTITION BY s.storeID ORDER BY d.quarter) AS previous_quarter_revenue,
    CASE
        WHEN LAG(SUM(f.total_sale)) OVER (PARTITION BY s.storeID ORDER BY d.quarter) IS NOT NULL 
        THEN (SUM(f.total_sale) - LAG(SUM(f.total_sale)) OVER (PARTITION BY s.storeID ORDER BY d.quarter)) / LAG(SUM(f.total_sale)) OVER (PARTITION BY s.storeID ORDER BY d.quarter) * 100
        ELSE 0
    END AS growth_rate
FROM fact_sales f
JOIN dim_store s ON f.storeID = s.storeID
JOIN dim_date d ON f.date_id = d.date_id
WHERE d.year = 2019
GROUP BY s.storeID, d.quarter
ORDER BY s.storeName, d.quarter;


-- Q3: Detailed Supplier Sales Contribution by Store and Product Name
-- For each store, show the total sales contribution of each supplier broken down by product name. Group results by store, then supplier, and then product name.
SELECT 
    st.storeName,
    sp.supplierName,
    p.productName,
    SUM(f.total_sale) AS total_sales
FROM fact_sales f
JOIN dim_store st ON f.storeID = st.storeID
JOIN dim_product p ON f.ProductID = p.ProductID
JOIN dim_supplier sp ON f.supplierID = sp.supplierID -- Updated join to use fact_sales for supplierID
GROUP BY st.storeName, sp.supplierName, p.productName
ORDER BY st.storeName, sp.supplierName, p.productName;

-- Q4: Seasonal Analysis of Product Sales Using Dynamic Drill-Down
-- Present total sales for each product, drilled down by seasonal periods (Spring, Summer, Fall, Winter).

SELECT 
    p.productName,
    CASE 
        WHEN d.month IN (3, 4, 5) THEN 'Spring'
        WHEN d.month IN (6, 7, 8) THEN 'Summer'
        WHEN d.month IN (9, 10, 11) THEN 'Fall'
        WHEN d.month IN (12, 1, 2) THEN 'Winter'
    END AS season,
    SUM(f.total_sale) AS total_sales
FROM fact_sales f
JOIN dim_product p ON f.ProductID = p.ProductID
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY p.productName, season
ORDER BY season, total_sales DESC;

-- Q5: Store-Wise and Supplier-Wise Monthly Revenue Volatility
-- Calculate the month-to-month revenue volatility for each store and supplier pair.

SELECT 
    st.storeName,
    sp.supplierName,
    d.year,
    SUM(f.total_sale) AS total_sales,
    LAG(SUM(f.total_sale)) OVER (PARTITION BY st.storeID, f.supplierID ORDER BY d.year) AS previous_month_sales,
    CASE
        WHEN LAG(SUM(f.total_sale)) OVER (PARTITION BY st.storeID, f.supplierID ORDER BY d.year) IS NOT NULL
        THEN (SUM(f.total_sale) - LAG(SUM(f.total_sale)) OVER (PARTITION BY st.storeID, f.supplierID ORDER BY d.year)) / LAG(SUM(f.total_sale)) OVER (PARTITION BY st.storeID, f.supplierID ORDER BY d.year) * 100
        ELSE 0
    END AS revenue_volatility
FROM fact_sales f
JOIN dim_store st ON f.storeID = st.storeID
JOIN dim_product p ON f.ProductID = p.ProductID
JOIN dim_supplier sp ON f.supplierID = sp.supplierID -- Updated join to use fact_sales for supplierID
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY st.storeID, st.storeName, f.supplierID, sp.supplierName, d.year
ORDER BY st.storeName, sp.supplierName, d.year;



-- Q6: Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)
-- Identify the top 5 products frequently bought together within a set of orders.

SELECT 
    p1.productName AS product1,
    p2.productName AS product2,
    COUNT(*) AS frequency
FROM fact_sales f1
JOIN fact_sales f2 ON f1.Order_ID = f2.Order_ID AND f1.ProductID < f2.ProductID
JOIN dim_product p1 ON f1.ProductID = p1.ProductID
JOIN dim_product p2 ON f2.ProductID = p2.ProductID
GROUP BY p1.productName, p2.productName
ORDER BY frequency DESC
LIMIT 5;


-- Q7: Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
-- Use the ROLLUP operation to aggregate yearly revenue data by store, supplier, and product.

SELECT 
    st.storeName,
    sp.supplierName,
    p.productName,
    SUM(f.total_sale) AS total_sales
FROM fact_sales f
JOIN dim_store st ON f.storeID = st.storeID
JOIN dim_product p ON f.ProductID = p.ProductID
JOIN dim_supplier sp ON f.supplierID = sp.supplierID -- Updated join to use fact_sales for supplierID
GROUP BY st.storeName, sp.supplierName, p.productName WITH ROLLUP
ORDER BY st.storeName, sp.supplierName, p.productName;

-- Q8: Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
-- For each product, calculate the total revenue and quantity sold in the first and second halves of the year.

SELECT 
    p.productName,
    SUM(CASE WHEN d.year = 2019 THEN f.total_sale ELSE 0 END) AS H1_total_sales,
    SUM(CASE WHEN d.year = 2019 THEN f.Quantity ELSE 0 END) AS H1_quantity,
    SUM(CASE WHEN d.year > 2019 THEN f.total_sale ELSE 0 END) AS H2_total_sales,
    SUM(CASE WHEN d.year > 2019 THEN f.Quantity ELSE 0 END) AS H2_quantity,
    SUM(f.total_sale) AS total_sales,
    SUM(f.Quantity) AS total_quantity
FROM fact_sales f
JOIN dim_product p ON f.ProductID = p.ProductID
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY p.productName
ORDER BY p.productName;

-- Q9: Identify High Revenue Spikes in Product Sales and Highlight Outliers
-- Calculate daily average sales for each product and flag days where the sales exceed twice the daily average by product as potential outliers.

WITH avg_daily_sales AS (
    SELECT 
        f.ProductID,
        d.date_id,
        d.full_date,
        AVG(f.total_sale) AS avg_sales
    FROM fact_sales f
    JOIN dim_date d ON f.date_id = d.date_id
    GROUP BY f.ProductID, d.date_id, d.full_date
)
SELECT 
    f.ProductID,
    f.Order_ID,
    f.total_sale,
    a.avg_sales,
    CASE 
        WHEN f.total_sale > 2 * a.avg_sales THEN 'Outlier'
        ELSE 'Normal'
    END AS sale_status
FROM fact_sales f
JOIN avg_daily_sales a ON f.ProductID = a.ProductID AND f.date_id = a.date_id
ORDER BY f.ProductID, f.Order_ID;


-- Q10: Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
-- Create a view named STORE_QUARTERLY_SALES that aggregates total quarterly sales by store, ordered by store name.

CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT 
    st.storeName,
    d.year,
    d.quarter,
    SUM(f.total_sale) AS total_sales
FROM fact_sales f
JOIN dim_store st ON f.storeID = st.storeID
JOIN dim_date d ON f.date_id = d.date_id
GROUP BY st.storeName, d.year, d.quarter
ORDER BY st.storeName, d.year, d.quarter;
