-- Databricks SQL / %sql-ready analytics queries for amazon_sales_delta or parquet aggregates
-- Replace default.amazon_sales_delta with your table name if different

-- 1) Total revenue and units sold overall
SELECT SUM(total_price) AS total_revenue,
       SUM(quantity) AS total_units_sold
FROM default.amazon_sales_delta;

-- 2) Daily revenue time series (last 365 days)
SELECT sale_date,
       SUM(total_price) AS revenue,
       SUM(quantity) AS units_sold
FROM default.amazon_sales_delta
WHERE sale_date IS NOT NULL AND sale_date >= date_sub(current_date(), 365)
GROUP BY sale_date
ORDER BY sale_date;

-- 3) Top 20 products by revenue
SELECT product_id_canon AS product_id,
       SUM(total_price) AS revenue,
       COUNT(DISTINCT order_id) AS orders
FROM default.amazon_sales_delta
GROUP BY product_id_canon
ORDER BY revenue DESC
LIMIT 20;

-- 4) Seller performance (top 50)
SELECT seller_canon AS seller_id,
       COUNT(*) AS orders,
       SUM(total_price) AS revenue,
       AVG(total_price) AS avg_order_value
FROM default.amazon_sales_delta
GROUP BY seller_canon
ORDER BY revenue DESC
LIMIT 50;

-- 5) Monthly cohort-style retention (first purchase month -> activity month)
WITH first_purchase AS (
  SELECT customer_id, MIN(sale_date) AS first_date
  FROM default.amazon_sales_delta
  GROUP BY customer_id
)
SELECT date_trunc('month', f.first_date) AS cohort_month,
       date_trunc('month', s.sale_date) AS activity_month,
       COUNT(DISTINCT s.customer_id) AS active_customers,
       SUM(s.total_price) AS revenue
FROM default.amazon_sales_delta s
JOIN first_purchase f ON s.customer_id = f.customer_id
GROUP BY cohort_month, activity_month
ORDER BY cohort_month, activity_month;