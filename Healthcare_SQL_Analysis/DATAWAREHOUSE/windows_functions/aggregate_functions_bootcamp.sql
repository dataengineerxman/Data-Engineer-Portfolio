SELECT * FROM dbo.Orders

SELECT COUNT(sales)
FROM dbo.Orders

SELECT SUM(sales)
FROM dbo.Orders

SELECT AVG(sales)
FROM dbo.Orders

SELECT MAX(sales)
FROM dbo.Orders

SELECT MIN(sales)
FROM dbo.Orders

/*Total Number of Orders*/
SELECT * FROM dbo.Orders;

SELECT
    COUNT(*) AS Total_Number_Of_Orders,
    SUM(sales) AS Total_Sales,
    AVG(sales) AS avg_sales,
    MAX(sales) AS highest_sale,
    MIN(sales) AS minimum_sale
FROM dbo.Orders

SELECT customer_id,
    COUNT(*) AS Total_Number_Of_Orders,
    SUM(sales) AS Total_Sales,
    AVG(sales) AS avg_sales,
    MAX(sales) AS highest_sale,
    MIN(sales) AS minimum_sale
FROM dbo.Orders
GROUP BY customer_id

SELECT * FROM dbo.customers;

SELECT count(*) AS customers,
       SUM(score) AS total_score,
       AVG(score) AS avg_score,
       MAX(score) AS highest_score,
       MIN(score) AS min_score
FROM dbo.customers;


SELECT country,
       count(*) AS customers,
       SUM(score) AS total_score,
       AVG(score) AS avg_score,
       MAX(score) AS highest_score,
       MIN(score) AS min_score
FROM dbo.customers
GROUP BY country



