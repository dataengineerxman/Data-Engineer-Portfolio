/*SubQuery Result Types*/
/*Scalar:
+ Returns one value
+ Use Aggregations*/
/*One Single Value*/
SELECT AVG(sales)
FROM Sales.Orders

/*Rows: Multiple Rows and One Column */
SELECT CustomerID
FROM Sales.Orders

/*Rows: Multiple Rows and Multiple Columns */
SELECT OrderID,
       OrderDate
FROM Sales.Orders

/*Location*/
/*FROM*/
SELECT *
FROM (
SELECT ProductID,
       price,
       AVG(price) OVER() as avg_price
FROM Sales.Products) t
WHERE price > avg_price

SELECT ProductID,
       price
FROM Sales.Products
WHERE price> (SELECT AVG(price)
FROM Sales.Products)


/*Total Amount of Sales*/
SELECT
    RANK() OVER(ORDER BY Total_Sales) AS Rank
FROM (
SELECT SUM(Sales) OVER() AS Total_Sales
FROM Sales.Orders) AS t




