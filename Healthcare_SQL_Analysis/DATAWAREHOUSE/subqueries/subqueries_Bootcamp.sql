/*INFORMATION SCHEMA*/
SELECT *
FROM INFORMATION_SCHEMA.TABLES

SELECT *
FROM INFORMATION_SCHEMA.COLUMNS

/*Subquery FROM*/
/*Find Product that have a price higher than average Price*/
SELECT *
FROM
    (
        SELECT ProductID,
               price,
               AVG(price) OVER() AVGPrice
        FROM SalesDB.Sales.Products) t
WHERE price>AVGPrice

/*Rank Customers based on their Total Amount of Sales*/
SELECT
    CustomerID,
    RANK() OVER (ORDER BY TotalSales DESC) AS Rank
FROM (
        SELECT
            CustomerID,
            SUM(Sales)  AS TotalSales
        FROM SalesDB.Sales.Orders
        GROUP BY CustomerID) t

/*SubQuery in SELECT*/
/*ProductID, ProductName, Prices and total number of Orders*/
SELECT
    ProductID,
    Product,
    Price,
    (SELECT COUNT(*) FROM SalesDB.Sales.Orders o) AS TotalOrders
FROM SalesDB.Sales.Products p

/*SubQuery in JOIN*/
/*Show all Customer Details and find the Total Orders for each Customer*/

SELECT c.*,
       o.TotalOrders
FROM SalesDB.Sales.Customers c
JOIN (
SELECT CustomerID,
       COUNT(*) AS TotalOrders
FROM SalesDB.Sales.Orders
GROUP BY CustomerID) o
ON c.customerID=o.CustomerID

/*Sub-Query in WHERE*/
/*Find the Products that have a price higher than average price for all Products*/
SELECT *,
       (SELECT AVG(price) FROM SalesDB.Sales.Products) AS AVGPrice
FROM SalesDB.Sales.Products
WHERE Price > (SELECT
               AVG(price) AVGPrice
        FROM SalesDB.Sales.Products)

/*SubQuery Using IN Operator*/
/*Order for Customers in Germany*/
SELECT *
FROM SalesDB.Sales.Orders
WHERE CustomerID IN (SELECT CustomerID
                     FROM SalesDB.Sales.Customers
                     WHERE Country='Germany')

SELECT *
FROM SalesDB.Sales.Orders
WHERE CustomerID NOT IN (SELECT CustomerID
                     FROM SalesDB.Sales.Customers
                     WHERE Country='Germany')

/*ALL & ANY*/
/*Female Employees whose salaries are greater than salaries of any male employees*/
SELECT *
FROM SalesDB.Sales.Employees
WHERE gender = 'F' AND
    salary > ANY (SELECT Salary FROM SalesDB.Sales.Employees
                                      WHERE Gender = 'M')

/*Female Employees whose salaries are greater than salaries of all male employees*/
SELECT *
FROM SalesDB.Sales.Employees
WHERE gender = 'F' AND
    salary > ALL (SELECT Salary FROM SalesDB.Sales.Employees
                                      WHERE Gender = 'M')

/*Correlated SubQuery*/
/*Show all Customer Details and find total Orders*/
/*Correlated*/
SELECT *,
       (SELECT count(*) FROM SalesDB.Sales.Orders o
                        WHERE o.CustomerID=c.CustomerID) AS TotalSales
FROM SalesDB.Sales.Customers c

--Exists: Subquery returns a value of not
--Show details of Customer of Orders made in Germany
SELECT *
FROM SalesDB.Sales.Orders o
WHERE EXISTS (SELECT 1 FROM SalesDB.Sales.Customers c
                       WHERE c.Country='Germany'
                       AND   o.CustomerID=c.CustomerID)

SELECT *
FROM SalesDB.Sales.Orders o
WHERE NOT EXISTS (SELECT 1 FROM SalesDB.Sales.Customers c
                       WHERE c.Country='Germany'
                       AND   o.CustomerID=c.CustomerID)









