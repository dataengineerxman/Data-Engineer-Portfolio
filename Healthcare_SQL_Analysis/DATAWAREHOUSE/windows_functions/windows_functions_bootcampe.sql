SELECT
    orderId,
    orderdate,
    productId,
    SUM(sales) OVER() AS Total
FROM SalesDB.Sales.Orders

/*Partition*/
SELECT
    orderId,
    orderdate,
    productId,
    SUM(sales) OVER(Partition BY ProductID) AS Total
FROM SalesDB.Sales.Orders


SELECT
    orderId,
    orderdate,
    productId,
    Sales,
    SUM(sales) OVER() AS TotalSales,
    SUM(sales) OVER(Partition BY ProductID) AS TotalByProd
FROM SalesDB.Sales.Orders


SELECT
    orderId,
    orderdate,
    productId,
    OrderStatus,
    Sales,
    SUM(sales) OVER(Partition BY ProductID) AS TotalByProd,
    SUM(sales) OVER(Partition BY ProductID, OrderStatus) AS TotalSalesByProductStatus
FROM SalesDB.Sales.Orders

/*ORDER BY*/
SELECT
    orderId,
    orderdate,
    sales,
    RANK() OVER(Order BY sales DESC) AS RankSales
FROM SalesDB.Sales.Orders


/*Frame*/
SELECT
    orderId,
    orderdate,
    orderstatus,
    sales,
    SUM(sales) OVER (PARTITION BY OrderStatus Order By OrderDate
    ROWS BETWEEN CURRENT ROW AND 2 FOLLOWING) AS TotalSales
FROM SalesDB.Sales.Orders;

/*Shortcut with Preceding*/
/*2 previous row from current row*/
SELECT
    orderId,
    orderdate,
    orderstatus,
    sales,
    SUM(sales) OVER (PARTITION BY OrderStatus Order By OrderDate
    ROWS 2 PRECEDING ) AS TotalSales
FROM SalesDB.Sales.Orders;


/*Default*/
/*ROW BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW*/
SELECT
    orderId,
    orderdate,
    orderstatus,
    sales,
    SUM(sales) OVER (PARTITION BY OrderStatus Order By OrderDate) AS TotalSalesDefault,
    SUM(sales) OVER (PARTITION BY OrderStatus Order By OrderDate
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW ) AS TotalSales
FROM SalesDB.Sales.Orders;

/*Can Only Be used in SELECT AND ORDER BY*/
  SELECT
    orderId,
    orderdate,
    orderstatus,
    sales,
    SUM(sales) OVER (PARTITION BY OrderStatus Order By OrderDate) AS TotalSales
    FROM SalesDB.Sales.Orders
ORDER BY orderstatus, SUM(sales) OVER (Partition BY OrderStatus ORDER BY orderdate) DESC;

/*Can be used with GROUP BY as long as you use the same Columns*/
/*Rank Customers based on Total Sales*/
SELECT customerID,
       sales,
       RANK() OVER(PARTITION BY  CustomerID ORDER BY customerID, sales DESC) AS ranking,
       SUM(sales) OVER(Partition BY CustomerID) AS TotalSales
FROM SalesDB.Sales.Orders
GROUP BY customerID, sales


SELECT customerID,
       SUM(sales)  AS TotalSales,
       RANK() OVER(ORDER BY SUM(sales) DESC) AS ranking
FROM SalesDB.Sales.Orders
GROUP BY customerID


/*Window Aggregate Functions*/
/*Count*/
SELECT productID,
       customerID,
       orderID,
       sales,
       COUNT(*) OVER (PARTITION BY ProductID) AS Number_Of_Rows,
       COUNT(1) OVER (PARTITION BY ProductID) AS Number_Of_Rows_with1,
       COUNT(sales) OVER (PARTITION BY ProductID) AS Number_Of_Sales,
       COUNT(*) OVER (PARTITION BY CustomerID) AS OrdersByCustomer,
       COUNT(*) OVER (PARTITION BY OrderID) AS CheckPK_Duplicates
FROM SalesDB.Sales.Orders
ORDER BY customerID, productID

--Find Duplicates
SELECT *
FROM (
SELECT orderID,
       sales,
       COUNT(*) OVER (PARTITION BY OrderID) AS CheckPK_Duplicates
FROM SalesDB.Sales.OrdersArchive) t
WHERE CheckPK_Duplicates>1

/*SUM*/
SELECT productID,
       customerID,
       orderID,
       orderdate,
       sales,
       SUM(Sales) OVER() SumOfSales,
       SUM(Sales) OVER(PARTITION BY productID) SumOfSalesProduct,
       SUM(Sales) OVER(PARTITION BY customerID) SumOfSalesCustomer
FROM SalesDB.Sales.Orders

/*Compare Month Sales to Total Sales*/
SELECT
    orderId,
    productID,
    sales,
    SUM(sales) OVER() AS TotalSales,
    ROUND(CAST (sales AS float) / SUM(sales) OVER() * 100,2) AS TotalSalesPercent
FROM SalesDB.Sales.Orders

/*AVG*/
SELECT
    orderId,
    productID,
    customerId,
    sales,
    AVG(sales) OVER() AS AVGSales,
    AVG(sales) OVER (PARTITION BY ProductID) AS AVGByProduct,
    AVG(sales) OVER (PARTITION BY CustomerID) AS AVGByCustomer
FROM SalesDB.Sales.Orders

SELECT *
FROM (
SELECT
    OrderID,
    ProductID,
    Sales,
    AVG(sales) OVER() AS avgsales
FROM SalesDB.Sales.Orders ) t
WHERE sales > avgSales


/*MIN & MAX*/
SELECT
    orderId,
    productID,
    customerId,
    sales,
    MIN(sales) OVER() AS MINSale,
    MAX(sales) OVER() AS MAXSale,
    MIN(sales) OVER (PARTITION BY ProductID) AS MINByProduct,
    MAX(sales) OVER (PARTITION BY ProductID) AS MAXByProduct,
    MIN(sales) OVER (PARTITION BY CustomerID) AS MINByCustomer,
    MAX(sales) OVER (PARTITION BY CustomerID) AS MAXByCustomer
FROM SalesDB.Sales.Orders

SELECT
    Department,
    Salary,
    MAX(salary) OVER() AS HighestSalary,
    MIN(salary) OVER() AS LowestSalary,
    MAX(salary) OVER(PARTITION BY Department) AS HighestSalaryByDepartment,
    MIN(salary) OVER(PARTITION BY Department) AS LowestSalaryByDepartment
FROM SalesDB.Sales.Employees

SELECT *
FROM (
    SELECT FirstName,
           LastName,
           Department,
           salary,
        MAX(salary) OVER() AS Highest_Salary
    FROM SalesDB.Sales.Employees
     ) t
WHERE salary = Highest_Salary

SELECT
    orderId,
    productID,
    customerId,
    sales,
    MIN(sales) OVER() AS MINSale,
    MAX(sales) OVER() AS MAXSale,
    Sales - MIN(sales) OVER() AS DeviationFromMin,
    MAX(sales)  OVER() - Sales AS DeviationFromMax
FROM SalesDB.Sales.Orders

/*Rolling & Running Total*/
/*
Tracking Current Sales with Target Sales
Providing insights into historical patterns
Aggregate sequence of members and the aggregation is
updated each time a new member is added: Analysis Over Time

Running Total
Aggregate all Values from the beginning up to the current point without
dropping off older data

Rolling Total
Aggregate all Values within a fixed time window (e.g. 30 days)
As new data is added the oldest data point will be dropped
*/

/*Running & RollingTotal(3 Months)*/
SELECT
    OrderDate,
    sales,
    DATENAME(MONTH, OrderDate) AS Month,
    SUM(sales) OVER (
        ORDER BY OrderDate
    ) AS RunningTotal,
    SUM(sales) OVER (
        ORDER BY OrderDate
        ROWS BETWEEN 2 PRECEDING AND
        CURRENT ROW
    ) AS RollingTotal
FROM SalesDB.Sales.Orders;

--Moving Average
SELECT
    OrderDate,
    sales,
    DATENAME(MONTH, OrderDate) AS Month,
    AVG(Sales) OVER(PARTITION BY ProductID) AVGByProduct,
    AVG(sales) OVER (
        PARTITION BY ProductID
        ORDER BY OrderDate
    ) AS AVGRunningTotal,
    AVG(sales) OVER (
        PARTITION BY ProductID
        ORDER BY OrderDate
        ROWS BETWEEN 2 PRECEDING AND
        CURRENT ROW
    ) AS RollingTotal
FROM SalesDB.Sales.Orders;


/*WINDOWS RANKING FUNCTIONS*/
/*
Sort Data first - Mandatory*/
/*
Integer Based Ranking - Number
    ROW_NUMBER()
    RANK()
    DENSE_RANK()
    NTILE()
Percentage Based Ranking - 0 to 1 and in between 0.25
    CUME_DIST()
    PERCENT_RANK()
*/

/*
Row Number
+ Unique number for each row -
+ 2 rows with same value - they do not share the same row number
*/

SELECT OrderID,
       ProductID,
       Sales,
       ROW_NUMBER() OVER(ORDER BY sales DESC) Sales_Row_Num
FROM SalesDB.Sales.Orders;

/*
 Rank: Handles the ties- share ranking
 2 rows with same value - share the same row number
 Leave gaps
*/

SELECT OrderID,
       ProductID,
       Sales,
       RANK() OVER(ORDER BY sales DESC) Rank
FROM SalesDB.Sales.Orders;

/*
 Dense Rank: It handles the ties
 2 rows with same value - they share the same row number
 No gaps
*/

SELECT OrderID,
       ProductID,
       Sales,
       DENSE_RANK() OVER(ORDER BY sales DESC) Rank
FROM SalesDB.Sales.Orders;

/*Row Number*/
/*Top Highest sales for each product*/
SELECT *
FROM (
SELECT OrderID,
       ProductID,
       Sales,
       ROW_NUMBER() OVER(PARTITION BY ProductID ORDER BY sales DESC) Row_Num
FROM SalesDB.Sales.Orders) t
WHERE Row_Num=1

/*Lowest Customer based on Total Sales*/
SELECT *
FROM (
    SELECT
        CustomerID,
        SUM(sales) AS TotalSales,
        ROW_NUMBER() OVER (ORDER BY SUM(sales) DESC) AS Row_Num
    FROM SalesDB.Sales.Orders
    GROUP BY CustomerID
) t
WHERE Row_Num <= 2;

/*Assign Unique IDs*/
SELECT '00' + CAST(ROW_NUMBER() OVER(ORDER BY OrderID, OrderDate) AS VARCHAR) Number,
       OrderID,
       ProductID
FROM SalesDB.Sales.OrdersArchive

/*Identify Duplicates*/
SELECT *
FROM (
SELECT OrderID,
       OrderStatus,
       CreationTime,
       RANK() OVER(PARTITION BY OrderID ORDER BY CreationTime DESC) rn
FROM SalesDB.Sales.OrdersArchive) t
WHERE rn>1

/*CUME_DIST*/
SELECT
    Sales,
    CUME_DIST() OVER(ORDER BY Sales DESC)
FROM SalesDB.Sales.Orders

/*PERCENT_RANK*/
SELECT
    Sales,
    ROUND(PERCENT_RANK() OVER(ORDER BY Sales DESC),2) AS PercentRank
FROM SalesDB.Sales.Orders

/*NTILE()*/
SELECT
    OrderID,
    Sales,
    NTILE(2) OVER (ORDER BY Sales DESC) AS Two_Buckets,
    NTILE(3) OVER (ORDER BY Sales DESC) AS Three_Buckets,
    NTILE(4) OVER (ORDER BY Sales DESC) AS Four_Buckets
FROM SalesDB.Sales.Orders


/*Segment Orders into High Medium and Low*/
SELECT *,
       CASE
           WHEN Buckets=1 THEN 'High'
           WHEN Buckets=2 THEN 'Medium'
           WHEN Buckets=3 THEN 'Low'
    END SalesSegmentation
FROM (
SELECT
    OrderID,
    Sales,
    NTILE(3) OVER (ORDER BY Sales DESC) AS Buckets
FROM SalesDB.Sales.Orders) t

/*Equalizing Load*/

SELECT *,
    NTILE(2) OVER (ORDER BY OrderID) AS Buckets
FROM SalesDB.Sales.Orders

--LAG (Month Over Month-Previous Month)
SELECT sales,
       LAG(Sales,1,0) OVER(ORDER BY OrderDate) AS Sales_Previous_Month
FROM SalesDB.Sales.Orders

--LEAD (Month Over Month-Next Month)
SELECT sales,
       LEAD(Sales,1,0) OVER(ORDER BY OrderDate) AS Sales_Next_Month
FROM SalesDB.Sales.Orders

--LAG-LEAD (Month Over Month-Performance - TOTAL SALES)
SELECT
    *,
    CurrentMonthSales - PreviousMonthSales AS MoMChange,
    ROUND(CAST((CurrentMonthSales - PreviousMonthSales) AS Float) / PreviousMonthSales*100,2) AS MoM_Perent
FROM (
SELECT MONTH(OrderDate) AS OrderMonth,
       SUM(Sales) AS CurrentMonthSales,
       LAG(SUM(Sales)) OVER (ORDER BY MONTH(OrderDate)) AS PreviousMonthSales
FROM SalesDB.Sales.Orders
GROUP BY MONTH(OrderDate)) t

/* Customer Retention*/
SELECT CustomerID,
       AVG(DaysUntilNextOrder) AVGDays,
       RANK() OVER (ORDER BY COALESCE(AVG(DaysUntilNextOrder), 999999)) RankAvg
FROM (
SELECT CustomerID,
       OrderDate,
       LEAD(OrderDate) OVER(PARTITION BY CustomerID Order BY OrderDate) NextOrder,
       DATEDIFF(day, OrderDate,LEAD(OrderDate) OVER(PARTITION BY CustomerID Order BY OrderDate)) AS DaysUntilNextOrder
FROM SalesDB.Sales.Orders)t
GROUP BY CustomerID

--Time Gap Analysis - Shipping Duration
SELECT OrderID,
       OrderDate,
       ShipDate,
       DATEDIFF(Day, OrderDate,ShipDate) AS BuyShipDifference
FROM SalesDB.Sales.Orders

SELECT MONTH(OrderDate) AS OrderDate,
       CASE
           WHEN MONTH(OrderDate)=1 THEN 'JAN'
           WHEN MONTH(OrderDate)=2 THEN 'FEB'
           WHEN MONTH(OrderDate)=3 THEN 'MAR'
        END AS OrderMonth_Long,
       AVG(DATEDIFF(Day, OrderDate,ShipDate)) AS AVGShip
FROM SalesDB.Sales.Orders
GROUP BY MONTH(OrderDate)

/*Number of Days between current Order and previous Order*/

SELECT OrderDate,
       LAG(OrderDate) OVER(ORDER BY OrderDate) AS Previous_Order,
       DATEDIFF(Day,LAG(OrderDate) OVER(ORDER BY OrderDate),OrderDate) AS DayDiffPreviousOrder
FROM SalesDB.Sales.Orders


/*First & Last*/
/*Find Highest and Lowest Sales for Each Product */
SELECT orderid,
       productid,
       sales,
       FIRST_VALUE(sales) OVER(PARTITION BY ProductID ORDER BY Sales) AS LowestSales,
       FIRST_VALUE(sales) OVER(PARTITION BY ProductID ORDER BY Sales DESC) AS HighestSales,
       LAST_VALUE(sales) OVER(PARTITION BY ProductID ORDER BY Sales
           ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) AS HighestSales_Last,
       Sales - FIRST_VALUE(sales) OVER(PARTITION BY ProductID ORDER BY Sales) AS SalesDifference
FROM SalesDB.Sales.Orders
ORDER BY productId, Sales
