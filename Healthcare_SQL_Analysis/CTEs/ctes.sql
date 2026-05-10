/*CTEs - StandAlone*/
WITH CTE_Total_Sales AS
         (SELECT CustomerID,
                 SUM(Sales) AS TotalSales
          FROM SalesDB.Sales.Orders
          GROUP BY CustomerID),
    CTE_Last_Order AS
        (
            SELECT CustomerID,
                MAX(OrderDate) LastOrderDate
            FROM SalesDB.Sales.Orders
            GROUP BY CustomerID
        ),
    CTE_Customer_Rank AS (
        SELECT CustomerID,
            RANK() OVER(ORDER BY TotalSales DESC) AS CustRank
        FROM CTE_Total_Sales
    ),
    CTE_Customer_Segment AS (
        SELECT CustomerID,
               CASE
                   WHEN TotalSales > 100 THEN 'High'
                   WHEN TotalSales > 80 THEN 'Medium'
               ELSE 'Low'
               END Customer_Segment
        FROM CTE_Total_Sales
        )
--MAIN QUERY
SELECT c.CustomerID,
       c.FirstName,
       c.LastName,
       cts.TotalSales,
       od.LastOrderDate,
       cr.CustRank,
       cs.Customer_Segment
FROM SalesDB.Sales.Customers c
    LEFT JOIN CTE_Total_Sales cts
    ON c.CustomerID=cts.CustomerID
    LEFT JOIN CTE_Last_Order od
    ON c.CustomerID=od.CustomerID
    LEFT JOIN CTE_Customer_Rank cr
    ON c.CustomerID=cr.CustomerID
    LEFT JOIN CTE_Customer_Segment cs
    ON c.CustomerID=cs.CustomerID
WHERE CustRank IS NOT NULL
ORDER BY CustRank

/*Recursive Query
  Sequence numbers from 1 to 20
 */
WITH series AS
    (SELECT 1
    AS MyNumber
    UNION ALL
    --Recursive
    SELECT  MyNumber+1
    FROM series
    WHERE MyNumber <20
)
SELECT *
FROM series
OPTION (MAXRECURSION 20)


/*Show Employee Hierarchy with each level in the organization*/

WITH CTE_EMP_Hierachy AS (
--ANCHOR
    SELECT EmployeeID,
           FirstName,
           ManagerID,
           1 AS Level
    FROM SalesDB.Sales.Employees
    WHERE ManagerID IS NULL
    UNION ALL
    SELECT e.EmployeeID,
        e.FirstName,
        e.ManagerID,
        Level + 1 AS Level
    FROM  SalesDB.Sales.Employees AS e
    INNER JOIN CTE_EMP_Hierachy ch
    ON e.ManagerID=ch.EmployeeID
    )

SELECT *
FROM CTE_EMP_Hierachy