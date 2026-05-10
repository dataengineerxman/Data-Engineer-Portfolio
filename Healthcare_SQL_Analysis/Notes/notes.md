+ Windows Functions
+ Allow you to the calculations (grouping) but do not lose level of details
WINDOW vs GROUP BY
+ Group BY returns a single row for each group
+ Windows function advanced analysis
+ Group By and Windows Functions support: 
  + COUNT
  + AVG
  + SUM
  + MAX
  + MIN
+ Windows Function only
  + Rank Functions
    + RANK
    + DENSE RANK
    + ROW_NUMBER
    + CUME_DIST
    + PERCENT_RANK
    + NTILE(n)
  + VALUE
    + LEAD
    + LAG
    + FIRST_VALUE

Example:
SELECT
    orderId,
    orderdate,
    productId,
    SUM(sales) OVER(Partition BY ProductID) AS Total
FROM SalesDB.Sales.Orders
    
Syntax of Windows Function
===================================================
Window Function Over(Partition | Order | Frame)
AVG(Sales) OVER (PARTITION BY Product ID ORDER BY Date ROWS UNBOUNDED PRECEDING)
Expression allows: 
Count - All Data Type
SUM, AVG, MAX, MIN - only numeric
RANK - Empty except NTILE - Numeric
VALUE - Any Data Type

PARTITION BY
+ Similar to Group BY
+ Divide into Groups (Windows Partitions)
+ OVER() Use the entire Data to do the calculations
+ Options
  + Empty
  + One Dimension (Field)
  + Multiple Columns, separated by a comma

ORDER BY is Required By RANK and VALUES

Frame
Subset of rows within each window that is relevant for the calculation
Window within a window
Rows Between Lower Values and Higher Value
Rows
    Current Row
    N Preceding - 2 row before current
    Unbounded Preceding
Unbounded
    Current Row
    N Following
    Unbounded Following - Last record in the window
You cannot use Frame Clause without Order By
Lower Values must be Before Higher Value
+ DEFAULT FRAME: 
  + ROW BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    + Row between first previous row and current row

+ Limitations
  + Can be used only in: 
    + SELECT
    + ORDER BY
      + We cannot use it to Filter Data or Group BY
  + You cannot nest Window Functions
  + Execute after Filtering the Data with WHERE Clause
  + Can be used with GROUP BY as long as you use the same Columns

+ WINDOWS RANKING FUNCTIONS
Sort Data first - Mandatory*/

Integer Based Ranking - Number
    ROW_NUMBER()
    RANK()
    DENSE_RANK()
    NTILE()
Percentage Based Ranking - 0 to 1 and in between 0.25
    CUME_DIST()
    PERCENT_RANK()

+ Expression Empty - except for NTILE() that takes a number
+ Partition Optional
+ Order - Mandatory
+ Frame - Not Allowed

CUME_DIST
+ Calculates the distribution of Data Points within a window
+ IF SQL finds to values with the same number ot takes the last row value

PERCENT_RANK
+ Calculates the relative position of each row within a window+ IF SQL finds to values with the same number ot takes the last row value
+ IF SQL finds to values with the same number ot takes the first row value 

CUME_DIST = Position Nr / Number of Rows
Percent_Rank = Position Nr -1 / Number of Rows -1

NTILE- Divides the rows into a specified number of approximately equal groups (Buckets)
Bucket Size = Number of Rows / Number of Buckets
Larger Groups come first
Use Cases
+ Data Segmentation
  + Divide the data set
+ Equalizing load processing

Value Windows Functions
+ Access a Value from another row
+ LAG to access Data from previous row
+ LEAD to access Data from the next row
+ First_Value to get Data from the first row
+ Last_Value to get Data from the last row

You must use ORDER BY
Frame 
LEAD AND LAG not Allowed
FIRST_VALUE optional
LAST_VALUE Should be used

OffSet- Number of rows forward or backward from current row. Default 1
Default Value - if next/previous is not available - Default = NULL



Subqueries 
+ Information Schema: Metadata
+ Temp Data Storage: Temporary space used for short-term tasks: processing queries, sorting data
+ Query inside the Main Query
  + Support the Main Query
  + Destroy SubQuery Results after the Main Query is done
+ External Query can not access the Subquery. 
  + Only main query can access.
  + Subqueries are used when you must operate on an intermediate result.
+ Query resolves and returns data and then Main query uses that info.
+ Subquery needs an alias


+ Subquery Types
+ ***************************************
  + Non Correlated
    + Independent from Main Query
  + Correlated
    + Depend on Main Query

  + Result Type
    + Scalar: One Single Value - Aggregations
    + Row: Multiple Rows with one Single Column
    + Table SubQuery: Multiple Rows & Multiple Columns

Location | Clauses
+ Subqueries can appear in several places in a SQL statement,
  + not only in the FROM clause. 
    + The most common locations are:
      SELECT
      FROM (derived tables)
      JOIN conditions
      WHERE
          Comparison
          Logical
           IN | Any | All | Exists
      HAVING
      INSERT / UPDATE / DELETE


+ Casting - Progress
length(master_patient_id::text) > 3  - Postgres Specific
+ OR
length(CAST(master_patient_id AS text)) > 3


IN FROM CLAUSE
    
SELECT se.*
FROM (
    SELECT *
    FROM surgical_encounters
    WHERE surgical_admission_date
        BETWEEN '2016-11-01'
        AND '2016-11-30' ) se
INNER JOIN (
    SELECT master_patient_id
    FROM patients
    WHERE date_of_birth >='1990-01-01'
) p ON se.master_patient_id=p.master_patient_id;

The query returns all columns from surgical_encounters for surgeries
that occurred in November 2016, but only if the surgery belongs
to a patient born on or after January 1, 1990.
The two filtered datasets are produced first in the 
FROM clause, then joined using master_patient_id,
and finally the columns from the surgical encounter dataset are returned.

IN SELECT
+ Used to aggregate data side by side with the main query's data allowing direct
comparison
+ Result must be SCALAR (One Value)

SUBQUERY IN WHERE
+ Multiple results

ANY
+Any values of the list.. at least one value
+ < ANY 
+ > All

Correlated and Non-Correlated Queries
+ Non-Correlated : Subquery can run independently from the main query
+ Correlated : Subquery depends on the main query


CTEs
+ Simpler number of steps than SubQuery
+ Bottom to top technique
+ Help eliminating redundancy
+ Improves readability
+ Introduces Modularity
  + Small queries and combine in final query
+ Reusability
+ Retrieves data from Cache and it is faster

+ Types
+ None-Recursive CTE - executed only once
  + Standalone CTE
  + Nested CTE
+ Recursive CTE

Standalone CTE
+ Defined and used independently in the query
+ Does not rely on other CTEs or queries
+ Main cannot be executed alone. Depends on the result of the CTE
+ You cannot sort (ORDER BY) in CTEs

+ Syntax
  + WITH CTE-NAME AS
    (
     SELECT... FROM ... WHERE
    )
SELECT .. FROM CTE-NAME WHERE

+ Multiple CTEs
  + Multipe CTEs independent from each other with one result
  + main query use them all 

+ Nested CTE
  + CTE inside another CTE
  + A CTE can use the results from another CTE
  + Main Query can use first and second CTE

Recursive CTE - executed multiple times until a condition is met
+ Anchor query is executed only once
+ UNION All to recursive query

Temp Tables
+ Temporary storage for intermediate results
+ Created and used within a session
+ Persist until the session ends or they are explicitly dropped
+ Useful for complex queries, breaking down tasks, and improving performance
+ Created using CREATE TEMPORARY TABLE or SELECT INTO
+ Can be indexed for faster access
+ Can be used in joins, subqueries, and other SQL operations
+ Temporary tables are session-specific and are automatically dropped when the session ends
+ Syntax:
+ CREATE TEMPORARY TABLE temp_table_name AS
SELECT ... INTO # FROM ...
WHERE ...;
+ What is a session?
  + A session is a connection between a user and the database. It can be established through various interfaces, such as command-line tools, applications, or web interfaces. Each session is independent and can have its own temporary tables, transactions, and settings. Temporary tables created in one session are not accessible in another session.



