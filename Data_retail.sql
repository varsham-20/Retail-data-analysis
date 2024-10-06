create database Retail_Data

use Retail_Data

Create table Data_Retail(
Product_ID varchar(max),	
Product_Name varchar(max),	
Category varchar(max),
Stock_Quantity varchar(max),
Supplier varchar(max),
Discount varchar(max),	
Rating varchar(max),
Reviews	varchar(max),
SKU	varchar(max),
Warehouse varchar(max),
Return_Policy varchar(max),
Brand varchar(max),
Supplier_Contact varchar(max),	
Placeholder	varchar(max),
Price varchar(max));

select column_name, data_type
from INFORMATION_SCHEMA.COLUMNS
where TABLE_NAME = 'Data_Retail'

bulk insert Data_Retail
from 'D:\Data_Retail_Analysis\Data_retail.csv'
with (fieldterminator=',',rowterminator='\n', firstrow=2, maxerrors=40)

select * from Data_Retail

-- To remove duplicates
with dup_rows as(select *, ROW_NUMBER() over (partition by Product_ID order by Product_ID) as Product
from Data_Retail)
select * from dup_rows
where Product > 1

-- There are no duplicate rows

Select * from Data_Retail

-- Step 1: Identify invalid Price values
SELECT Product_ID, Price
FROM Data_Retail
WHERE ISNUMERIC(Price) = 0;
--No invalid Prices

---------------------------------------------------------------------------------------------------------------------------------------

/* 1. Identifies products with prices higher than the average price within their category.*/

SELECT DISTINCT Category
FROM Data_Retail

/* 3 Types of category are there
1. Home
2. Electronics
3. Clothing */

SELECT Category, AVG(CAST(Price AS DECIMAL(10, 2))) AS Average_Price
FROM Data_Retail
GROUP BY Category
ORDER BY Average_Price;

/* Category wise average price of the products in ascending order  
Clothing	499.546151
Home	505.928896
Electronics	510.351806 */

SELECT Product_ID, Product_Name, Category, Price
FROM Data_Retail AS DR1
WHERE CAST(Price AS DECIMAL(10, 2)) > (
    SELECT AVG(CAST(Price AS DECIMAL(10, 2)))
    FROM Data_Retail AS DR2
    WHERE DR1.Category = DR2.Category
)
ORDER BY CAST(Price AS DECIMAL(10, 2)) DESC;

/*  1. clothing category product A has the highest price.
	2. There are more products with a price higher than the average price. 
	3. Compared to products in other categories, the clothing category has the highest prices.
*/
-------------------------------------------------------------------------------------------------------------------------------------

/* 2. Finding Categories with Highest Average Rating Across Products */

SELECT DISTINCT Product_Name, Category
FROM Data_Retail

/* 1. There are 3 products and 3 categories
   2. All 3 products belongs to all 3 categories */

--Below query is for category wise average rating
SELECT 
    Category, 
    AVG(CAST(Rating AS DECIMAL(10, 2))) AS Average_Rating
FROM 
    Data_Retail
GROUP BY 
    Category
ORDER BY 
    Average_Rating DESC;

--Below query is for products and category wise average rating

SELECT 
    DR.Product_Name, 
    DR.Category, 
    AVG(CAST(DR.Rating AS DECIMAL(10, 2))) AS Average_Rating
FROM 
    Data_Retail AS DR
GROUP BY 
    DR.Product_Name, 
    DR.Category
ORDER BY 
    Average_Rating DESC;

/* Home has an average rating of approximately 2.995.
Electronics has an average rating of approximately 2.977.
Clothing has an average rating of approximately 2.975.
Home is the category with the highest average rating across the products listed.
Product B is having the highest average rating.*/
-----------------------------------------------------------------------------------------------------------------------------------------

/* 3. Find the most reviewed product in each warehouse. */

WITH RankedProducts AS (
    SELECT 
        Warehouse,
        Product_ID,
		Category,
        Product_Name,
        Reviews,
        RANK() OVER (PARTITION BY Warehouse ORDER BY CAST(Reviews AS INT) DESC) AS ReviewRank
    FROM 
        Data_Retail
)
SELECT 
    Warehouse,
    Product_ID,
	Category,
    Product_Name,
    Reviews
FROM 
    RankedProducts
WHERE 
    ReviewRank = 1;

/* Warehouse A: Products A and C have the highest number of reviews in Warehouse A.
   Warehouse B: Product B and C is the most reviewed product in Warehouse B.
   Warehouse C: Product C is the most reviewed product in Warehouse C.
*/
-------------------------------------------------------------------------------------------------------------------------------------------
/* 4. find products that have higher-than-average prices within their category, along with their discount and supplier. */

WITH CategoryAveragePrices AS (
    SELECT 
        Category, 
        AVG(CAST(Price AS FLOAT)) AS AveragePrice
    FROM 
        Data_Retail
    GROUP BY 
        Category
)
SELECT 
    dr.Product_ID,
    dr.Product_Name,
    dr.Category,
    dr.Price,
    dr.Discount,
    dr.Supplier
FROM 
    Data_Retail dr
JOIN 
    CategoryAveragePrices cap ON dr.Category = cap.Category
WHERE 
    CAST(dr.Price AS FLOAT) > cap.AveragePrice
ORDER BY 
    CAST(dr.Price AS FLOAT) DESC;

/* Product A in the clothing category has a highest price from supplier X. 
Product B in the electronics category has a second highest price from supplier Z.
*/
-------------------------------------------------------------------------------------------------------------------------------------------

/* 5. Query to find the top 2 products with the highest average rating in each category. */

WITH RankedProducts AS (
    SELECT 
        Category,
        Product_ID,
        Product_Name,
        Rating,
        RANK() OVER (PARTITION BY Category ORDER BY CAST(Rating AS FLOAT) DESC) AS RatingRank
    FROM 
        Data_Retail
)
SELECT 
    Category,
    Product_ID,
    Product_Name,
    Rating
FROM 
    RankedProducts
WHERE 
    RatingRank <= 2
ORDER BY 
    Rating DESC;

/* 1. In the Home category, Product B has the highest average rating of 4.9987, followed by Product C with an average rating of 4.9980.
2.  In the Electronics category, Product C appears twice with different Product IDs, with the highest rating being 4.9984 and
the second highest rating being 4.9976.
3.  In the Clothing category, Product B has the highest average rating of 4.9978, followed by Product A with an average rating of 4.9957. */

-----------------------------------------------------------------------------------------------------------------------------------------

/* 6. Analysis Across All Return Policy Categories(Count, Avgstock, total stock, weighted_avg_rating, etc) */

SELECT 
    Return_Policy,
    COUNT(*) AS Product_Count,
    AVG(CAST(Stock_Quantity AS FLOAT)) AS Avg_Stock_Quantity,
    SUM(CAST(Stock_Quantity AS FLOAT)) AS Total_Stock_Quantity,
    SUM(CAST(Rating AS FLOAT) * CAST(Stock_Quantity AS FLOAT)) / SUM(CAST(Stock_Quantity AS FLOAT)) AS Weighted_Avg_Rating,
    AVG(CAST(Discount AS FLOAT)) AS Avg_Discount,
    SUM(CAST(Price AS FLOAT) * CAST(Stock_Quantity AS FLOAT) * (1 - CAST(Discount AS FLOAT) / 100)) AS Total_Revenue,
    SUM(CAST(Reviews AS INT)) AS Total_Reviews,
    AVG(CAST(Price AS FLOAT)) AS Avg_Price,
    COUNT(DISTINCT Product_ID) AS Total_Products,
    COUNT(DISTINCT Supplier) AS Total_Suppliers
FROM 
    Data_Retail
GROUP BY 
    Return_Policy
ORDER BY 
    Return_Policy ASC;

/* 
* All three return policies have a similar number of products, around 1600 to 1700. This suggests a fairly balanced distribution of products 
across the different return policies.
* The weighted average rating is very close across all return policies, approximately 2.99 to 2.996. This indicates that product ratings do not 
vary significantly with return policy.
* The average discount is slightly higher for the 7 Days return policy (25.89%) compared to the 30 Days (25.69%) and 15 Days (25.25%) policies.
* The 7 Days return policy generates the highest total revenue, followed closely by the 15 Days and 30 Days policies.
This could indicate that a shorter return policy might be associated with higher sales volumes.
1. The 7-day return policy has the highest number of products and the highest total stock quantity. The average stock quantity per product is also the highest among the three categories. 
The average rating is slightly below 3, similar to the 15-day policy. 
2. The 15-day return policy has a moderate number of products with an average stock quantity close to 50 units per product and a total stock quantity of over 80,000 units. 
The average rating is slightly below 3.
3. The 30-day return policy has a number of products and total stock quantity similar to the 15-day policy, with an average stock quantity per product around 49 units. 
The average rating is slightly below 3, similar to the other two policies.
*/
