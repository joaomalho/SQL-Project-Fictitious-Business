# F. Using MySQL, write the queries to retrieve the following information:
#1. List all the customer’s names, dates, and products or services used/booked/rented/bought by these customers in a range of two dates.
# 1.1 Total Sales per costumer per year, per country

#General considerations:

#Since the steps are in the range of the dozens of milliseconds, the queries are considered optimized.
#Some ways to optimize a queries like this is to make sure only the necessary columns are grouping are used, as well indexed.
#When it comes to subselects, when possible it is preferential to substitute it into a join, although in these cases the subselect was necessary.

-- EXPLAIN ANALYZE
SELECT customer.CustomerName AS CustomerName  
	, YEAR(orders.OrderDate) AS OrdersYear
	, Sum(orders.Quantity) AS NumberOfProducts
    , count(orders.orderID) AS NumberOfOrders 
    , companystore.Address AS StoreAddress
    , country.CountryName AS StoreCountry
    , sum(product.productprice) AS TotalSalesAmount
	, companystore.VAT AS VAT
    , cast(sum(product.productprice)*companystore.VAT AS DECIMAL(11,2))+sum(product.productprice) AS TotalSalesAmountWithVAT
FROM orders
LEFT JOIN customer
	ON orders.customerID = customer.ID
LEFT JOIN companystore
	ON companystore.ID = orders.StoreID 
LEFT JOIN country 
	ON companystore.CountryID = country.ID
LEFT JOIN product
	ON product.productID = Orders.productID
GROUP BY YEAR(orders.OrderDate)
	, customer.CustomerName
    , companystore.Address
	, companystore.CountryID 
    , country.CountryName
    , companystore.VAT
ORDER BY orders.customerID
	, YEAR(orders.OrderDate);
# The execution plan follows the format:
# Step performed, the cost (if it applied), the time taken, the rows affected, the loops performed
# -> Sort the results by orders.CustomerID and OrdersYear (took 51.224 to 51.325 ms, 600 rows affected, 1 loop)
# -> Scan the temporary table (took 50.651 to 50.814 ms, 600 rows affected, 1 loop)
# -> Aggregate the results using a temporary table (took 50.648 ms, 600 rows affected, 1 loop)
# -> Perform a nested loop left join (estimated cost of 6903.56, took 0.281 to 30.967 ms, 5997 rows affected, 1 loop)
# -> Perform a nested loop left join (estimated cost of 3605.21, took 0.268 to 22.753 ms, 5997 rows affected, 1 loop)
# -> Perform a left hash join between companystore.ID and orders.StoreID (estimated cost of 1805.36, took 0.240 to 16.366 ms, 5997 rows affected, 1 loop)
# -> Perform a nested loop left join (estimated cost of 2703.65, took 0.129 to 14.169 ms, 5997 rows affected, 1 loop)
# -> Scan the orders table (estimated cost of 604.70, took 0.070 to 3.214 ms, 5997 rows affected)
# -> Perform a single-row index lookup on customer using the PRIMARY key (cost of 0.25, took 0.001 to 0.002 ms, 1 loop)
# -> Perform a hash join (no cost estimate given)
# -> Scan the companystore table (cost of 0.00, took 0.067 to 0.076 ms, 3 rows affected, 1 loop)
# -> Perform a single-row index lookup on country using the PRIMARY key (cost of 0.00, took 0.001 to 0.001 ms, 1 loop)
# -> Perform a single-row index lookup on product using the PRIMARY key (cost of 0.08, took 0.001 to 0.001 ms, 1 loop)

#2. List the best three customers /products /services /places (you are free to define the criteria for what means “best”)
# 2.1 Best as more sales orders disregarding value, per costumer
EXPLAIN ANALYZE
SELECT BestOrderCostumerRank.*
FROM (	
		SELECT ROW_NUMBER() OVER (ORDER BY COUNT(orders.orderID) DESC) AS BestOrderCustomer # Return a rank per 
			, FORMAT(COUNT(orders.orderID),0) AS NumberOfOrders 
			, FORMAT(SUM(orders.Quantity),0) AS NumberOfProducts
			, customer.CustomerName AS CustomerName
			, country.CountryName AS StoreCountry
			, CONCAT('€', FORMAT(SUM(product.productprice), 2)) AS TotalSalesAmount
			, companystore.VAT AS VAT
			, CONCAT('€', FORMAT(SUM(product.productprice)*companystore.VAT, 2)) AS TotalVAT
			, CONCAT('€', FORMAT(SUM(product.productprice)*(1+companystore.VAT), 2)) AS TotalSalesAmountWithVAT
		FROM orders
		LEFT JOIN customer
			ON orders.customerID = customer.ID
		LEFT JOIN companystore
			ON companystore.ID = orders.StoreID 
		LEFT JOIN country 
			ON companystore.CountryID = country.ID
		LEFT JOIN product
			ON product.productID = Orders.productID
		GROUP BY customer.CustomerName
			, country.CountryName 
			, companystore.VAT
		ORDER BY BestOrderCustomer
			, customer.customerName
			, country.CountryName 
			, companystore.VAT
	) AS BestOrderCostumerRank
WHERE BestOrderCostumerRank.BestOrderCustomer <= 3
;
# Step performed, the cost (if it applied), the time taken, the rows affected, the loops performed
# -> Filter: (bestordercostumerrank.BestOrderCustomer <= 3)  (cost=0.34..2026.49 rows=5996) (actual time=89.405..89.463 rows=3 loops=1) 
# This step filters sub table by BestOrderCustomer less or equal 3. It took 89 miliseconds seconds to complete and processed 5996 rows in a single loop and result in 3 rows
# -> Table scan on BestOrderCostumerRank  (cost=2.50..2.50 rows=0) (actual time=89.402..89.447 rows=150 loops=1)
# This step sacns sub table BestOrderCostumerRank. It took 89 miliseconds to complete and result in 150 rows in a single loop and costs 2,5 
# -> Materialize  (cost=0.00..0.00 rows=0) (actual time=89.401..89.401 rows=150 loops=1)
# This step stored table without cost
# -> Sort: BestOrderCustomer, customer.CustomerName, country.CountryName, companystore.VAT  (actual time=88.864..88.910 rows=150 loops=1)
# This step is the sort by BestOrderCustomer, customer.CustomerName, country.CountryName, companystore.VAT 
# -> Table scan on <temporary>  (cost=2.50..2.50 rows=0) (actual time=88.566..88.636 rows=150 loops=1)
# This step scan table 
# -> Temporary table  (cost=0.00..0.00 rows=0) (actual time=88.564..88.564 rows=150 loops=1)
# This step store the temporary table
# -> Window aggregate: row_number() OVER (ORDER BY count(orders.OrderID) desc )   (actual time=88.312..88.453 rows=150 loops=1)
# This step aggregate result in the rows aggregation by function row_number over counts of orderID per row descendent
# -> Sort: count(orders.OrderID) DESC  (actual time=88.303..88.344 rows=150 loops=1)
# This step execute descendent sort of Counts of orderID
# -> Table scan on <temporary>  (actual time=88.084..88.147 rows=150 loops=1)
# This step scane table
# -> Aggregate using temporary table  (actual time=88.080..88.080 rows=150 loops=1)
# This step aggregate temporaty table
# Following steps are Executions of left joins  
# -> Nested loop left join  (cost=6903.54 rows=17991) (actual time=0.169..60.311 rows=5997 loops=1)
# -> Nested loop left join  (cost=3605.19 rows=17991) (actual time=0.162..44.148 rows=5997 loops=1)
# -> Left hash join (companystore.ID = orders.StoreID)  (cost=1805.34 rows=17991) (actual time=0.151..31.540 rows=5997 loops=1)
# -> Nested loop left join  (cost=2703.65 rows=5997) (actual time=0.081..27.444 rows=5997 loops=1)
# -> Table scan on orders  (cost=604.70 rows=5997) (actual time=0.043..6.082 rows=5997 loops=1)
# Scan table orders
# -> Single-row index lookup on customer using PRIMARY (ID=orders.CustomerID)  (cost=0.25 rows=1) (actual time=0.003..0.003 rows=1 loops=5997)
# Define the primary key as customer ID 
# -> Table scan on companystore  (cost=0.00 rows=3) (actual time=0.040..0.047 rows=3 loops=1)
# This step scans table companystore 
# -> Single-row index lookup on country using PRIMARY (ID=companystore.CountryID)  (cost=0.00 rows=1) (actual time=0.002..0.002 rows=1 loops=5997)
# This step set foreign key countryID
# -> Single-row index lookup on product using PRIMARY (ProductID=orders.ProductID)  (cost=0.08 rows=1) (actual time=0.002..0.002 rows=1 loops=5997)
# This step set foreign key productID

# OPTIMIZATION
# create a view for costumer profile rate, this view would regard all metric per costumer since sales number, amount, nº of orders, and by timeline. 
# This would be more accurate and performing in terms of resource costs

# 2.2 Best as more sales Products disregarding value, per costumer
EXPLAIN ANALYZE
SELECT BestProductCostumerRank.*
FROM (
		SELECT row_number() OVER (ORDER BY count(orders.Quantity) DESC) AS BestProductCustomer
			, FORMAT(COUNT(orders.orderID),0) AS NumberOfOrders 
			, FORMAT(SUM(orders.Quantity),0) AS NumberOfProducts
			, customer.CustomerName AS CustomerName
			, country.CountryName AS StoreCountry
			, CONCAT('€', FORMAT(SUM(product.productprice), 2)) AS TotalSalesAmount
			, companystore.VAT AS VAT
			, CONCAT('€', FORMAT(SUM(product.productprice)*companystore.VAT, 2)) AS TotalVAT
			, CONCAT('€', FORMAT(SUM(product.productprice)*(1+companystore.VAT), 2)) AS TotalSalesAmountWithVAT
		FROM orders
		LEFT JOIN customer
			ON orders.customerID = customer.ID
		LEFT JOIN companystore
			ON companystore.ID = orders.StoreID 
		LEFT JOIN country 
			ON companystore.CountryID = country.ID
		LEFT JOIN product
			ON product.productID = Orders.productID
		GROUP BY customer.CustomerName
			, country.CountryName 
			, companystore.VAT
		ORDER BY BestProductCustomer
			, customer.CustomerName
			, country.CountryName 
			, companystore.VAT
	) AS BestProductCostumerRank
WHERE BestProductCostumerRank.BestProductCustomer <= 3
;
#-> Filter: (bestproductcostumerrank.BestProductCustomer <= 3)  (cost=0.34..2026.49 rows=5996) (actual time=45.244..45.286 rows=3 loops=1)    
#-> Table scan on BestProductCostumerRank  (cost=2.50..2.50 rows=0) (actual time=45.241..45.275 rows=150 loops=1)        
#-> Materialize  (cost=0.00..0.00 rows=0) (actual time=45.240..45.240 rows=150 loops=1)            
#-> Sort: BestProductCustomer, customer.CustomerName, country.CountryName, companystore.VAT  (actual time=44.887..44.919 rows=150 loops=1)                
#-> Table scan on <temporary>  (cost=2.50..2.50 rows=0) (actual time=44.759..44.793 rows=150 loops=1)                    
#-> Temporary table  (cost=0.00..0.00 rows=0) (actual time=44.758..44.758 rows=150 loops=1)                        
#-> Window aggregate: row_number() OVER (ORDER BY count(orders.Quantity) desc )   (actual time=44.604..44.701 rows=150 loops=1)                            
#-> Sort: count(orders.Quantity) DESC  (actual time=44.599..44.629 rows=150 loops=1)                                
#-> Table scan on <temporary>  (actual time=44.480..44.513 rows=150 loops=1)                                    
#-> Aggregate using temporary table  (actual time=44.478..44.478 rows=150 loops=1)                                        
#-> Nested loop left join  (cost=6903.54 rows=17991) (actual time=0.133..30.293 rows=5997 loops=1)                                            
#-> Nested loop left join  (cost=3605.19 rows=17991) (actual time=0.127..22.188 rows=5997 loops=1)                                                
#-> Left hash join (companystore.ID = orders.StoreID)  (cost=1805.34 rows=17991) (actual time=0.121..15.341 rows=5997 loops=1)                                                    
#-> Nested loop left join  (cost=2703.65 rows=5997) (actual time=0.050..13.517 rows=5997 loops=1)                                                        
#-> Table scan on orders  (cost=604.70 rows=5997) (actual time=0.033..2.917 rows=5997 loops=1)                                                        
#-> Single-row index lookup on customer using PRIMARY (ID=orders.CustomerID)  (cost=0.25 rows=1) (actual time=0.001..0.002 rows=1 loops=5997)                                                    
#-> Hash                                                        
#-> Table scan on companystore  (cost=0.00 rows=3) (actual time=0.040..0.046 rows=3 loops=1)                                                
#-> Single-row index lookup on country using PRIMARY (ID=companystore.CountryID)  (cost=0.00 rows=1) (actual time=0.001..0.001 rows=1 loops=5997)                                            
#-> Single-row index lookup on product using PRIMARY (ProductID=orders.ProductID)  (cost=0.08 rows=1) (actual time=0.001..0.001 rows=1 loops=5997)'

# 2.3 Best as more sales Sales Amount, per costumer
-- EXPLAIN ANALYZE
SELECT BestBuyerAmountCostumerRank.*
FROM (
		SELECT row_number() OVER (ORDER BY sum(product.productprice) DESC) AS BestBuyerAmountCustomer
			, FORMAT(COUNT(orders.orderID),0) AS NumberOfOrders 
			, FORMAT(SUM(orders.Quantity),0) AS NumberOfProducts
			, customer.CustomerName AS CustomerName
			, country.CountryName AS StoreCountry
			, CONCAT('€', FORMAT(SUM(product.productprice), 2)) AS TotalPurchaseAmount
			, companystore.VAT AS VAT
			, CONCAT('€', FORMAT(SUM(product.productprice)*companystore.VAT, 2)) AS TotalVAT
			, CONCAT('€', FORMAT(SUM(product.productprice)*(1+companystore.VAT), 2)) AS TotalSalesAmountWithVAT
		FROM orders
		LEFT JOIN customer
			ON orders.customerID = customer.ID
		LEFT JOIN companystore
			ON companystore.ID = orders.StoreID 
		LEFT JOIN country 
			ON companystore.CountryID = country.ID
		LEFT JOIN product
			ON product.productID = Orders.productID
		GROUP BY customer.CustomerName
			, country.CountryName 
			, companystore.VAT
		ORDER BY BestBuyerAmountCustomer
			, customer.CustomerName
			, country.CountryName 
			, companystore.VAT
	) AS BestBuyerAmountCostumerRank
WHERE BestBuyerAmountCostumerRank.BestBuyerAmountCustomer <= 3
;
#-> Filter: (bestbuyeramountcostumerrank.BestBuyerAmountCustomer <= 3)  (cost=0.34..2026.49 rows=5996) (actual time=70.509..70.569 rows=3 loops=1)    
#-> Table scan on BestBuyerAmountCostumerRank  (cost=2.50..2.50 rows=0) (actual time=70.506..70.552 rows=150 loops=1)        
#-> Materialize  (cost=0.00..0.00 rows=0) (actual time=70.504..70.504 rows=150 loops=1)            
#-> Sort: BestBuyerAmountCustomer, customer.CustomerName, country.CountryName, companystore.VAT  (actual time=69.909..69.966 rows=150 loops=1)                
#-> Table scan on <temporary>  (cost=2.50..2.50 rows=0) (actual time=69.700..69.750 rows=150 loops=1)                    
#-> Temporary table  (cost=0.00..0.00 rows=0) (actual time=69.698..69.698 rows=150 loops=1)                        
#-> Window aggregate: row_number() OVER (ORDER BY sum(product.ProductPrice) desc )   (actual time=69.425..69.601 rows=150 loops=1)                            
#-> Sort: sum(product.ProductPrice) DESC  (actual time=69.418..69.477 rows=150 loops=1)                                
#-> Table scan on <temporary>  (actual time=69.203..69.261 rows=150 loops=1)                                    
#-> Aggregate using temporary table  (actual time=69.200..69.200 rows=150 loops=1)                                        
#-> Nested loop left join  (cost=6903.54 rows=17991) (actual time=0.147..45.477 rows=5997 loops=1)                                            
#-> Nested loop left join  (cost=3605.19 rows=17991) (actual time=0.143..33.264 rows=5997 loops=1)                                                
#-> Left hash join (companystore.ID = orders.StoreID)  (cost=1805.34 rows=17991) (actual time=0.136..23.639 rows=5997 loops=1)                                                    
#-> Nested loop left join  (cost=2703.65 rows=5997) (actual time=0.068..20.942 rows=5997 loops=1)                                                        
#-> Table scan on orders  (cost=604.70 rows=5997) (actual time=0.050..4.803 rows=5997 loops=1)                                                        
#-> Single-row index lookup on customer using PRIMARY (ID=orders.CustomerID)  (cost=0.25 rows=1) (actual time=0.002..0.002 rows=1 loops=5997)                                                    
#-> Hash                                                        
#-> Table scan on companystore  (cost=0.00 rows=3) (actual time=0.039..0.045 rows=3 loops=1)                                                
#-> Single-row index lookup on country using PRIMARY (ID=companystore.CountryID)  (cost=0.00 rows=1) (actual time=0.001..0.001 rows=1 loops=5997)                                            
#-> Single-row index lookup on product using PRIMARY (ProductID=orders.ProductID)  (cost=0.08 rows=1) (actual time=0.002..0.002 rows=1 loops=5997)'

# 2.4 Best as more sales Sales Amount, per product
-- EXPLAIN ANALYZE
SELECT BestSellerAmountProductRank.*
FROM (
		SELECT row_number() OVER (ORDER BY sum(product.productprice) DESC) AS BestSellerAmountProduct
			, product.productname AS ProductName
            , FORMAT(COUNT(orders.orderID),0) AS NumberOfOrders 
			, FORMAT(SUM(orders.Quantity),0) AS NumberOfProducts
			, country.CountryName AS StoreCountry
			, CONCAT('€', FORMAT(SUM(product.productprice), 2)) AS TotalSalesAmount
			, companystore.VAT AS VAT
			, CONCAT('€', FORMAT(SUM(product.productprice)*companystore.VAT, 2)) AS TotalVAT
			, CONCAT('€', FORMAT(SUM(product.productprice)*(1+companystore.VAT), 2)) AS TotalSalesAmountWithVAT
		FROM orders
        LEFT JOIN product
			ON product.productID = Orders.productID
		LEFT JOIN companystore
			ON companystore.ID = orders.StoreID 
		LEFT JOIN country 
			ON companystore.CountryID = country.ID
		GROUP BY product.productname
			, companystore.VAT
            , country.CountryName
		ORDER BY BestSellerAmountProduct
			, product.productname
			, country.CountryName 
			, companystore.VAT
	) AS BestSellerAmountProductRank
WHERE BestSellerAmountProductRank.BestSellerAmountProduct <= 3
;
#-> Filter: (bestselleramountproductrank.BestSellerAmountProduct <= 3)  (cost=0.34..2026.49 rows=5996) (actual time=37.127..37.204 rows=3 loops=1)    
#-> Table scan on BestSellerAmountProductRank  (cost=2.50..2.50 rows=0) (actual time=37.125..37.185 rows=273 loops=1)        
#-> Materialize  (cost=0.00..0.00 rows=0) (actual time=37.124..37.124 rows=273 loops=1)            
#-> Sort: BestSellerAmountProduct, product.ProductName, country.CountryName, companystore.VAT  (actual time=36.305..36.395 rows=273 loops=1)                
#-> Table scan on <temporary>  (cost=2.50..2.50 rows=0) (actual time=36.064..36.124 rows=273 loops=1)                    
#-> Temporary table  (cost=0.00..0.00 rows=0) (actual time=36.063..36.063 rows=273 loops=1)                        
#-> Window aggregate: row_number() OVER (ORDER BY sum(product.ProductPrice) desc )   (actual time=35.757..35.969 rows=273 loops=1)                            
#-> Sort: sum(product.ProductPrice) DESC  (actual time=35.752..35.826 rows=273 loops=1)                                
#-> Table scan on <temporary>  (actual time=35.409..35.483 rows=273 loops=1)                                    
#-> Aggregate using temporary table  (actual time=35.407..35.407 rows=273 loops=1)                                        
#-> Nested loop left join  (cost=3601.65 rows=17991) (actual time=0.102..19.880 rows=5997 loops=1)                                            
#-> Left hash join (companystore.ID = orders.StoreID)  (cost=1801.80 rows=17991) (actual time=0.096..13.560 rows=5997 loops=1)                                                
#-> Nested loop left join  (cost=2703.65 rows=5997) (actual time=0.052..11.723 rows=5997 loops=1)                                                    
#-> Table scan on orders  (cost=604.70 rows=5997) (actual time=0.039..2.793 rows=5997 loops=1)                                                    
#-> Single-row index lookup on product using PRIMARY (ProductID=orders.ProductID)  (cost=0.25 rows=1) (actual time=0.001..0.001 rows=1 loops=5997)                                                
#-> Hash                                                    
#-> Table scan on companystore  (cost=0.00 rows=3) (actual time=0.025..0.030 rows=3 loops=1)                                            
#-> Single-row index lookup on country using PRIMARY (ID=companystore.CountryID)  (cost=0.00 rows=1) (actual time=0.001..0.001 rows=1 loops=5997)'

# 2.5 Best as more orders amount, per product
-- EXPLAIN ANALYZE 
SELECT BestSellerQuantityProductRank.*
FROM (
		SELECT row_number() OVER (ORDER BY sum(orders.quantity) DESC) AS BestSellerQuantityProduct
			, product.productname AS ProductName
            , FORMAT(COUNT(orders.orderID),0) AS NumberOfOrders 
			, FORMAT(SUM(orders.Quantity),0) AS NumberOfProducts
			, CONCAT('€', FORMAT(SUM(product.productprice), 2)) AS TotalSalesAmount
			, CONCAT('€', FORMAT(SUM(product.productprice)*companystore.VAT, 2)) AS TotalVAT
			, CONCAT('€', FORMAT(SUM(product.productprice)*(1+companystore.VAT), 2)) AS TotalSalesAmountWithVAT
		FROM orders
		LEFT JOIN companystore
			ON companystore.ID = orders.StoreID 
		LEFT JOIN country 
			ON companystore.CountryID = country.ID
		LEFT JOIN product
			ON product.productID = Orders.productID
		GROUP BY product.productname
			, companystore.VAT
	) AS BestSellerQuantityProductRank
WHERE BestSellerQuantityProductRank.BestSellerQuantityProduct <= 3
;

# 2.6 Best as more orders amount, per product, per year
-- EXPLAIN ANALYZE
SELECT row_number() OVER (PARTITION BY YEAR(orders.orderdate) ORDER BY sum(orders.quantity) DESC,(COUNT(orders.orderID)) DESC,SUM(product.productprice) DESC) AS BestSellerQuantityProduct 
	, productsubcategoryid.productsubcategory AS ProductCategory
	, FORMAT(COUNT(orders.orderID),0) AS NumberOfOrders 
	, FORMAT(SUM(orders.Quantity),0) AS NumberOfProducts
	, CONCAT('€', FORMAT(SUM(product.productprice), 2)) AS TotalSalesAmount
	, CONCAT('€', FORMAT(SUM(product.productprice)*companystore.VAT, 2)) AS TotalVAT
	, CONCAT('€', FORMAT(SUM(product.productprice)*(1+companystore.VAT), 2)) AS TotalSalesAmountWithVAT
	, YEAR(orders.orderdate) AS YEARs
FROM orders
LEFT JOIN companystore
	ON companystore.ID = orders.StoreID 
LEFT JOIN country 
	ON companystore.CountryID = country.ID
LEFT JOIN product
	ON product.productID = Orders.productID
LEFT JOIN productsubcategoryid
	ON productsubcategoryid.id = product.productsubcategoryID
GROUP BY productsubcategoryid.productsubcategory
		, YEAR(orders.orderdate)
        , companystore.VAT
ORDER BY YEAR(orders.orderdate)
;
#-> Sort: YEARs  (actual time=39.534..39.551 rows=72 loops=1)    
#-> Table scan on <temporary>  (cost=2.50..2.50 rows=0) (actual time=39.489..39.505 rows=72 loops=1)        
#-> Temporary table  (cost=0.00..0.00 rows=0) (actual time=39.488..39.488 rows=72 loops=1)            
#-> Window aggregate: row_number() OVER (PARTITION BY year(orders.OrderDate) ORDER BY sum(orders.Quantity) desc,count(orders.OrderID) desc,sum(product.ProductPrice) desc )   (actual time=39.348..39.397 rows=72 loops=1)                
#-> Sort: YEARs, sum(orders.Quantity) DESC, count(orders.OrderID) DESC, sum(product.ProductPrice) DESC  (actual time=39.327..39.346 rows=72 loops=1)                    
#-> Table scan on <temporary>  (actual time=39.233..39.250 rows=72 loops=1)                        
#-> Aggregate using temporary table  (actual time=39.232..39.232 rows=72 loops=1)                            
#-> Nested loop left join  (cost=10196.01 rows=17991) (actual time=0.194..25.359 rows=5997 loops=1)                                
#-> Nested loop left join  (cost=6897.66 rows=17991) (actual time=0.085..19.130 rows=5997 loops=1)                                    
#-> Nested loop left join  (cost=3599.31 rows=17991) (actual time=0.081..11.130 rows=5997 loops=1)                                        
#-> Left hash join (companystore.ID = orders.StoreID)  (cost=1799.46 rows=17991) (actual time=0.071..4.612 rows=5997 loops=1)                                            
#-> Table scan on orders  (cost=604.70 rows=5997) (actual time=0.029..2.872 rows=5997 loops=1)                                            
#-> Hash                                                
#-> Table scan on companystore  (cost=0.00 rows=3) (actual time=0.023..0.028 rows=3 loops=1)                                        
#-> Single-row covering index lookup on country using PRIMARY (ID=companystore.CountryID)  (cost=0.00 rows=1) (actual time=0.001..0.001 rows=1 loops=5997)                                    
#-> Single-row index lookup on product using PRIMARY (ProductID=orders.ProductID)  (cost=0.08 rows=1) (actual time=0.001..0.001 rows=1 loops=5997)                                
#-> Single-row index lookup on productsubcategoryid using PRIMARY (ID=product.ProductSubCategoryID)  (cost=0.08 rows=1) (actual time=0.001..0.001 rows=1 loops=5997)'

#3. Get the average amount of sales/bookings/rents/deliveries for a period that involves 2 or more years, as in the following example. This query only returns one record:
# __________________________________________________________________________________________________________________
#|PeriodOfSales    | TotalSales (euros) | YearlyAverage (of the given period) | MonthlyAverage (of the given period)|
#|01/2010 – 10/2021| XXXXX € 			| XXXXX € 							  | XXXX €								|
#|__________________________________________________________________________________________________________________|
DELIMITER $$
SET @FirstYear = (SELECT MIN(YEAR(orders.OrderDate)) FROM orders) ;
SET @SecondYear = (SELECT (MIN(YEAR(orders.OrderDate))+1) FROM orders);
SET @ThirdYear = (SELECT (MIN(YEAR(orders.OrderDate))+2) FROM orders);

EXPLAIN ANALYZE
SELECT 'Full Business Period' AS PeriodDescription
	, CONCAT(MIN(YEAR(orders.OrderDate)),' - ',MAX(YEAR(orders.OrderDate))) AS PeriodBucket 
	, CONCAT('€', FORMAT(SUM(product.productprice), 2)) AS TotalSales
	, CONCAT('€', FORMAT(SUM(product.productprice) / COUNT( DISTINCT YEAR(orders.OrderDate)), 2)) AS YearlyAVG
    , CONCAT('€', FORMAT(SUM(product.productprice) / COUNT( DISTINCT YEAR(orders.OrderDate)) / COUNT( DISTINCT MONTH(orders.OrderDate)), 2)) AS MonthlyAVG
FROM orders
LEFT JOIN product
	ON orders.productID = Orders.productID
UNION 
SELECT 'First Period' AS PeriodDescription
	, CONCAT(@FirstYear,' - ',@SecondYear) AS PeriodBucket 
	, CONCAT('€', FORMAT(SUM(product.productprice), 2)) AS TotalSales
	, CONCAT('€', FORMAT(SUM(product.productprice) / COUNT( DISTINCT YEAR(orders.OrderDate)), 2)) AS YearlyAVG
    , CONCAT('€', FORMAT(SUM(product.productprice) / COUNT( DISTINCT YEAR(orders.OrderDate)) / COUNT( DISTINCT MONTH(orders.OrderDate)), 2)) AS MonthlyAVG
FROM orders
LEFT JOIN product
	ON orders.productID = Orders.productID
WHERE YEAR(OrderDate) BETWEEN @FirstYear AND @SecondYear 
UNION 
SELECT 'Second Period' AS PeriodDescription
	, CONCAT(@SecondYear,' - ',@ThirdYear) AS PeriodBucket 
	, CONCAT('€', FORMAT(SUM(product.productprice), 2)) AS TotalSales
	, CONCAT('€', FORMAT(SUM(product.productprice) / COUNT( DISTINCT YEAR(orders.OrderDate)), 2)) AS YearlyAVG
    , CONCAT('€', FORMAT(SUM(product.productprice) / COUNT( DISTINCT YEAR(orders.OrderDate)) / COUNT( DISTINCT MONTH(orders.OrderDate)), 2)) AS MonthlyAVG
FROM orders
LEFT JOIN product
	ON orders.productID = Orders.productID
WHERE YEAR(OrderDate) BETWEEN @SecondYear AND @ThirdYear 
UNION 
SELECT '1 Quarter of 1 Year Period' AS PeriodDescription
	, CONCAT('2019/01/01',' - ','2022/03/31') AS PeriodBucket 
	, CONCAT('€', FORMAT(SUM(product.productprice), 2)) AS TotalSales
	, CONCAT('€', FORMAT(SUM(product.productprice) / COUNT( DISTINCT YEAR(orders.OrderDate)), 2)) AS YearlyAVG
    , CONCAT('€', FORMAT(SUM(product.productprice) / COUNT( DISTINCT YEAR(orders.OrderDate)) / COUNT( DISTINCT MONTH(orders.OrderDate)), 2)) AS MonthlyAVG
FROM orders
LEFT JOIN product
	ON orders.productID = Orders.productID
WHERE OrderDate BETWEEN '20190101' AND '20220331'
$$
DELIMITER ;
# The execution plan follows the format:
# Step performed, the cost (if it applied), the time taken, the rows affected, the loops performed
#-> Table scan on <union temporary>  (cost=365684.61..365686.52 rows=4) (actual time=1393.185..1393.187 rows=4 loops=1)
#-> Union materialize with deduplication  (cost=365683.97..365683.97 rows=4) (actual time=1393.182..1393.182 rows=4 loops=1)
# this step excute the union funtion
#-> Aggregate: count(distinct month(orders.OrderDate)), count(distinct year(orders.OrderDate)), sum(product.ProductPrice), count(distinct year(orders.OrderDate)), sum(product.ProductPrice), sum(product.ProductPrice), max(year(orders.OrderDate)), min(year(orders.OrderDate))  (cost=117541.49 rows=1) (actual time=504.101..504.102 rows=1 loops=1)
#-> Left hash join (no condition)  (cost=58770.89 rows=587706) (actual time=0.178..66.490 rows=587706 loops=1)
#-> Table scan on orders  (cost=604.70 rows=5997) (actual time=0.050..3.905 rows=5997 loops=1)
#-> Table scan on product  (cost=0.00 rows=98) (actual time=0.061..0.092 rows=98 loops=1)
#-> Aggregate: count(distinct month(orders.OrderDate)), count(distinct year(orders.OrderDate)), sum(product.ProductPrice), count(distinct year(orders.OrderDate)), sum(product.ProductPrice), sum(product.ProductPrice)  (cost=117541.49 rows=1) (actual time=228.332..228.332 rows=1 loops=1)
#-> Left hash join (no condition)  (cost=58770.89 rows=587706) (actual time=0.155..36.627 rows=297234 loops=1)
#-> Filter: (year(orders.OrderDate) between <cache>((@FirstYear)) and <cache>((@SecondYear)))  (cost=604.70 rows=5997) (actual time=0.041..4.264 rows=3033 loops=1)
#-> Table scan on orders  (cost=604.70 rows=5997) (actual time=0.031..3.201 rows=5997 loops=1)
#-> Table scan on product  (cost=0.00 rows=98) (actual time=0.047..0.065 rows=98 loops=1)
#-> Aggregate: count(distinct month(orders.OrderDate)), count(distinct year(orders.OrderDate)), sum(product.ProductPrice), count(distinct year(orders.OrderDate)), sum(product.ProductPrice), sum(product.ProductPrice)  (cost=117541.49 rows=1) (actual time=235.485..235.485 rows=1 loops=1)
#-> Left hash join (no condition)  (cost=58770.89 rows=587706) (actual time=0.149..38.052 rows=301350 loops=1)
#-> Filter: (year(orders.OrderDate) between <cache>((@SecondYear)) and <cache>((@ThirdYear)))  (cost=604.70 rows=5997) (actual time=0.037..4.874 rows=3075 loops=1)
#-> Table scan on orders  (cost=604.70 rows=5997) (actual time=0.031..3.711 rows=5997 loops=1)
#-> Table scan on product  (cost=0.00 rows=98) (actual time=0.041..0.068 rows=98 loops=1)
#-> Aggregate: count(distinct month(orders.OrderDate)), count(distinct year(orders.OrderDate)), sum(product.ProductPrice), count(distinct year(orders.OrderDate)), sum(product.ProductPrice), sum(product.ProductPrice)  (cost=13059.10 rows=1) (actual time=425.052..425.053 rows=1 loops=1)
#-> Left hash join (no condition)  (cost=6529.68 rows=65294) (actual time=0.201..70.294 rows=487256 loops=1)
#-> Filter: (orders.OrderDate between \'20190101\' and \'20220331\')  (cost=604.70 rows=666) (actual time=0.040..11.291 rows=4972 loops=1)
#-> Table scan on orders  (cost=604.70 rows=5997) (actual time=0.031..5.246 rows=5997 loops=1)
#-> Table scan on product  (cost=0.02 rows=98) (actual time=0.084..0.104 rows=98 loops=1)

# OPTIMIZATION
# this is somethign that could be perfectly vizualized via PowerBI without queries creation because is not perfoming create views nether tables for this type of queires which are almost ad hoc questions
# , with powerbI reporting users can easely filter the necessary time lines that they need

#4. Get the total sales/bookings/rents/deliveries by geographical location (city/country).
-- EXPLAIN ANALYZE
SELECT country.CountryName as Location
	, 'Full Business Period' AS PeriodDescription
	, CONCAT(MIN(YEAR(orders.OrderDate)),' - ',MAX(YEAR(orders.OrderDate))) AS PeriodBucket 
	, CONCAT('€', FORMAT(SUM(product.productprice), 2)) AS TotalSales
	, CONCAT('€', FORMAT(SUM(product.productprice) / COUNT( DISTINCT YEAR(orders.OrderDate)), 2)) AS YearlyAVG
    , CONCAT('€', FORMAT(SUM(product.productprice) / COUNT( DISTINCT YEAR(orders.OrderDate)) / COUNT( DISTINCT MONTH(orders.OrderDate)), 2)) AS MonthlyAVG
FROM orders
LEFT JOIN product
	ON orders.productID = Orders.productID
LEFT JOIN companystore
	on orders.storeID = companystore.ID
LEFT JOIN country
	on companystore.countryID = country.ID
GROUP BY country.CountryName
;
#-> Group aggregate: count(distinct tmp_field), count(distinct tmp_field), sum(product.productprice), count(distinct tmp_field), sum(product.productprice), sum(product.productprice), max(tmp_field), min(tmp_field)  (actual time=1200.245..1641.231 rows=3 loops=1)
#-> Sort: country.CountryName  (actual time=980.024..1123.790 rows=587706 loops=1)        -> Stream results  (cost=58771.46 rows=587706) (actual time=0.403..439.552 rows=587706 loops=1)
#-> Left hash join (no condition)  (cost=58771.46 rows=587706) (actual time=0.391..99.495 rows=587706 loops=1)
#-> Nested loop left join  (cost=4802.60 rows=5997) (actual time=0.275..29.436 rows=5997 loops=1)
#-> Nested loop left join  (cost=2703.65 rows=5997) (actual time=0.258..19.250 rows=5997 loops=1)
#-> Table scan on orders  (cost=604.70 rows=5997) (actual time=0.213..5.492 rows=5997 loops=1)
#-> Single-row index lookup on companystore using PRIMARY (ID=orders.StoreID)  (cost=0.25 rows=1) (actual time=0.002..0.002 rows=1 loops=5997)
# -> Single-row index lookup on country using PRIMARY (ID=companystore.CountryID)  (cost=0.25 rows=1) (actual time=0.001..0.001 rows=1 loops=5997)
#-> Table scan on product  (cost=0.00 rows=98) (actual time=0.057..0.084 rows=98 loops=1)'

# OPTIMIZATION
# Create views regarding sales evolution per time lines, monthly, quarterly, hald year and yearly to get this type of answers directly and could be created views per costumer, stores and products
# , which would be easier to use in BI reports
# Some ways to optimize a simple select like this is to make sure only the necessary columns are grouping are used, as well indexed.

#5. List all the locations where products/services were sold, and the product has customer’s ratings (Yes, your ERD must consider that customers can give ratings).
-- EXPLAIN ANALYZE 
SELECT 'Business' AS RatingObject
	, SUM(orders.Quantity) AS NumberSoldProducts
    , COUNT(orders.orderID) AS NumberOfOrders
    , COUNT( distinct orders.CustomerID) AS NumberOfCustomers
	, ROUND(AVG(orders.rating),2) AS Rating
FROM orders
UNION
SELECT 'Portugal Store (Store 1)' AS RatingObject
	, SUM(orders.Quantity) AS NumberSoldProducts
    , COUNT(orders.orderID) AS NumberOfOrders
    , COUNT( distinct orders.CustomerID) AS NumberOfCustomers
	, ROUND(AVG(orders.rating),2) AS Rating
FROM orders
WHERE orders.StoreID = 1
UNION
SELECT 'Spain Store (Store 2)' AS RatingObject
	, SUM(orders.Quantity) AS NumberSoldProducts
    , COUNT(orders.orderID) AS NumberOfOrders
    , COUNT( distinct orders.CustomerID) AS NumberOfCustomers
	, ROUND(AVG(orders.rating),2) AS Rating
FROM orders
WHERE orders.StoreID = 2
UNION
SELECT 'France Store (Store 3)' AS RatingObject
	, SUM(orders.Quantity) AS NumberSoldProducts
    , COUNT(orders.orderID) AS NumberOfOrders
    , COUNT( distinct orders.CustomerID) AS NumberOfCustomers
	, ROUND(AVG(orders.rating),2) AS Rating
FROM orders
WHERE orders.StoreID = 3
;
#-> Table scan on <union temporary>  (cost=2449.84..2451.75 rows=4) (actual time=18.264..18.265 rows=4 loops=1)    
#-> Union materialize with deduplication  (cost=2449.20..2449.20 rows=4) (actual time=18.262..18.262 rows=4 loops=1)        
#-> Aggregate: avg(orders.Rating), sum(orders.Quantity), count(orders.OrderID), count(distinct orders.CustomerID)  (cost=1204.40 rows=1) (actual time=6.334..6.335 rows=1 loops=1)            
#-> Table scan on orders  (cost=604.70 rows=5997) (actual time=0.059..3.752 rows=5997 loops=1)        
#-> Aggregate: avg(orders.Rating), sum(orders.Quantity), count(orders.OrderID), count(distinct orders.CustomerID)  (cost=410.40 rows=1) (actual time=4.563..4.564 rows=1 loops=1)            
#-> Index lookup on orders using StoreID (StoreID=1)  (cost=212.70 rows=1977) (actual time=1.006..3.974 rows=1977 loops=1)        
#-> Aggregate: avg(orders.Rating), sum(orders.Quantity), count(orders.OrderID), count(distinct orders.CustomerID)  (cost=410.60 rows=1) (actual time=3.758..3.759 rows=1 loops=1)            
#-> Index lookup on orders using StoreID (StoreID=2)  (cost=212.80 rows=1978) (actual time=0.112..3.183 rows=1978 loops=1)        
#-> Aggregate: avg(orders.Rating), sum(orders.Quantity), count(orders.OrderID), count(distinct orders.CustomerID)  (cost=423.40 rows=1) (actual time=3.546..3.546 rows=1 loops=1)            
#-> Index lookup on orders using StoreID (StoreID=3)  (cost=219.20 rows=2042) (actual time=0.131..2.932 rows=2042 loops=1)'
