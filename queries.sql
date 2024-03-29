## TASK 1:
SELECT 
	country_code AS country,
	COUNT(store_code) AS total_no_store 
FROM 
    dim_store_details
GROUP BY 
    country
ORDER BY 
    total_no_store DESC;



## TASK 2:
SELECT locality, 
       COUNT(locality) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    locality
ORDER BY
    total_no_stores DESC
LIMIT 
    7;


## TASK 3:
SELECT ROUND(SUM (orders_table.product_quantity * dim_products.product_price)) AS total_sales,
	       dim_date_times.month
FROM 
    orders_table
JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY 
    dim_date_times.month
ORDER BY 
    total_sales DESC
LIMIT 6;


##TASK 4:

SELECT COUNT (product_quantity) AS number_of_sales,
	   SUM(product_quantity) AS product_quantity_count,
(CASE 
	WHEN store_code LIKE '%WEB%' THEN 'online'
	WHEN store_code NOT LIKE '%WEB%' THEN 'offline'
	
END) AS location
FROM 
	orders_table
GROUP BY 
    store_code
ORDER BY 
    location DESC
LIMIT 2;

###TASK 5

SELECT  store_type, 
		total_sales,
		ROUND( (COUNT(total_sales)*100/ SUM(COUNT(total_sales)) over()), 2) AS "percentage_total(%)"
		
FROM 
	(
	SELECT ROUND( SUM(dim_products.product_price * orders_table.product_quantity) ) AS total_sales,
	       dim_store_details.store_type
		  
FROM 
    orders_table
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN 
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
GROUP BY 
    dim_store_details.store_type
ORDER BY 
    store_type DESC
	) AS "percentage_total(%)"
	GROUP BY store_type,total_sales
	ORDER BY total_sales DESC;

###TASK 6 


SELECT ROUND(SUM(dim_products.product_price * orders_table.product_quantity)) AS "Total_sales",
	   dim_date_times.year AS "Year",
	   dim_date_times.month AS "Month"
 	
FROM 
    orders_table
JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY  
    dim_date_times.year,dim_date_times.month
ORDER BY "Total_sales" DESC
LIMIT 10;

###TASK 7 
SELECT 
	  SUM(staff_numbers) AS "Total_staff_numbers",
	  country_code AS "Country_code" 
FROM 
	dim_store_details

GROUP BY 
    country_code
ORDER BY
     "Total_staff_numbers" DESC;




###TASK 8


SELECT ROUND(SUM(dim_products.product_price * orders_table.product_quantity)) AS "Total_sales",
	   dim_store_details.store_type,
	   dim_store_details.country_code  
FROM 
	orders_table
JOIN 
	dim_products ON orders_table.product_code = dim_products.product_code
JOIN 
	dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE 
	country_code = 'DE'
GROUP BY  dim_store_details.store_type , dim_store_details.country_code
ORDER BY "Total_sales" ASC; 



###TASK 9


WITH cte AS
(	
SELECT year,
	   CAST ((CONCAT(year, ' ', month, ' ' ,day, ' ' ,timestamp) ) AS timestamp) AS transaction
	  
FROM
	 dim_date_times
), cte2 AS (
	SELECT	year,
	        transaction,
	        LEAD(transaction,1) OVER(ORDER BY transaction) AS next_transaction
FROM	  
	cte
),cte3 AS(
	SELECT year,
		   transaction,
		   next_transaction,
		   (transaction - next_transaction) AS difference_between_transactions
	       
FROM
	cte2
	)	
	SELECT year,
		   --transaction,
	       --next_transaction,
		   AVG(difference_between_transactions) AS actual_time_taken
FROM cte3
GROUP BY year
ORDER BY actual_time_taken 
LIMIT 5;






