This file contains the SQL queries in this project 
## Task 1: How many stores does the business have and in how many countries. 
- Query to get countries the business operates in and the country with the most physical stores
  
``` SELECT * FROM dim_store_details;
WITH store_counts AS (
    SELECT country_code, COUNT(*) AS store_count
    FROM dim_store_details
    GROUP BY country_code
)
SELECT 
    (SELECT ARRAY_AGG(country_code) FROM store_counts) AS countries_operating_in,
    country_code AS country_with_most_stores,
    store_count
FROM store_counts
ORDER BY store_count DESC
LIMIT 3;
```
- use ARRAY_AGG to group data in a structured list

### RESULT of QUERY
![image](https://github.com/user-attachments/assets/c87e0170-8f0c-4087-a125-54a29e63f106)

## TASK 2: Which locations currently have the most stores. 
- Query to get locations with the most stores
  
``` SELECT locality, COUNT(*) AS store_count
FROM dim_store_details
GROUP BY locality
ORDER BY store_count DESC
LIMIT 7;
```

### RESULT of QUERY
![image](https://github.com/user-attachments/assets/9c3b869c-61b7-4c0a-a466-4448518c25ba)

## TASK 3: Which months produced the largest amount of sales. 
- Query to find the months with the most sales

``` SELECT 
    d.month, 
    SUM(o.product_quantity * p.product_price) AS total_sales
FROM orders_table o
JOIN dim_date_times d ON o.date_uuid = d.date_uuid
JOIN dim_products p ON o.product_code = p.product_code
GROUP BY d.month
ORDER BY total_sales DESC
LIMIT 6;
```

### RESULT of QUERY
![image](https://github.com/user-attachments/assets/ec1e4a65-3696-4539-b0f9-9762ed6cedf2)

## TASK 4: How many sales are coming from online? 
- The company is looking to increase its online sales. They want to know how many sales are happening online vs. offline.

```
WITH sales_data AS (
    SELECT 
        o.user_uuid,
        o.product_code,
        o.product_quantity,
        p.product_price,
        c.date_payment_confirmed,
        CASE 
            WHEN o.user_uuid IS NOT NULL AND c.date_payment_confirmed IS NOT NULL 
                THEN 'Web'
            ELSE 'Offline'
        END AS sales_channel
    FROM orders_table o
    JOIN dim_products p ON o.product_code = p.product_code
    LEFT JOIN dim_users u ON o.user_uuid = u.user_uuid
    LEFT JOIN dim_card_details c ON o.card_number = c.card_number
)
SELECT 
    sales_channel,
    COUNT(product_code) AS total_orders,
    SUM(product_quantity) AS total_products_sold,
    SUM(product_quantity * product_price) AS total_sales_amount
FROM sales_data
GROUP BY sales_channel;
```

### RESULT of QUERY
![image](https://github.com/user-attachments/assets/e4c37bd3-6ee2-42f9-82c6-f19225c5458b)

## TASK 5: What percentage of Sales comes through each type of store?
  - The sales team wants to know which of the different store types is generated the most revenue so they know where to focus.
  - Find out the total revenue coming from each of the different store types and the number of sales made as a percentage.

``` SELECT  dim_date_times.year,
		dim_date_times.month, 
		round(SUM(orders_table.product_quantity * dim_products.product_price)) AS revenue
FROM orders_table
	JOIN dim_date_times    ON  orders_table.date_uuid    = dim_date_times.date_uuid
	JOIN dim_products      ON  orders_table.product_code = dim_products.product_code
	JOIN dim_store_details ON orders_table.store_code    = dim_store_details.store_code
GROUP BY 	dim_date_times.month,
			dim_date_times.year
ORDER BY    SUM(orders_table.product_quantity * dim_products.product_price)  DESC
LIMIT 10;
```

### RESULT of QUERY
![image](https://github.com/user-attachments/assets/219d0512-7537-44dd-935f-d62ba4f40596)

## TASK 7: What is our staff Headcount
  - The operations team would like to know the overall staff numbers in each location around the world. Perform a query to determine the staff numbers in each of the countries the company sells in.

``` SELECT  
    SUM(staff_numbers) AS total_staff_numbers,  
    country_code  
FROM dim_store_details  
GROUP BY country_code  
ORDER BY total_staff_numbers DESC;
```

### RESULT of QUERY
![image](https://github.com/user-attachments/assets/cf375e49-bfa4-47cf-87d1-e960dd9ac4d8)

## TASK 8: Which German Store Type is selling the most 
  - The sales team is looking to expand their territory in Germany.
  - Determine which type of store is generating the most sales in Germany.

``` SELECT 
    ROUND(COUNT(*) ) AS sales,
    dim_store_details.store_type,
    dim_store_details.country_code
FROM orders_table
JOIN dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN dim_products ON orders_table.product_code = dim_products.product_code
JOIN dim_store_details ON orders_table.store_code = dim_store_details.store_code
WHERE dim_store_details.country_code = 'DE'
GROUP BY dim_store_details.store_type, dim_store_details.country_code
ORDER BY sales DESC;
```

### RESULT of QUERY
![image](https://github.com/user-attachments/assets/3bcc8711-4c8a-4e0b-b67c-74891e77ebcc)

## TASK 9: How quickly is the company making sales
  - Sales would like the get an accurate metric for how quickly the company is making sales.
First step is to add a new column containing th full time stamp since the timestamp, date, month, year are in separate columns

``` ALTER TABLE dim_date_times
ADD timestamp_converted TIMESTAMP;
UPDATE dim_date_times
SET timestamp_converted = TO_TIMESTAMP(timestamp, 'YYYY-MM-DD HH24:MI:SS');
```

Then update the column "timestap_converted" with the timestamp data from the 4 columns mentioned above 

``` UPDATE dim_date_times
SET timestamp_converted = TO_TIMESTAMP(
    year || '-' || month || '-' || day || ' ' || timestamp,
    'YYYY-MM-DD HH24:MI:SS'
);
```

Subsequently add new columns containing the time difference between the timestamps in dim_date_times,
which are calculated and the dim_date_times table updated with the time differences

``` ALTER TABLE dim_date_times
ADD COLUMN time_diff interval;
UPDATE dim_date_times
SET time_diff = x.time_diff
FROM (
   SELECT timestamp_converted, 
          timestamp_converted - LAG(timestamp_converted) OVER (ORDER BY timestamp_converted) AS time_diff
   FROM dim_date_times
 ) AS x
 WHERE dim_date_times.timestamp_converted = x.timestamp_converted;
```

Aggregate the year and the time difference and present results
```SELECT 
    dim_date_times.year,
    CONCAT(
        '"hours": ', EXTRACT(hour FROM AVG(dim_date_times.time_diff)), ' ',
        '"minutes": ', EXTRACT(minute FROM AVG(dim_date_times.time_diff)), ' ',
        '"seconds": ', ROUND(EXTRACT(second FROM AVG(dim_date_times.time_diff)), 2), ' '
    ) AS actual_time_taken
FROM dim_date_times
GROUP BY dim_date_times.year
ORDER BY AVG(dim_date_times.time_diff) DESC
LIMIT 5;
```

### RESULT of QUERY
![image](https://github.com/user-attachments/assets/20b4add0-9e17-4ee6-ba0f-1638765258be)

