-- SELECT staff_numbers
-- FROM dim_store_details
-- WHERE staff_numbers !~ '^[0-9]+$';  -- Finds non-numeric rows

-- SELECT COUNT(*) FROM dim_store_details;

-- SELECT * 
-- FROM information_schema.columns 
-- WHERE table_name = 'dim_store_details';
-- SELECT staff_numbers
-- FROM dim_store_details;
-- DELETE FROM dim_store_details
-- WHERE staff_numbers ~ '[A-Za-z].*[0-9]|[0-9].*[A-Za-z]';

-- DELETE FROM dim_store_details
-- WHERE staff_numbers ~ '^[A-Za-z0-9]+$' AND staff_numbers !~ '^[0-9]+$';
-- UPDATE dim_store_details
-- SET staff_numbers = REGEXP_REPLACE(staff_numbers, '[A-Za-z]', '', 'g')
-- WHERE staff_numbers ~ '^[0-9]*[A-Za-z][0-9]*$';


-- SELECT COUNT(*) FROM dim_products;
-- SELECT * 
-- FROM dim_products;
-- UPDATE dim_products
-- SET product_price = REPLACE(product_price, '£', '')
-- WHERE product_price LIKE '£%';

-- SELECT product_price
-- FROM dim_products
-- WHERE product_price !~ '^[0-9]+$';  -- Finds non-numeric rows

-- DELETE FROM dim_products
-- WHERE product_price IN ('XCD69KUI0K', 'N9D2BZQX63', 'ODPMASE7V7');

-- SELECT column_name
-- FROM information_schema.columns
-- WHERE table_name = 'dim_products';

-- SELECT * FROM dim_products;
-- ALTER TABLE dim_products DROP COLUMN "Unnamed: 0";
-- SELECT "EAN" FROM dim_products;

-- UPDATE dim_products
-- SET still_available = TRUE
-- WHERE still_available = 'Still_avaliable';

-- SELECT * 
-- FROM dim_products;

-- UPDATE dim_products
-- SET removed_still_available = TRUE
-- WHERE removed_still_available = 'Still_avaliable';

-- UPDATE dim_products
-- SET still_available = 
--     CASE 
--         WHEN LOWER(TRIM(still_available)) = 'Still_available' THEN TRUE
--         WHEN LOWER(TRIM(still_available)) = 'removed' THEN FALSE
--         ELSE NULL  -- Handle unexpected values
--     END;

-- UPDATE dim_products
-- SET still_available = TRUE
-- WHERE still_available IS NULL;
-- ALTER TABLE dim_products
-- RENAME COLUMN removed_still_available TO still_avaliable;
-- ALTER TABLE dim_products
-- RENAME COLUMN still_avaliable TO still_available;


-- SELECT * FROM dim_date_times;
-- SELECT COUNT(*) FROM dim_date_times;
-- DELETE FROM dim_date_times 
-- WHERE date_uuid !~ '^[0-9a-fA-F-]{36}$';

-- SELECT date_uuid FROM dim_date_times WHERE date_uuid !~ '^[0-9a-fA-F-]{36}$';

-- SELECT * FROM orders_table;
-- SELECT * FROM dim_store_details;
-- SELECT table_name, column_name
-- FROM information_schema.columns
-- WHERE table_schema = 'public'  -- Replace 'public' with your schema if necessary
-- ORDER BY table_name, ordinal_position;
-- SELECT column_name
-- FROM information_schema.columns
-- WHERE table_name = 'orders_table' AND table_schema = 'public'
-- INTERSECT
-- SELECT column_name
-- FROM information_schema.columns
-- WHERE table_name = 'dim_users' AND table_schema = 'public';

-- -- Add primary key to dim_store_details
-- ALTER TABLE dim_store_details
-- ADD CONSTRAINT pk_store_details PRIMARY KEY (store_code);

-- -- Add primary key to dim_products
-- ALTER TABLE dim_products
-- ADD CONSTRAINT pk_products PRIMARY KEY (product_code);

-- -- Add primary key to dim_date_times
-- ALTER TABLE dim_date_times
-- ADD CONSTRAINT pk_date_times PRIMARY KEY (date_uuid);

-- -- Add primary key to dim_card_details
-- ALTER TABLE dim_card_details
-- ADD CONSTRAINT pk_card_details PRIMARY KEY (card_number);

-- ALTER TABLE dim_users
-- ADD CONSTRAINT pk_users PRIMARY KEY (user_uuid);

-- ALTER TABLE dim_users
-- ADD PRIMARY KEY (user_uuid);

-- Add foreign key referencing dim_products
-- ALTER TABLE orders_table
-- ADD CONSTRAINT fk_orders_product_code
-- FOREIGN KEY (product_code) 
-- REFERENCES dim_products(product_code)
-- ON DELETE CASCADE;

-- Add foreign key referencing dim_users

-- INSERT INTO dim_users (user_uuid, first_name, last_name, date_of_birth, company, email_address, address, country, country_code, phone_number, join_date)  
-- VALUES ('4f3703c0-e020-4d36-93f2-38f077e8467e', 'John', 'Doe', '1990-01-01', 'TechCorp', 'john.doe@example.com', '123 Main St', 'USA', '+1', '1234567890', '2023-01-01');
-- INSERT INTO dim_users (user_uuid, first_name, last_name, date_of_birth, company, email_address, address, country, country_code, phone_number, join_date)  
-- VALUES ('7508866e-18dd-4530-890b-a7fda09595f0', 'Jane', 'Doe', '1992-05-14', 'TechCorp', 'jane.doe@example.com', '456 Elm St', 'USA', '+1', '9876543210', '2023-06-01');

-- SELECT * FROM dim_users WHERE user_uuid = '74a4e9bf-4baf-4f17-bd35-594471d68938';
-- INSERT INTO dim_users (user_uuid, first_name, last_name, date_of_birth, company, email_address, address, country, country_code, phone_number, join_date)  
-- VALUES ('74a4e9bf-4baf-4f17-bd35-594471d68938', 'John', 'Doe', '1990-08-21', 'InnovateX', 'john.doe@example.com', '789 Oak St', 'USA', '+1', '1234567890', '2023-07-15');
-- INSERT INTO orders_table (order_id, user_uuid, order_date, total_amount)  
-- VALUES ('ORDER125', '74a4e9bf-4baf-4f17-bd35-594471d68938', '2024-02-04', 150.99);
-- SELECT * FROM orders_table

-- Rows in dim_users but not in orders_table
-- SELECT user_uuid
-- FROM dim_users
-- EXCEPT
-- SELECT user_uuid
-- FROM orders_table;

-- Rows in orders_table but not in dim_users
-- SELECT user_uuid
-- FROM orders_table
-- EXCEPT
-- SELECT user_uuid
-- FROM dim_users;

-- DELETE FROM orders_table
-- WHERE user_uuid NOT IN (SELECT user_uuid FROM dim_users);




-- ALTER TABLE orders_table
-- ADD CONSTRAINT fk_orders_user_uuid
-- FOREIGN KEY (user_uuid) 
-- REFERENCES dim_users(user_uuid)
-- ON DELETE CASCADE;

-- Drop the existing foreign key constraint
-- ALTER TABLE orders_table DROP CONSTRAINT fk_orders_user_uuid;

-- Alter the column to allow NULL values
-- ALTER TABLE orders_table ALTER COLUMN user_uuid DROP NOT NULL;

-- Recreate the foreign key constraint with 'ON DELETE SET NULL'
-- ALTER TABLE orders_table
  -- ADD CONSTRAINT fk_orders_user_uuid FOREIGN KEY (user_uuid) REFERENCES dim_users(user_uuid) ON DELETE SET NULL;
-- SELECT constraint_name
-- FROM information_schema.table_constraints
-- WHERE table_name = 'orders_table' AND constraint_type = 'FOREIGN KEY';

-- DELETE FROM orders_table WHERE user_uuid = 'a9073341-1fa3-4bcc-982f-5287f4224485';


-- ALTER TABLE orders_table
-- ADD CONSTRAINT fk_orders_user_uuid FOREIGN KEY (user_uuid)
-- REFERENCES dim_users(user_uuid);

-- Add foreign key referencing dim_date_times
-- ALTER TABLE orders_table
-- ADD CONSTRAINT fk_orders_date_uuid
-- FOREIGN KEY (date_uuid) 
-- REFERENCES dim_date_times(date_uuid)
-- ON DELETE CASCADE;

-- Add foreign key referencing dim_card_details

-- Rows in dim_card_details but not in orders_table
-- SELECT * FROM dim_card_details;
-- SELECT card_number
-- FROM dim_card_details
-- EXCEPT
-- SELECT card_number
-- FROM orders_table;

-- Rows in orders_table but not in dim_card_details
-- SELECT card_number
-- FROM orders_table
-- EXCEPT
-- SELECT card_number
-- FROM dim_card_details;


-- ALTER TABLE orders_table
-- ADD CONSTRAINT fk_orders_card_number
-- FOREIGN KEY (card_number) 
-- REFERENCES dim_card_details(card_number)
-- ON DELETE CASCADE;

-- SELECT user_uuid
-- FROM dim_users
-- WHERE user_uuid = '6904f151-6d32-4d1e-b477-41801a9a8e83';

-- INSERT INTO dim_users (user_uuid, [other_columns])
-- VALUES ('6904f151-6d32-4d1e-b477-41801a9a8e83', [other_values]);

-- DELETE FROM orders_table
-- WHERE user_uuid = '6904f151-6d32-4d1e-b477-41801a9a8e83';

-- SELECT * FROM dim_users WHERE user_uuid = '4f3703c0-e020-4d36-93f2-38f077e8467e';
-- SELECT column_name, data_type 
-- FROM information_schema.columns 
-- WHERE table_name IN ('dim_users', 'orders_table') AND column_name = 'user_uuid';
-- SELECT * FROM dim_users WHERE user_uuid = '4f3703c0-e020-4d36-93f2-38f077e8467e';
-- INSERT INTO dim_users (user_uuid, name) VALUES ('4f3703c0-e020-4d36-93f2-38f077e8467e', 'John Doe');
-- SELECT * FROM dim_users
-- SELECT COUNT(*) FROM orders_table;

