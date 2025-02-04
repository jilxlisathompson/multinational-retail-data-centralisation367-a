import psycopg2


# TASK 1: Cast the columns of the orders_tables to the correct datatypes 
# The ? in VARCHAR should be replaced with an integer representing the maximum length of the values in that column.
"""
+------------------+--------------------+--------------------+
|   orders_table   | current data type  | required data type |
+------------------+--------------------+--------------------+
| date_uuid        | TEXT               | UUID               |
| user_uuid        | TEXT               | UUID               |
| card_number      | TEXT               | VARCHAR(?)         |
| store_code       | TEXT               | VARCHAR(?)         |
| product_code     | TEXT               | VARCHAR(?)         |
| product_quantity | BIGINT             | SMALLINT           |
+------------------+--------------------+--------------------+
"""
class DatabaseManager:

    def __init__(self, db_config: dict):
        self.db_config = db_config
        self.conn = None
        self.cursor = None
    

    
    def connect(self):
        """
        Args:
        Return:
        """

        try: 
            self.conn = psycopg2.connect(**self.db_config)
            self.conn.autocommit = True 
            self.cursor = self.conn.cursor()
            print("Database connection established.")
        except Exception as e:
            print(f"Error connecting to database: {e}")
            self.conn = None
            self.cursor = None

    def close(self):
        """
        Args:
        Return:
        """
        if self.cursor:
            self.cursor.close()
        if self.conn:
            self.conn.close()
            print("Database connection closed.")
        
    def get_max_length(self, column_name: str, table_name: str):
        """
        Args:

        Return:
        """
        try:      
            query = f"SELECT MAX(LENGTH(CAST({column_name} AS TEXT))) FROM {table_name};"
            self.cursor.execute(query)
            return self.cursor.fetchone()[0] or 1  # Default to 1 if column is empty 
        except Exception as e:
            print(f"Error getting max length for {column_name}: {e}")
            return 1

    def alter_orders_table(self):
        """
        Args:
        Return:
        """
        try: 
            table_name = "orders_table"

            # Get max length for VARCHAR columns
            varchar_columns = ["card_number", "store_code", "product_code"]
            max_lengths = {col: self.get_max_length(col, table_name) 
                        for col in varchar_columns}
            
            # Construct ALTER TABLE query 
            alter_query = f""" ALTER TABLE {table_name} 
                                    ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid::UUID,
                                    ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid::UUID,
                                    ALTER COLUMN card_number TYPE VARCHAR({max_lengths['card_number']}),
                                    ALTER COLUMN store_code TYPE VARCHAR({max_lengths['store_code']}),
                                    ALTER COLUMN product_code TYPE VARCHAR({max_lengths['product_code']}),
                                    ALTER COLUMN product_quantity TYPE SMALLINT;
                                """
            self.cursor.execute(alter_query)
            print("Table schema updated succesfully!")
        except Exception as e:
            print(f"Error updating table schema: {e}")
    
    # TASK 2: cast the columns of the dim_users to the correct data types 
    def alter_dim_users(self):
        """
        The column required to be changed in the users table are as follows:

        +----------------+--------------------+--------------------+
        | dim_users      | current data type  | required data type |
        +----------------+--------------------+--------------------+
        | first_name     | TEXT               | VARCHAR(255)       |
        | last_name      | TEXT               | VARCHAR(255)       |
        | date_of_birth  | TEXT               | DATE               |
        | country_code   | TEXT               | VARCHAR(?)         |
        | user_uuid      | TEXT               | UUID               |
        | join_date      | TEXT               | DATE               |
        +----------------+--------------------+--------------------+

        """

        try: 
            table_name = "dim_users"

            # Get max length for VARCHAR columns
            varchar_columns = [ "country_code"]
            max_lengths = {col: self.get_max_length(col, table_name) 
                        for col in varchar_columns}
            
            # Construct ALTER TABLE query 
            alter_query = f""" ALTER TABLE {table_name} 
                                    ALTER COLUMN first_name TYPE VARCHAR(255),
                                    ALTER COLUMN last_name TYPE VARCHAR(255),
                                    ALTER COLUMN date_of_birth TYPE DATE USING date_of_birth::DATE,
                                    ALTER COLUMN country_code TYPE VARCHAR({max_lengths['country_code']}),
                                    ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID,
                                    ALTER COLUMN join_date TYPE DATE USING join_date::DATE;
                                """
            self.cursor.execute(alter_query)
            print("Table schema for dim_users updated successfully!")
        except Exception as e:
            print(f"Error updating table schema: {e}")

    # TASK 3: update the dim_store_details table
    def alter_dim_store_details(self):
        """
        There are two latitude columns in the store details table. Using SQL, merge one of the columns into the other so you have one latitude column.

        Then set the data types for each column as shown below:

        +---------------------+-------------------+------------------------+
        | store_details_table | current data type |   required data type   |
        +---------------------+-------------------+------------------------+
        | longitude           | TEXT              | NUMERIC                |
        | locality            | TEXT              | VARCHAR(255)           |
        | store_code          | TEXT              | VARCHAR(?)             |
        | staff_numbers       | TEXT              | SMALLINT               |
        | opening_date        | TEXT              | DATE                   |
        | store_type          | TEXT              | VARCHAR(255) NULLABLE  |
        | latitude            | TEXT              | NUMERIC                |
        | country_code        | TEXT              | VARCHAR(?)             |
        | continent           | TEXT              | VARCHAR(255)           |
        +---------------------+-------------------+------------------------+
        There is a row that represents the business's website change the location column values from N/A to NULL.
        """
        """Merges latitude columns and updates the table schema."""
        table_name = "dim_store_details"

        # Merge latitude columns (assuming the columns are named latitude_1 and latitude_2)
        # Update latitude_1 with latitude_2 if latitude_1 is NULL or empty
        # merge_query = f"""
        #     UPDATE {table_name}
        #     SET latitude = COALESCE("latitude_1", "latitude_2")
        #     WHERE latitude IS NULL OR latitude = '';
        # """
        # self.cursor.execute(merge_query)

        # Set data types for each column
        # First, replace N/A with NULL in the location column
        # update_location_query = """
        #     UPDATE {table_name}
        #     SET location = NULL
        #     WHERE location = 'N/A';
        # """
        # self.cursor.execute(update_location_query)

        # Now, alter the table schema
        alter_query = f"""
            ALTER TABLE {table_name}
            ALTER COLUMN longitude TYPE NUMERIC USING longitude::NUMERIC,
            ALTER COLUMN locality TYPE VARCHAR(255),
            ALTER COLUMN store_code TYPE VARCHAR({self.get_max_length('store_code', table_name)}),
            ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::SMALLINT,
            ALTER COLUMN opening_date TYPE DATE USING opening_date::DATE,
            ALTER COLUMN store_type TYPE VARCHAR(255),
            ALTER COLUMN latitude TYPE NUMERIC USING latitude::NUMERIC,
            ALTER COLUMN country_code TYPE VARCHAR({self.get_max_length('country_code', table_name)}),
            ALTER COLUMN continent TYPE VARCHAR(255);
        """
        self.cursor.execute(alter_query)
        print("Table schema for dim_store_details updated successfully!")
    
    # TASK 4: Make changes to the dim_products table for the delivery team 
    """
    You will need to do some work on the products table before casting the data types correctly.

    The product_price column has a £ character which you need to remove using SQL.

    The team that handles the deliveries would like a new human-readable column added for the weight so they can quickly make decisions on delivery weights.

    Add a new column weight_class which will contain human-readable values based on the weight range of the product.

    +--------------------------+-------------------+
    | weight_class VARCHAR(?)  | weight range(kg)  |
    +--------------------------+-------------------+
    | Light                    | < 2               |
    | Mid_Sized                | >= 2 - < 40       |
    | Heavy                    | >= 40 - < 140     |
    | Truck_Required           | => 140            |
    +----------------------------+-----------------+
    """

    def alter_dim_products(self):
        """
        This function performs the following operations on the dim_products table:
        1. Removes the £ sign from the product_price column.
        2. Adds a new column 'weight_class'.
        3. Updates the 'weight_class' column based on the weight ranges.
        """

        try:
            # Step 1: Remove the £ character from product_price
            remove_currency_query = """
                UPDATE dim_products
                SET product_price = REPLACE(product_price, '£', '')
                WHERE product_price LIKE '£%';
            """
            self.cursor.execute(remove_currency_query)

            # Step 2: Add the weight_class column
            add_column_query = """
                ALTER TABLE dim_products
                ADD COLUMN weight_class VARCHAR(50);
            """
            self.cursor.execute(add_column_query)

            # Step 3: Update weight_class based on the weight ranges
            update_weight_class_query = """
                UPDATE dim_products
                SET weight_class = 
                    CASE
                        WHEN weight < 2 THEN 'Light'
                        WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
                        WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
                        WHEN weight >= 140 THEN 'Truck_Required'
                        ELSE 'Unknown'
                    END;
            """
            self.cursor.execute(update_weight_class_query)

            # Commit the changes
            # self.connection.commit()

            print("Table dim_products updated successfully.")
        
        except Exception as e:
            # If there's an error, print the error message
            print(f"Error occurred: {e}")

    # TASK 5: Update the dim_products table with the required data types

    """
    After all the columns are created and cleaned, change the data types of the products table.

    You will want to rename the removed column to still_available before changing its data type.

    Make the changes to the columns to cast them to the following data types:

    +-----------------+--------------------+--------------------+
    |  dim_products   | current data type  | required data type |
    +-----------------+--------------------+--------------------+
    | product_price   | TEXT               | NUMERIC            |
    | weight          | TEXT               | NUMERIC            |
    | EAN             | TEXT               | VARCHAR(?)         |
    | product_code    | TEXT               | VARCHAR(?)         |
    | date_added      | TEXT               | DATE               |
    | uuid            | TEXT               | UUID               |
    | still_available | TEXT               | BOOL               |
    | weight_class    | TEXT               | VARCHAR(?)         |
    +-----------------+--------------------+--------------------+

    """

    def alter_dim_products2(self):
        """
        This function performs the following operations on the dim_products table:
        1. Renames the removed 'still_available' column.
        2. Changes the data types of several columns in the dim_products table.
        """
        table_name='dim_products'

        try:
            # Step 1: Rename the removed column to still_available
            # rename_column_query = """
            #     ALTER TABLE dim_products
            #     RENAME COLUMN removed TO still_available;
            # """
            # self.cursor.execute(rename_column_query)

            # Step 2: Alter the data types of the columns
            alter_column_queries = f"""
                ALTER TABLE {table_name}
                ALTER COLUMN product_price TYPE NUMERIC USING product_price::NUMERIC,
                ALTER COLUMN weight TYPE NUMERIC USING weight::NUMERIC,
                ALTER COLUMN "EAN" TYPE VARCHAR(255), 
                ALTER COLUMN product_code TYPE VARCHAR(255), 
                ALTER COLUMN date_added TYPE DATE USING date_added::DATE,
                ALTER COLUMN uuid TYPE UUID USING uuid::UUID,
                ALTER COLUMN still_available TYPE BOOL USING still_available::BOOLEAN,
                ALTER COLUMN weight_class TYPE VARCHAR(255);  
            """

            # Execute each ALTER query
            self.cursor.execute(alter_column_queries)

            print("Table dim_products updated successfully.")

        except Exception as e:
            # If there's an error, print the error message
            print(f"Error occurred: {e}")

    # TASK 6: Update the dim_date_times_table
    """
    Now update the date table with the correct types:

    +-----------------+-------------------+--------------------+
    | dim_date_times  | current data type | required data type |
    +-----------------+-------------------+--------------------+
    | month           | TEXT              | VARCHAR(?)         |
    | year            | TEXT              | VARCHAR(?)         |
    | day             | TEXT              | VARCHAR(?)         |
    | time_period     | TEXT              | VARCHAR(?)         |
    | date_uuid       | TEXT              | UUID               |
    +-----------------+-------------------+--------------------+
    """

    def alter_dim_date_times_table(self):

           # Alter column data types
        try:
            alter_query = """
                ALTER TABLE dim_date_times
                ALTER COLUMN month TYPE VARCHAR(20),
                ALTER COLUMN year TYPE VARCHAR(4),
                ALTER COLUMN day TYPE VARCHAR(2),
                ALTER COLUMN time_period TYPE VARCHAR(20),
                ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;
            """

            # Execute query
            self.cursor.execute(alter_query)
            print("dim_date_times table updated successfully.")

        except psycopg2.Error as e:
            print("Error occurred:", e)

    # TASK 7: Updating the dim_card_details table 
    """
    Now we need to update the last table for the card details.

    Make the associated changes after finding out what the lengths of each variable should be:

    +------------------------+-------------------+--------------------+
    |    dim_card_details    | current data type | required data type |
    +------------------------+-------------------+--------------------+
    | card_number            | TEXT              | VARCHAR(?)         |
    | expiry_date            | TEXT              | VARCHAR(?)         |
    | date_payment_confirmed | TEXT              | DATE               |
    +------------------------+-------------------+--------------------+
    """

    def update_dim_card_details_table(self):
        """
        This function updates the dim_card_details table:
        1. Changes card_number and expiry_date to VARCHAR of appropriate lengths.
        2. Converts date_payment_confirmed to DATE.
        """
        try:

            table_name = "dim_card_details"

            # Get max length for VARCHAR columns
            varchar_columns = ["card_number", "expiry_date"]
            max_lengths = {col: self.get_max_length(col, table_name) 
                        for col in varchar_columns}

            # Step 2: Alter column data types
            alter_query = f"""
                ALTER TABLE dim_card_details
                ALTER COLUMN card_number TYPE VARCHAR({max_lengths['card_number']})),
                ALTER COLUMN expiry_date TYPE VARCHAR({max_lengths['expiry_date']})),
                ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::DATE;
            """
            self.cursor.execute(alter_query)

            print("dim_card_details table updated successfully!")

        except Exception as e:
            print("Error occurred:", e)
    
    # TODO: TASK 9: Finalising the star-based schema & adding the foreign keys to the orders table 
    """
    With the primary keys created in the tables prefixed with dim we can now create 
    the foreign keys in the orders_table to reference the primary keys in the other tables.

    Use SQL to create those foreign key constraints that reference the primary keys of the other table.

    This makes the star-based database schema complete.


    """
    # TODO: TASK 10: Update the latest code changes to Github
    """
    Update your GitHub repository with the latest code changes from your local project. 
    Start by staging your modifications and creating a commit. Then, push the changes to your GitHub repository.

    Additionally, document your progress by adding to your GitHub README file. 
    You can refer to the relevant lesson in the prerequisites for this task for more information.

    At minimum, your README file should contain the following information:

    Project Title
    Table of Contents, if the README file is long
    A description of the project: what it does, the aim of the project, and what you learned
    Installation instructions
    Usage instructions
    File structure of the project
    License information
    You don't have to write all of this at once, but make sure to update your README file as you go along, 
    so that you don't forget to add anything.


    """



        









