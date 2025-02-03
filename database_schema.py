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
            query = f"SELECT MAX(LENGTH({column_name})) FROM {table_name};"
            self.cursor.execute(query)
            return self.cursor.fectone()[0] or 1  # Default to 1 if column is empty 
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
                                    ALTER COLUMN date_uuid SET DATA TYPE UUID USING date_uuid:UUID,
                                    ALTER COLUMN user_uuid SET DATA TYPE UUID USING user_uuid:UUID,
                                    ALTER COLUMN card_number SET DATA TYPE VARCHAR({max_lengths['card_number']}),
                                    ALTER COLUMN store_code SET DATA VARCHAR({max_lengths['store_code']}),
                                    ALTER COLUMN product_code SET DATA VARCHAR({max_lengths['product_code']}),
                                    ALTER COLUMN product_quality SET DATA TYPE SMALLINT;
                                """
            self.cursor.execute(alter_query)
            print("Table schema updated succesfully!")
        except Exception as e:
            print(f"Error updating table schema: {e}")


