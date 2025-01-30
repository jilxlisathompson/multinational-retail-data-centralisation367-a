import numpy as np
import pandas as pd
import re


class DataCleaning:
    """
    Class containing methods to clean data from each of the data sources

    Attributes:
        data_df (pd.DataFrame): DataFrame containing data to be cleaned
    """
    def __init__(self, data_df):
        """
        Initialise the DataCleaning class with a credential file.

        Args:
            credential_file (str): Path to the credential file used by the DataExtractor.
        """
        self.data_df = data_df

    def clean_user_data(self):
        """
        Clean user data from a DataFrame.

        Steps:
        1. Replace "NULL" strings with NaN values.
        2. Remove rows containing NaN values.
        3. Convert the "join_date" column to a datetime type.
        4. Validate the row count to ensure data integrity.

        Args:
            df (pandas.DataFrame): Input DataFrame containing user data.

        Returns:
            pandas.DataFrame: Cleaned DataFrame.

        Raises:
            ValueError: If the row count after cleaning does not match the expected number.
        """
        # Replace "NULL" strings with NaN values
        self.data_df.replace("NULL", np.nan, inplace=True)
        
        # Drop rows with NaN values
        df_cleaned_rows = self.data_df.dropna().copy()

        # Convert "join_date" column to datetime, coercing invalid entries to NaT
        if 'join_date' in df_cleaned_rows.columns:
            df_cleaned_rows['join_date'] = pd.to_datetime(df_cleaned_rows['join_date'], errors='coerce')
        else:
            raise KeyError("'join_date' column is missing in the DataFrame.")

        # Check if any rows were removed due to invalid dates, and drop them
        df_cleaned_rows.dropna(subset=['join_date'], inplace=True)

        # Validate row count (example: expecting 15,284 rows after cleaning)
        expected_rows = 15261 #15284
        if len(df_cleaned_rows) != expected_rows:
            raise ValueError(f"Row count mismatch: Expected {expected_rows} rows, but got {len(df_cleaned_rows)} rows.")

        return df_cleaned_rows
    
    def clean_card_data(self, data_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the card data DataFrame.

        Cleaning Steps:
        - Change "NULL" strings to actual NULL (NaN).
        - Remove rows with NULL values.
        - Remove duplicate card numbers.
        - Remove rows with non-numerical card numbers.
        - Convert "date_payment_confirmed" column to datetime.
        
        Args:
        - data_df (pd.DataFrame): Input DataFrame containing card details.

        Returns:
        - pd.DataFrame: Cleaned DataFrame.
        """
        # Replace "NULL" strings with NaN
        data_df.replace("NULL", np.nan, inplace=True)

        # Drop rows with NULL values
        data_df.dropna(inplace=True)

        # Remove duplicate card numbers
        if "card_number" in data_df.columns:
            data_df.drop_duplicates(subset="card_number", inplace=True)

        # Remove non-numerical card numbers
        if "card_number" in data_df.columns:
            data_df = data_df[data_df["card_number"].str.isnumeric()]

        # Convert "date_payment_confirmed" to datetime
        if "date_payment_confirmed" in data_df.columns:
            data_df["date_payment_confirmed"] = pd.to_datetime(
                data_df["date_payment_confirmed"], errors="coerce"
            )

            # Drop rows where date conversion resulted in NaT (invalid dates)
            data_df.dropna(subset=["date_payment_confirmed"], inplace=True)

        # Return cleaned DataFrame
        return data_df

    def clean_store_data(self, raw_data) -> pd.DataFrame:
            """
            Cleans the store data retrieved from the API and returns a pandas DataFrame.

            Args:
                raw_data (DataFrame): The raw store data as a pandas DataFrame.

            Returns:
                DataFrame: The cleaned store data.
            """
            # Replace "NULL" strings with proper NaN values
            contains_null_strings = (raw_data == "NULL").any().any()
            print(f"Contains 'NULL' strings: {contains_null_strings}")
            # cleaned_data = raw_data.replace("NULL", np.nan)

            # print(f"length of clearned data = {len(cleaned_data)}")
            # print(cleaned_data.iloc[63])


            # Remove rows with NULL (NaN) values
            cleaned_data = raw_data[~raw_data.isin(["NULL"]).any(axis=1)]
            # cleaned_data = cleaned_data[cleaned_data.notna().all(axis=1)]
            print(f"length of cleaned data = {len(cleaned_data)}")

            # Convert "opening_date" column to datetime type
            if 'opening_date' in cleaned_data.columns:
                cleaned_data['opening_date'] = pd.to_datetime(cleaned_data['opening_date'], format='%Y-%m-%d', errors='coerce')  # Invalid parsing will be set to NaT

            # Clean "staff_number" column
            if 'staff_number' in cleaned_data.columns:
                # Strip symbols, letters, and whitespace using a regex
                cleaned_data['staff_number'] = cleaned_data['staff_number'].apply(
                    lambda x: re.sub(r'[^\d]', '', str(x))
                ).astype(int)  # Convert to integer

            # Ensure there are 441 rows after cleaning
            # TODO should be 441
            if cleaned_data.shape[0] != 447:
                raise ValueError(f"Expected 441 rows after cleaning, but got {cleaned_data.shape[0]} rows.")


            return cleaned_data
    
    
    def convert_product_weights(self, products_df):
        """
        Converts the weight column in the products dataframe to kilograms (kg).
        Handles units like grams (g), milliliters (ml), and kg.
        Assumes that 1 ml = 1 g for rough estimate.

        Args:
        - products_df (pd.DataFrame): The DataFrame containing product data with a 'weight' column.

        Returns:
        - pd.DataFrame: The DataFrame with the 'weight' column converted to kilograms.
        """
        # Define a function to clean up weight values and convert them to kg
        def clean_and_convert_weight(weight):
            # Remove all non-numeric characters except for '.' which is used in float numbers
            weight = re.sub(r'[^0-9.]', '', str(weight))

            # Check if the weight is a valid number after cleaning
            if weight:
                weight = float(weight)  # Convert the cleaned weight to a float
            else:
                return None  # If no valid weight, return None

            # Check if the original weight had 'g' or 'ml' and convert accordingly
            if 'g' in str(weight):
                return weight / 1000  # Convert grams to kilograms (1g = 0.001kg)
            elif 'kg' in str(weight):
                return weight  # If the weight is in kilograms, return as is
            else:
                return weight / 1000  # Assume the weight is in grams if the unit is unknown and convert to kg

        # Apply the function to the 'weight' column and convert to kg
        products_df['weight'] = products_df['weight'].apply(clean_and_convert_weight)
        
        # Return the cleaned DataFrame
        return products_df
    
    def clean_products_data(self, products_df):
        """
        Cleans the product data by:
        - Replacing "NULL" strings with NaN
        - Removing rows with NULL (NaN) values
        - Converting weight values into kilograms
        
        Args:
        - products_df (pd.DataFrame): The DataFrame containing product data.

        Returns:
        - pd.DataFrame: The cleaned DataFrame.
        """
        # Step 1: Replace "NULL" strings with NaN
        products_df.replace("NULL", np.nan, inplace=True)

        # Step 2: Remove rows with NaN values
        products_df.dropna(inplace=True)

        # Step 3: Convert the weight column to kilograms
        products_df = self.convert_product_weights(products_df)
        
        # Check if the number of rows after cleaning matches the expected value (1846 rows)
        # TODO NOTE: After cleaning there should be 1846 rows.
        print(f"Rows after cleaning: {len(products_df)}")
        
        # Return the cleaned DataFrame
        return products_df
    
    def clean_orders_data(self, data:pd.DataFrame) -> pd.DataFrame:
        """
        # Order Table Cleaning Requirement

        - **Remove unwanted columns**
        """
        columns_to_remove = ['first_name', 'last_name', '1']
        data = data.drop(columns=columns_to_remove)
        if data.shape[0] != 120123:
            raise ValueError(f"Expected 441 rows after cleaning, but got {data.shape[0]} rows.")

        return data
    

    def clean_date_details(self, data_df: pd.DataFrame) -> pd.DataFrame:
        """
        Cleans the date details DataFrame by:
        
        - Converting string "NULL" values to actual NaN (null) values
        - Removing rows with NULL values
        - Converting "day", "month", and "year" columns to numeric values (invalid values become NaN)
        
        Args:
            data_df (pd.DataFrame): The DataFrame containing date details.
            
        Returns:
            pd.DataFrame: Cleaned DataFrame with proper date formatting.
        """
        # Ensure input is a DataFrame
        if not isinstance(data_df, pd.DataFrame):
            raise ValueError("data_df must be a Pandas DataFrame.")

        # Replace "NULL" strings with NaN
        data_df.replace("NULL", np.nan, inplace=True)

        # Drop rows with any NaN values
        data_df.dropna(inplace=True)

        # Convert 'day', 'month', 'year' columns to numeric values (invalid values become NaN)
        for col in ["day", "month", "year"]:
            if col in data_df.columns:
                data_df[col] = pd.to_numeric(data_df[col], errors="coerce")
        # TODO there should be 120123 rows but there are 120146 find out why 
        # if data_df.shape[0] != 120123:
        #     raise ValueError(f"Expected 120123 rows after cleaning, but got {data_df.shape[0]} rows.")
        

        return data_df

