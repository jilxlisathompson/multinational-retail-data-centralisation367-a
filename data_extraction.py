import pandas as pd
from database_utils import DatabaseConnector  # Ensure this is correctly implemented and accessible
import pdfplumber
import pandas as pd
import requests
from io import BytesIO, StringIO
import boto3

class DataExtractor:
    def __init__(self, credential_file='login.yaml', api_key="yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX", ):
        """
        Initializes the DataExtractor with a DatabaseConnector instance.

        Args:
        - credential_file: Path to the credentials YAML file.
        """
        self.db_connector = DatabaseConnector(credential_file)  # Create an instance of DatabaseConnector
        self.headers = {'x-api-key': api_key}
        self.s3_client = boto3.client('s3')

    def read_rds_table(self, table_name):
        """
        Extracts the specified table from the RDS database and returns it as a pandas DataFrame.

        Args:
        - table_name: Name of the table to extract data from.

        Returns:
        - A pandas DataFrame containing the table's data.
        """
        # Ensure the table exists in the database
        tables = self.db_connector.list_db_tables()  # Use the list_db_tables method from DatabaseConnector
        if table_name not in tables:
            raise ValueError(f"Table {table_name} does not exist in the database.")

        # Initialize the database engine
        engine = self.db_connector.init_db_engine()

        # Read the table into a pandas DataFrame
        df = pd.read_sql_table(table_name, engine)  # Use the engine set by init_db_engine()
        return df
    
    def retrieve_pdf_data(self, pdf_url) -> pd.DataFrame:


        # # Set the path to your JVM if needed
        # java_path = "/Library/Internet Plug-Ins/JavaAppletPlugin.plugin/Contents/Home"  # Change this to the correct path
        # os.environ['JAVA_HOME'] = java_path
        # jpype.startJVM()


       # Download the PDF
        response = requests.get(pdf_url)
        pdf_data = BytesIO(response.content)

        # Open the downloaded PDF with pdfplumber
        with pdfplumber.open(pdf_data) as pdf:
            first_page = pdf.pages[0]
            table = first_page.extract_table()

            # Convert the table into a DataFrame
            df = pd.DataFrame(table[1:], columns=table[0])

        return df
    
    def list_number_of_stores(self, number_stores_endpoint, headers):
            try:
                # Send a GET request to retrieve the number of stores
                response = requests.get(number_stores_endpoint, headers=headers)
                
                # Check if the request was successful (HTTP status 200)
                if response.status_code == 200:
                    # Parse the JSON response and return the number of stores
                    data = response.json()
                    print("here is data")
                    print(data.keys())
                    return data['number_stores']
                    # return data.get('number_of_stores', 0)  # Assuming the key is 'number_of_stores'
                else:
                    # If the response code is not 200, raise an exception
                    response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error occurred: {e}")
                return None
    
    def retrieve_stores_data(self, number_of_stores, retrieve_store_endpoint):
        # Initialize an empty list to hold store data
        stores_data = []
        
        
        # Loop through each store number and get its details
        for store_number in range(1, number_of_stores + 1):
            # Construct the URL for the store
            store_url = retrieve_store_endpoint.format(store_number=store_number)
            
            # Make a GET request to retrieve store data
            response = requests.get(store_url, headers=self.headers)
            
            if response.status_code == 200:
                # If the response is successful, convert the store data to a dictionary
                store_data = response.json()
                stores_data.append(store_data)
            else:
                print(f"Error retrieving store {store_number}: {response.status_code}")
        
        # Convert the list of store data into a pandas DataFrame
        stores_df = pd.DataFrame(stores_data)
        
        return stores_df
    
    def extract_from_s3(self, s3_url):
        # Parse the S3 URL to extract the bucket name and object key
        s3_parts = s3_url.replace("s3://", "").split("/")
        bucket_name = s3_parts[0]
        object_key = "/".join(s3_parts[1:])

        # Download the CSV file from S3
        obj = self.s3_client.get_object(Bucket=bucket_name, Key=object_key)

        # Get the file content from the response
        file_content = obj['Body'].read().decode('utf-8')

        # Read the CSV data into a pandas DataFrame
        data = pd.read_csv(StringIO(file_content))

        return data
    

    def retrieve_date_details(self, retrieve_endpoint, number_of_dates=1) -> pd.DataFrame:
        # Initialize an empty list to hold date details
        data = []
        
        # Loop through each store number and get its details
        for store_number in range(1, number_of_dates + 1):
            # Construct the URL for the store
            url = retrieve_endpoint.format(store_number=store_number)
            
            # Make a GET request to retrieve store data
            response = requests.get(url, headers=self.headers)
            
            if response.status_code == 200:
                # If the response is successful, convert the store data to a dictionary
                date_info = response.json()  # ✅ Store response JSON in a separate variable
                # data_info = pd.json_normalize(date_info)
                # if isinstance(date_info, dict):
                #     data.append(date_info)  # ✅ Append the dictionary to the list
                # elif isinstance(date_info, list):
                #     data.extend(date_info)  # ✅ If response is a list, extend instead of append
            else:
                print(f"Error retrieving date: {response.status_code}")
        
        # Convert the list of date details into a pandas DataFrame
        date_details_df = pd.DataFrame(date_info)
        
        return date_details_df

    def list_number_of_data(self, number_endpoint, headers):
            try:
                # Send a GET request to retrieve the number of stores
                response = requests.get(number_endpoint, headers=headers)
                
                # Check if the request was successful (HTTP status 200)
                if response.status_code == 200:
                    # Parse the JSON response and return the number of stores
                    data = response.json()
                    return len(data)
                    # return data.get('number_of_stores', 0)  # Assuming the key is 'number_of_stores'
                else:
                    # If the response code is not 200, raise an exception
                    response.raise_for_status()
            except requests.exceptions.RequestException as e:
                print(f"Error occurred: {e}")
                return None

