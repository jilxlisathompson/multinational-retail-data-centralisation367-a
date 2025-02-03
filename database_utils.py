import yaml
import pandas as pd
from sqlalchemy import create_engine, inspect


class DatabaseConnector:
    """
    To connect with and upload data to the database. 

    Attributes:
        credential_file (int): file containing the YAML credentials 
        # TODO rewrite the attributes of the class so that when it 
        # instantiated credential_file is optional
    """
    def __init__(self, credential_file=0):
        """
        Initialize the DatabaseConnector with the path to the credentials file.
        """
        self.credential_file = credential_file

    def read_db_creds(self) -> None:
        """
        Reads the database credentials from a YAML file and 
        returns them as a dictionary.
        Args:

        Return:
            dict of YAML credentials  

        """
        try:
            with open(self.credential_file, 'r') as file:
                creds = yaml.safe_load(file)
            return creds
        except FileNotFoundError:
            print(f"Error: The file {self.credential_file} was not found.")
            return None
        except yaml.YAMLError as e:
            print(f"Error parsing the YAML file: {e}")
            return None

    
    def list_db_tables(self):
        """
        Lists all the tables in the database.
        Args: 
        Return:
            prints the names of the tables in the database
        """
        engine = self.init_db_engine()
        if not engine:
            print("Error: Could not initialize database engine.")
            return None

        try:
            # Use SQLAlchemy's inspect method to get the list of tables
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            print("Tables in the database:", tables)
            return tables
        except Exception as e:
            print(f"Error retrieving table list: {e}")
            return None
        
    def init_db_engine(self):
        """
        Initialises and returns the SQLAlchemy engine using 
        credentials from the YAML file.
        Args:
        Return:
            SQLAlchemy engine 
        """
        creds = self.read_db_creds()  # Get credentials
        if not creds:
            print("Error: Could not read credentials.")
            return None

        try:
            user = creds['RDS_USER']
            password = creds['RDS_PASSWORD']
            host = creds['RDS_HOST']
            port = creds['RDS_PORT']
            dbname = creds['RDS_DATABASE']
            
            # Construct the database URL
            db_url = f"postgresql://{user}:{password}@{host}:{port}/{dbname}"
            
            # Initialize and return the SQLAlchemy engine
            engine = create_engine(db_url)
            return engine
        except KeyError as e:
            print(f"Error: Missing key in credentials file - {e}")
            return None
        
    def upload_to_db(self,  database_path, data_df, table_name):
        """
        Extract, clean, and upload data to the database.

        Args:
        - table_name: Name of the table to extract data from.
        - database_path: Path to the SQLite database.

        Workflow:
        1. Extract data from the RDS database.
        2. Clean the data.
        3. Store it in a local SQLite database in a table called 'dim_users'.
        """
        try:
            # Ensure clean_user_data is implemented correctly
            # cleaned_data_df = DataCleaning.clean_user_data(data_df)
            

            # Store the data in the SQLite table 'dim_users'
            # engine = create_engine(f"postgresql://{database_path}")
            engine = create_engine("postgresql://postgres:@localhost:5432/sales_data")
            print("Here in upload_db printing engine")
            print(engine)
            data_df.to_sql(
                name=table_name,      # Name of the SQL table
                con=engine,            # Connection engine
                if_exists="replace",   # Options: 'fail', 'replace', 'append'
                index=False            # Prevent pandas from adding the DataFrame index as a column
            )

            print("DataFrame successfully stored in the database!")
        except Exception as e:
            print(f"Error during upload process: {e}")

