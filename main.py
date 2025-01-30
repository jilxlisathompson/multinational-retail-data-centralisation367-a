import database_utils
import os 
import data_extraction, data_cleaning

def main():
    # checking directory contents
    print("Hello, Welcome")
    print(os.listdir())

    # extractdata = database_utils.DatabaseConnector('login.yaml')
    data_extractor = data_extraction.DataExtractor('login.yaml')
    db_connector = database_utils.DatabaseConnector('login.yaml')

    #     # Continue with your logic
    tables = db_connector.list_db_tables()
    print("Tables")
    print(tables)

    # # Read a specific table into a pandas DataFrame
    # table_name = 'legacy_users'  # Example table name
    # legacy_users_df = data_extractor.read_rds_table(table_name)
    # print(legacy_users_df.columns)

    # # testing cleaning data 
    # data_to_clean = data_cleaning.DataCleaning(legacy_users_df)
    # cleaned_df = data_to_clean.clean_user_data()
    # table_name_new = "dim_users"
    # # upload data
    database_path = '/Users/eh19686/Documents/Bootcamps/AICore_Bootcamp/data_engineering/learning_sql/first_db/my_db/sales_data.db'
    # # db_connector.upload_to_db(database_path= database_path, data_df=cleaned_df, table_name=table_name_new)


    # # # testing extracting data url from data_extraction
    # data_url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf'
    # data_df = data_extraction.DataExtractor.retrieve_pdf_data(db_connector, data_url)
    # data_to_clean = data_cleaning.DataCleaning(data_df)
    # cleaned_card_details = data_to_clean.clean_card_data(data_df=data_df)
    # table_name_card = "dim_card_details"
    # db_connector.upload_to_db(database_path= database_path, data_df=cleaned_card_details, table_name=table_name_card)

    # 
    headers = {
        "x-api-key": "yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX"
    }
    # Endpoint for number of stores
    number_stores_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores"
    
    # Instantiate the DataExtractor class and get the number of stores
    # extractor = data_extraction.DataExtractor()
    # number_of_stores = extractor.list_number_of_stores(number_stores_endpoint, headers)
    #     # Print the result
    # if number_of_stores is not None:
    #     print(f"Number of stores: {number_of_stores}")
    # else:
    #     print("Failed to retrieve the number of stores.")

    # 
    # Specify the retrieve store endpoint
    # retrieve_store_endpoint = "https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}"
    # data_extractor = data_extraction.DataExtractor(api_key='yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX')
    # # Call the method to retrieve all store data
    # stores_df = data_extractor.retrieve_stores_data(number_of_stores, retrieve_store_endpoint)
    # print(stores_df)
    # print(f"length of stores DataFrame: {len(stores_df)}")
    # data_to_clean = data_cleaning.DataCleaning(stores_df)
    # cleaned_store_details = data_to_clean.clean_store_data(stores_df)
    # print("Heres the cleaned data")
    # print(cleaned_store_details)
    # table_name_card = "dim_card_details"
    # db_connector = database_utils.DatabaseConnector()
    # db_connector.upload_to_db(database_path= database_path, data_df=cleaned_store_details, table_name=table_name_card)

    # 
    # data_extractor = data_extraction.DataExtractor()
    # s3_url = "s3://data-handling-public/products.csv"
    # s3_df = data_extractor.extract_from_s3(s3_url)

    # # Print the first few rows of the DataFrame
    # print(s3_df.head())

    # # Initialize the DataCleaning class and convert weights
    # data_to_clean = data_cleaning.DataCleaning(s3_df)
    # cleaned_products_df = data_to_clean.convert_product_weights(s3_df)
    # cleaned_products_df = data_to_clean.clean_products_data(cleaned_products_df)
    # # upload
    # table_name_card = "dim_products"
    # db_connector = database_utils.DatabaseConnector()
    # db_connector.upload_to_db(database_path= database_path, data_df=cleaned_products_df, table_name=table_name_card)



    # Display the cleaned DataFrame
    # print(cleaned_products_df)

    # Using the database table listing methods you created earlier list_db_tables, 
    # list all the tables in the database to get the name of 
    # the table containing all information about the product orders.
    # orders_table_name = 'orders_table'
    # orders_table_df = data_extractor.read_rds_table(orders_table_name)
    # data_to_clean = data_cleaning.DataCleaning(orders_table_df)
    # cleaned_orders_df = data_to_clean.clean_orders_data(data=orders_table_df)
    # table_name_card = "orders_table"
    # db_connector = database_utils.DatabaseConnector()
    # db_connector.upload_to_db(database_path= database_path, data_df=cleaned_orders_df, table_name=table_name_card)

    # Task 8: Retrieve and clean the dates event data
    extractor = data_extraction.DataExtractor()
    date_details_endpoint = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    number_of = extractor.list_number_of_data( date_details_endpoint, headers)
    print("number of ")
    print(number_of)
        # Print the result
    if number_of is not None:
        print(f"Number of _______: {number_of}")
    else:
        print("Failed to retrieve the number of _______.")

    date_details_df = data_extractor.retrieve_date_details( date_details_endpoint)
    print("here printing date_details df")
    print(date_details_df)
    data_to_clean = data_cleaning.DataCleaning(date_details_df)
    cleaned_df = data_to_clean.clean_date_details(date_details_df)
    print("here is cleaned data")
    print(cleaned_df)

    # print(f"length of stores DataFrame: {len(date_details_df)}")
    # data_to_clean = data_cleaning.DataCleaning(date_details_df)
    # cleaned_date_details = data_to_clean.clean_store_data(date_details_df)
    # print("Heres the cleaned data")
    # print(cleaned_date_details)
    table_name_card = "dim_date_times"
    db_connector = database_utils.DatabaseConnector()
    db_connector.upload_to_db(database_path= database_path, data_df=cleaned_df, table_name=table_name_card)
    




# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    main()
