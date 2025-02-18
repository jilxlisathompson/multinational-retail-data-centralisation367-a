# Multinational Retail Data Centralisation 
## Project Description 
AIM: 

The aim of this project is to leverage data analysis and SQL queries to provide key insights into the business’s operations, sales, and performance across various dimensions. The project focuses on the following objectives:
  - Operational Scope: Determine the number of physical stores across countries and identify the country with the most stores to understand the geographical spread of the business.
  - Sales Performance by Location: Identify locations with the highest concentration of stores and examine which sales channels (online or offline) are generating the most sales to guide marketing and operational strategies.
  - Revenue Generation: Calculate the total sales by month and store type, identifying trends and highlighting the most successful months or store formats. This helps prioritise resources and sales efforts in high-performing locations or times.
  - Staffing Insights: Assess the staffing distribution across countries, which can inform decisions regarding workforce allocation, training, and support, especially in high-performing regions.
  - Sales Efficiency: Analyse the time taken for sales transactions, which provides a metric for evaluating operational efficiency and helps optimise processes for faster sales conversion.

By answering these questions, the project will help the business optimise its store strategy, improve online and offline sales efforts, and make data-driven decisions that align with its goals for growth and customer engagement.

## Project Utils 
### 1. Data Extraction

  DataExtractor is a Python utility class designed for extracting data from various sources, including relational databases, PDFs, APIs, and AWS S3 buckets. It provides methods for retrieving and processing data, returning it in a standardised Pandas DataFrame format for easy analysis and manipulation.

#### Features:
Extract Data from RDS: Retrieve tables from a relational database and convert them into Pandas DataFrames.
Extract Data from PDFs: Download a PDF from a URL and extract tabular data using pdfplumber.
Retrieve Store Data: Fetch store information from an API by querying an endpoint and parsing the response.
Extract Data from S3: Pull CSV data directly from an S3 bucket.
Retrieve Event Data: Fetch event date details from a specified API.

### 3. Database Cleaning
The DataCleaning class is a utility for cleaning and processing data in pandas DataFrames. It provides several methods to clean data from different sources, including user data, card details, store data, products, orders, and date details. Each method is tailored to handle common cleaning tasks such as handling missing values, removing duplicates, converting data types, and performing specific data transformations.

#### Features:
Clean User Data: Replaces "NULL" strings with NaN, removes rows with missing values, and ensures proper date formatting.
Clean Card Data: Handles "NULL" values, removes duplicates, and processes non-numeric card numbers.
Clean Store Data: Cleans store details, converts date columns, and processes staff numbers.
Convert Product Weights: Converts weights from grams (g) or milliliters (ml) to kilograms (kg).
Clean Products Data: Replaces "NULL" values, removes rows with missing values, and converts product weight.
Clean Orders Data: Removes unwanted columns and ensures the correct number of rows after cleaning.
Clean Date Details: Processes date-related data by converting day, month, and year columns to numeric values.

### 5. Database Utils
The DatabaseConnector class is designed to facilitate seamless interaction with relational databases. It provides functionalities to:
  - Connect to the database using credentials stored in a YAML file
  - List all tables in the database
  - Upload data from a pandas DataFrame to the database

#### Features:
Flexible Credential Management: You can provide the path to a YAML credentials file or opt for default credentials.
Database Table Listing: Easily list all tables in the connected database using list_db_tables.
Data Upload: Upload data from a pandas DataFrame to a specified database table using upload_to_db.

### 7. Database Schema
The DatabaseManager class handles database connections and performs data type alterations on various tables. The methods implemented in this class assist in updating data types to reflect the required schema, handle data clean-up (such as removing unwanted characters), and perform column transformations to ensure optimal data integrity.

#### Key Functions:
connect(): Establishes a connection to the PostgreSQL database using the provided configuration dictionary.
close(): Closes the cursor and connection to the database, ensuring that all resources are released.
get_max_length(column_name: str, table_name: str): Calculates the maximum length of values in a specified column, ensuring that VARCHAR columns are properly sized.
alter_orders_table(): Alters the orders_table to cast columns to the correct data types as specified.
alter_dim_users(): Alters the dim_users table to ensure the correct data types for columns like first_name, last_name, and date_of_birth.
alter_dim_store_details(): Merges latitude columns and updates the data types for various columns in the dim_store_details table.
alter_dim_products(): Cleans up the dim_products table by removing unwanted characters (like the £ sign) and adding a weight_class column based on predefined weight ranges.
alter_dim_products2(): Renames columns and alters the data types of the columns in the dim_products table to ensure they conform to the required types.
alter_dim_date_times_table(): Alters the dim_date_times table to adjust the data types of date-related columns.

## Installation instructions
To get started with the project, follow these steps to install all the required dependencies:

1. **Clone the repository** to your local machine:

   ```bash
   git clone https://github.com/yourusername/your-repository.git
   cd your-repository



## Usage instructions
### Data Extractor
To extract data from an RDS table:

```python
extractor = DataExtractor(credential_file='login.yaml')
df = extractor.read_rds_table('your_table_name')

### Data Cleaning

- Initialise with a DataFrame
```python
cleaner = DataCleaning(data_df)

- Clean user data
```python
cleaned_df = cleaner.clean_user_data()

- Clean card data
```python
cleaned_card_data = cleaner.clean_card_data(data_df)

- Convert product weights to kg
```python
cleaned_products = cleaner.clean_products_data(products_df)

## What I have learned

1. Data Extraction Techniques:

  Learned how to extract data from multiple sources, including:
  - Relational databases (RDS): Using SQL queries and connecting to databases to retrieve data in a structured format (Pandas DataFrame).
  - PDFs: Leveraging pdfplumber to extract tabular data from PDFs and process it for analysis.
  - APIs: Fetching data from REST APIs, handling JSON responses, and parsing them into useful formats.
  - AWS S3: Using boto3 to pull CSV data directly from S3 buckets for easy analysis.
  - Event data from APIs: Extracting specific event-related data through API calls and processing the results.

2. Working with Pandas DataFrames:

  - Gained experience in transforming raw data into Pandas DataFrames, making it easier to manipulate, clean, and analye.
    Improved skills in handling different data formats (CSV, JSON, tabular) and converting them into a consistent DataFrame structure.

3. API Integration and Parsing:
  - Improved understanding of making API requests with requests library and handling responses.
  - Familiarised with parsing JSON data, and how to extract, clean, and structure it for further processing or storage.

4. Database Interactions:
  - Strengthened knowledge of connecting to and querying relational databases (PostgreSQL, MySQL, etc.) using SQLAlchemy and psycopg2.
  - Gained hands-on experience in executing SQL commands directly from Python and retrieving data in a usable format.

5. AWS S3 and Cloud Integration:
  - Learned how to integrate Python with AWS S3 to pull data from cloud storage, specifically working with boto3.
  - Gained insights into working with cloud storage and handling large datasets from external sources.

6. Error Handling and Data Validation:
  - Practiced implementing error-handling mechanisms for dealing with failures during data extraction, such as connection issues, missing data, or malformed responses.
  - Developed skills for validating data before processing to ensure consistency and avoid errors during analysis.

7. Testing and Automation:
  - Created unit tests to ensure that data extraction methods work correctly, validating that the extraction processes handle edge cases (empty data, invalid formats, etc.).
  - Gained knowledge in automating the data extraction processes to save time and improve efficiency.

8. Efficient Code Structure and Reusability:
  - Learned how to structure Python classes and methods to be modular, reusable, and easy to extend in future projects.
  - Developed utility classes that abstract away the complexity of data extraction, making it easier to handle different data sources with minimal changes to the main application code.

9. Documentation and Readability:
  - Improved skills in documenting Python classes and methods for clarity, making code more understandable for collaborators or users of the repository.
  - Learned the importance of clear, well-organised code documentation to support open-source projects or team collaborations.

10. Best Practices in Data Engineering:
  - Gained practical experience in following best practices for data extraction, including using reliable libraries, managing dependencies, and handling errors effectively.
  - Enhanced understanding of the flow of data from different sources to processing environments, preparing for more complex data engineering tasks.

## File structure of the project
multinational-retail-data-centralisation367
|-> data_extraction.py
|-> data_cleaning.py
|-> database_utils.py
|-> database_schema.py
|-> test_data_cleaning.py
|-> requirements.txt

## License information
