# Multinational-Retail-Data-Centralisation
1 Environment setup
1.1 GitHub repo created

2 Extracted and cleaned the data from data sources
2.1 Setting up a new database on pgAdmin4 names Sales_Data
2.2 New python script was created named data_extraction.py and within it, a class DataExtractor was created to extract csv files, an API and S3 bucket files
2.3 Created a python script named database_utils.py and within it, a class DatabaseConnector was created to connet and upload data to the database
2.4 Created another script named data_cleaning and within it, class DataCleaning was created with methods to clean data from each of the data sources
2.5 Created db_cred.yaml file with given credentials inside DatabaseConnector then developed a method to extract the data from the database
2.6 init_bd_engine, read_rds_table were created
2.7 Uploaded to sales_data database using upload_to_db method called dim_users table
2.8 Created retrieve_pdf_data in DataExtractor class which uses tabula to extract the pages from a given link
2.9 Created clean_card_data inside DataCleaning class and the uploaded to a table called dim_card_details
2.10 Extracted data therough an API with given key and value
2.11  Created a method called list_number_of_stores inside class DataExtractor which returns the number of stores to extract
2.12 Created another method called retrieve_store_data which takes the retrieved store endpoint as an argument and extracts all the stores from the API, saving them in a pandas Dataframe
2.13 Another  method was created called called_clean_store_data in DataCleaning class, which cleanse the retrieved data from the API and returns a pandas Dataframe
2.14 Using upload_to_db method to send to database with table name dim_store_details
2.15 Extract and clean the product details
2.16 Extracting a csv format file from s3 bucket on AWS. Created a method in DataExtractor named extract_from_s3 to extract from a given s3 address. 
2.17 Created a method named convert_product_weights to convert weights in all other values to kg.
2.18 Anotheur method was created called clean_products_data which cleanse the returned dataframe and uploaded to the database using upload_to_db method with table name dim_products
2.19 Using  list_db_tables method, extracted the orders data using read_rds_table1 and returned a pandas dataframe
2.20 Created  a new method called clean_orders_data which cleanse  and removed some columns and the uploaded to the database using upload_to_db with table name orders_table
2.21 The final cource of data is a json file with given link stored on AWS s3. Cleanse and stored in the databse with table name dim_date_times

3 Creating a database schema (SQL)
3.1 Casted the columns of orders_table to the correct data types
3.2 Casted the columns of dim_users_table to the correct data types
3.3 Casted the columns of dim_store_details to the correct data types
3.4 In dim_products table, created a new columns called weight range in kg 
3.5 Casted the columns of dim_products to the correct data types and renaming the column "removed" as "still_available"
3.6 Casted the columns of dim_date_times to the correct data types
3.7 Casted the columns of dim_card_details to the correct data types
3.8 Created the primary keys in the dimension tables
3.9 Adding the foreign key to orders_table

4 Querying the data
4.1 Performing queries on how many stores does the business have and in which countries?
4.2 Whcih locations currently have the most stores?
4.3 Which month produce the average highest cost of sales typically?
4.4  How many sales are coming from online?
4.5 What percentage of sales comes through each type of store?
4.6 Which month in each year produced the highest cost of sales?
4.7 What is the staff headcount?
4.8 Which German store type is selling the most?
4.9 How quickly is the compant making sales?


