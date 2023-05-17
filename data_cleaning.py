
import psycopg2
import yaml
import pandas as pd
import numpy as np
import re
from sqlalchemy import inspect
from database_utils import DatabaseConnector
from data_extraction import DataExtractor
from sqlalchemy import create_engine




class DataCleaning:
      def __init__(self, table_name = 'legacy_users'):
            engine = DatabaseConnector.init_db_engine(self)
            self.df_orders = DataExtractor.read_rds_tables(self, table_name, engine)
            #self.new_df_orders = DataExtractor.read_rds_tables(self, table_name, engine) # Task 6
            self.df_pdf = DataExtractor.retrieve_pdf_data(self)
            self.df_api = DataExtractor.retrieve_stores_data(self, num_of_stores = 451) 
            self.df_bucket = DataExtractor.extract_from_s3(self)
            self.sales_date_df = DataExtractor.sales_date(self)

      def clean_user_data(self):
          self.df_orders = self.df_orders.dropna(how='any').dropna(how='any', axis=1) 
          self.df_orders.update(self.df_orders)
          self.df_orders = self.df_orders.reset_index(drop=True)
          self.df_orders.update(self.df_orders)
          return self.df_orders


      def standardise_phone_number(self):
          self.df_orders['phone_number'] = self.df_orders['phone_number'].astype('str')
          self.df_orders['phone_number'] = self.df_orders['phone_number'].str.replace(r'[^0-9]+', '')
          self.df_orders.update(self.df_orders)
          return self.df_orders
      
      def clean_card_data(self):     #####
          df_data = pd.concat(self.df_pdf, axis=1, ignore_index=False)
          df_data = df_data.replace('?','')
          df_data.update(df_data)
          df_data = df_data.dropna(how='any').dropna(how='any', axis=1) 
          df_data.update(df_data)
          return df_data
         
      def called_clean_store_data(self):
          #self.df_api.drop_duplicates()
          #self.df_api = self.df_api.dropna(how='all')
          #self.df_api = self.df_api.dropna(axis=1)
          #self.df_api.update(self.df_api)
          #return self.df_api
          #print(self.df_api.iloc[:,5].head(60))
          print(self.df_api.head(40))
      
          
      
      def convert_product_weights(self):  
          self.df_bucket['weights_in_kg'] = self.df_bucket['weight'].str.extract(r'(\d+.\d+)').astype('float')
          for page in self.df_bucket['weights_in_kg']:
            if 'x' in str(page):
                page = page.replace('x', '*') 
                page = page.replace(' ', '')
                page = eval(page)
            
          cells_to_divide = self.df_bucket['weight'].str.contains('kg',na=False)  
          self.df_bucket['weights_in_kg'].iloc[~cells_to_divide.values] = self.df_bucket['weights_in_kg'].iloc[~cells_to_divide.values].multiply(0.001)
          return self.df_bucket
          #print (self.df_bucket)
         

      def clean_products_data(self): 
          self.df_bucket = self.df_bucket.dropna(how='all')
          self.df_bucket['removed'] = self.df_bucket['removed'].astype('category')
          self.df_bucket['category'] = self.df_bucket['category'].astype('category')
          return self.df_bucket
          

      def clean_orders_data(self, table_name = 'orders_table'): # Task 6
          engine = DatabaseConnector.init_db_engine(self)
          self.new_df_orders = DataExtractor().read_rds_tables(table_name, engine)
          self.new_df_orders = self.new_df_orders.drop(["first_name", "last_name", "1", "index"], axis=1)
          self.new_df_orders = self.new_df_orders.dropna(how='any').dropna(how='any', axis=1) 
          return self.new_df_orders
      
      def cleaning_sales_date(self):
          self.sales_date_df['time_period'] = self.sales_date_df['time_period'].astype('category')
          self.sales_date_df = self.sales_date_df.dropna(how='any').dropna(how='any',axis=1)
          self.sales_date_df = self.sales_date_df.drop_duplicates()
          self.sales_date_df.update(self.sales_date_df)
          return self.sales_date_df
          
       
           
       
        
        
clean_data = DataCleaning()
#clean_data.clean_user_data()
#df_legacy = clean_data.standardise_phone_number()       

#engine = DatabaseConnector().init_db_engine() # engine for yaml
#df_pdf = DataExtractor().retrieve_pdf_data()
#df = DataCleaning().standardise_phone_number()

#print(engine)
#engine.connect()
# engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'postgres'}:{'###'}@{'localhost'}:{'5432'}/{'Sales_Data'}") # yaml
# DatabaseConnector().upload_to_db(df,'dim_users', engine)


# engine = DataExtractor().retrieve_pdf_data() # engine for pdf
# df = DataCleaning().clean_card_data()
# engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'postgres'}:{'###'}@{'localhost'}:{'5432'}/{'Sales_Data'}") #pdf
# DatabaseConnector().upload_to_db(df,'dim_card_details', engine)

# engine = DataExtractor().retrieve_stores_data(num_of_stores=451) # engine for df_api
# df = DataCleaning().called_clean_store_data()
# engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'postgres'}:{'###'}@{'localhost'}:{'5432'}/{'Sales_Data'}") #api
# DatabaseConnector().upload_to_db(df,'dim_store_details', engine)

# engine = DataExtractor().extract_from_s3() # engine for df_bucket
# df = DataCleaning().convert_product_weights()
# #df = DataCleaning().clean_products_data()
# engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'postgres'}:{'###'}@{'localhost'}:{'5432'}/{'Sales_Data'}") #bucket
# DatabaseConnector().upload_to_db(df,'dim_products', engine)

# engine = DatabaseConnector().init_db_engine()
# df = DataCleaning().clean_orders_data()
# engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'postgres'}:{'###'}@{'localhost'}:{'5432'}/{'Sales_Data'}") #new_df_orders. Task 6
# DatabaseConnector().upload_to_db(df,'orders_table', engine)

# engine = DataExtractor().sales_date()
# df = DataCleaning().cleaning_sales_date()
# engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'postgres'}:{'###'}@{'localhost'}:{'5432'}/{'Sales_Data'}") #sales_date
# DatabaseConnector().upload_to_db(df,'dim_date_times', engine)

#clean_data.clean_card_data()
clean_data.called_clean_store_data()

#df_bucket = clean_data.convert_product_weights()
# clean_data.clean_products_data()   
#clean_data.clean_orders_data()
#clean_data.cleaning_sales_date()
