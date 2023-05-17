from sqlalchemy import create_engine
from sqlalchemy import inspect
from database_utils import DatabaseConnector
from tabula import read_pdf
import tabula
import numpy as np
import psycopg2
import yaml
import pandas as pd
import requests
import json
import time
import boto3 
import io
import os
from botocore import UNSIGNED
from botocore.client import Config



class DataExtractor: 
   def __init__(self):
      pass

   def list_db_tables(self, engine):
      inspector = inspect(engine)
      return inspector.get_table_names()
     

   def read_rds_tables(self, table_name, engine):
      table = pd.read_sql_table(table_name, engine)
      return table
   
   def retrieve_pdf_data(self):
      url = 'https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf' 
      df = tabula.io.read_pdf(url)#, pages='all')
      return df
   
   def list_number_of_stores(self):
     
      header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
      api_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
      num_of_stores = requests.get(api_url, headers = header_dict).json()
      return num_of_stores
 
   def retrieve_stores_data(self, num_of_stores):
      header_dict = {'x-api-key': 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'}
      retrieve_store_url = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/'
      store_data=[]  
      for page in range (num_of_stores):
         ext_all_stores = requests.get(retrieve_store_url + str(page),headers=header_dict).json()
         column_heading = ext_all_stores.keys()
         store_data.append(list(ext_all_stores.values()))
         
      df = pd.DataFrame((store_data),columns=column_heading)
      return df
      

   def extract_from_s3(self):
      s3 = boto3.client("s3", config=Config(signature_version=UNSIGNED))
      products_data = s3.get_object(Bucket='data-handling-public', Key='products.csv')
      products_df = pd.read_csv(products_data['Body'])
      return products_df
   
   def sales_date(self):
      date_data_url = 'http://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json'
      date_sales = requests.get(date_data_url).json()
      sales_date_df = pd.DataFrame(date_sales) 
      return sales_date_df

      
      
 

data_ext = DataExtractor()
engine_obj = DatabaseConnector()
# engine = engine_obj.init_db_engine()
# table_names = data_ext.list_db_tables(engine)
#print(table_names)
# # # df_orders = data_ext.read_rds_tables ('legacy_users',engine)
#  
#new_df_orders = data_ext.read_rds_tables('orders_table',engine) ### Task 6 orders_table
#print(new_df_orders) ## Task 6

# data_ext.retrieve_pdf_data()
#num_stores = data_ext.list_number_of_stores()
# num_of_stores = data_ext.list_number_of_stores()
#df_api = data_ext.retrieve_stores_data(num_of_stores = 451)
#df_bucket = data_ext.extract_from_s3()
#sales_date_df = data_ext.sales_date()




