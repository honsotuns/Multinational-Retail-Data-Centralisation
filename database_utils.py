import psycopg2
import yaml
import pandas as pd
from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy import create_engine
from botocore import UNSIGNED
from botocore.client import Config
import boto3 

'''  DatabaseConnector Class '''
class DatabaseConnector:
    def __init__(self):
      pass

    ''' This method loads the yaml file from db_creds.yaml '''  
    def read_db_credentials(self):
        with open('db_creds.yaml', 'r') as file:
            configuration = yaml.safe_load(file)
        return configuration
    
    ''' This method creates engine using AWS RDS '''
    def init_db_engine(self):
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'aicore_admin'}:{'AiCore2022'}@{'data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com'}:{'5432'}/{'postgres'}")
        return engine
           
    ''' This method sends bd_creds.yaml file to SQL '''
    def upload_to_db_as_dim_users(self, df_orders, table_name , engine_yaml):
        df_orders.to_sql(table_name, engine_yaml, if_exists='replace')

    ''' This method sends df_pdf file to SQL '''
    def upload_to_db_as_dim_card_details(self, df_pdf,table_name, engine_pdf):
        df_pdf.to_sql(table_name, engine_pdf, if_exists='replace') 

    ''' This method sends df_api file to SQL '''
    def upload_to_db_as_dim_store_details(self,df_api, table_name, engine_api):
        df_api.to_sql(table_name, engine_api, if_exists='replace')

    ''' This method sends df_bucket file to SQL '''
    def upload_to_db_as_dim_products(self,df_bucket, table_name, engine_bucket):
        df_bucket.to_sql(table_name, engine_bucket, if_exists='replace')

    ''' This method sends orders_table file to SQL '''
    def upload_to_db_as_orders_table(self, df_new_orders, table_name, engine_orders):
        df_new_orders.to_sql(table_name, engine_orders, if_exists='replace') 

    ''' This method sends sales_date file to SQL '''
    def upload_to_db_as_dim_date_times(self, sales_date_df, table_name, engine_sales):
        sales_date_df.to_sql(table_name, engine_sales, if_exists='replace')
    
    def run_methods(self):
        configuration = DatabaseConnector().read_db_credentials()
        DatabaseConnector().init_db_engine
    
        print(configuration)
if __name__ == '__main__':
     DatabaseConnector().run_methods()




        

