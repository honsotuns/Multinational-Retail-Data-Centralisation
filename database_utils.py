import psycopg2
import yaml
import pandas as pd
from sqlalchemy import inspect
from sqlalchemy import text
from sqlalchemy import create_engine
from botocore import UNSIGNED
from botocore.client import Config
import boto3 


class DatabaseConnector:
    def __init__(self):
      pass
      
    def read_db_credentials(self):
        with open('db_creds.yaml', 'r') as file:
            configuration = yaml.safe_load(file)
        return configuration

    def init_db_engine(self):
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'aicore_admin'}:{'AiCore2022'}@{'data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com'}:{'5432'}/{'postgres'}")
        return engine
           
    ''' This method(engine) is for bd_creds.yaml '''
    def upload_to_db_as_dim_users(self, df_orders, table_name , engine_yaml):
        df_orders.to_sql(table_name, engine_yaml, if_exists='replace')

    ''' This method (engine) is for df_pdf'''
    def upload_to_db_as_dim_card_details(self, df_pdf,table_name, engine_pdf):
        df_pdf.to_sql(table_name, engine_pdf, if_exists='replace') 

    ''' This method (engine) is for df_api'''
    def upload_to_db_as_dim_store_details(self,df_api, table_name, engine_api):
        df_api.to_sql(table_name, engine_api, if_exists='replace')

    ''' This method (engine) is for df_bucket'''
    def upload_to_db_as_dim_products(self,df_bucket, table_name, engine_bucket):
        df_bucket.to_sql(table_name, engine_bucket, if_exists='replace')

    ''' This method (engine) is for orders_table'''
    def upload_to_db_as_orders_table(self, df_new_orders, table_name, engine_orders):
        df_new_orders.to_sql(table_name, engine_orders, if_exists='replace') 

    ''' This method (engine) is for sales_date'''
    def upload_to_db_as_dim_date_times(self, sales_date_df, table_name, engine_sales):
        sales_date_df.to_sql(table_name, engine_sales, if_exists='replace')

    def run_methods(self):
        configuration = DatabaseConnector().read_db_credentials()
        DatabaseConnector().init_db_engine
    
        print(configuration)
if __name__ == '__main__':
     DatabaseConnector().run_methods()




        

