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
        #db_creds = self.read_db_credentials()
        #engine = create_engine(f"{db_creds['RDS_DATABASE']}+{db_creds['RDS_DBAPI']}://{db_creds['RDS_USER']}:{db_creds['RDS_PASSWORD']}@{db_creds['RDS_HOST']}:{db_creds['RDS_PORT']}/{db_creds['RDS_DATABASE']}")
        engine = create_engine(f"{'postgresql'}+{'psycopg2'}://{'aicore_admin'}:{'AiCore2022'}@{'data-handling-project-readonly.cq2e8zno855e.eu-west-1.rds.amazonaws.com'}:{'5432'}/{'postgres'}")
        #print(engine)
        return engine 

    ''' This method(engine) is for bd_creds.yaml '''
   
    # def upload_to_db(self, df, table_name ,engine):
    #     #engine = DatabaseConnector.init_db_engine(self)
    #     df.to_sql(table_name, engine, if_exists='replace')

    ''' This method (engine) is for df_pdf'''

    # def upload_to_db(self, df, df_data, engine):
    #     df.to_sql(df_data, engine, if_exists='replace') 


    ''' This method (engine) is for df_api'''
    
    # def upload_to_db(self,df, df_api, engine):
    #     df.to_sql(df_api, engine, if_exists='replace') 

    ''' This method (engine) is for df_bucket'''

    def upload_to_db(self,df, df_bucket, engine):
        df.to_sql(df_bucket, engine, if_exists='replace') 

    # ''' This method (engine) is for orders_table'''
    # def upload_to_db(self,df, new_df_orders, engine):
    #     df.to_sql(new_df_orders, engine, if_exists='replace') 

    # ''' This method (engine) is for dim_date_time'''
    # def upload_to_db(self,df, sales_date_df, engine):
    #     df.to_sql(sales_date_df, engine, if_exists='replace') 
        

   




        
        
database_connect = DatabaseConnector()
# credential = database_connect.read_db_credentials()
# engine = database_connect.init_db_engine()
#database_connect.upload_to_db()


      
       






