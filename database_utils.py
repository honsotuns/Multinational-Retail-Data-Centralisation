from sqlalchemy import create_engine
import psycopg2
import yaml
import pandas as pd
from sqlalchemy import inspect
from sqlalchemy import text






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


   
    def upload_to_db(self, df, table_name ,engine):
        #engine = DatabaseConnector.init_db_engine(self)
        df.to_sql(table_name, engine, if_exists='append')
        
        

    
       
        
    

        
database_connect = DatabaseConnector()
credential = database_connect.read_db_credentials()
engine = database_connect.init_db_engine()
  
      
       






