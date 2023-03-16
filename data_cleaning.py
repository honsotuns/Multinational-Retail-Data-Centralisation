from sqlalchemy import create_engine
import psycopg2
import yaml
import pandas as pd
from sqlalchemy import inspect
from database_utils import DatabaseConnector
from data_extraction import DataExtractor



class DataCleaning:
      def __init__(self, table_name = 'legacy_users'):
            engine = DatabaseConnector.init_db_engine(self)
            self.df_orders = DataExtractor.read_rds_tables(self, table_name, engine)
     

      def clean_user_data(self):
          #print(self.df_orders.duplicated().sum())
          #print(self.df_orders.info())
          #print(df_orders.loc[:,'last_name'])
          self.df_orders = self.df_orders.dropna(how='any').dropna(how='any', axis=1) 
          self.df_orders.update(self.df_orders)
          #print(self.df_orders)
          
          self.df_orders = self.df_orders.reset_index(drop=True)
          #print(self.df_orders['index'])


      def standardise_phone_number(self):
          self.df_orders['phone_number'] = self.df_orders['phone_number'].astype('str')
          #print(df_orders.info())
          self.df_orders['phone_number'] = self.df_orders['phone_number'].str.replace(r'[^0-9]+', '')
          self.df_orders.update(self.df_orders)
          #print(self.df_orders)
          #print(self.df_orders['phone_number'])
          return self.df_orders

     
      

data_clean = DataCleaning()
data_clean.clean_user_data()
df_legacy = data_clean.standardise_phone_number()       
engine = DatabaseConnector().init_db_engine()
print(engine)
#engine.connect()
DatabaseConnector().upload_to_db(df_legacy,'dim_users', engine)
#print(df_legacy.head())
   

