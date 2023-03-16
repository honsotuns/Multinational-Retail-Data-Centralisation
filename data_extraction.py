from sqlalchemy import create_engine
import psycopg2
import yaml
import pandas as pd
from sqlalchemy import inspect
from database_utils import DatabaseConnector


class DataExtractor: 
   def __init__(self):
      pass

   def list_db_tables(self, engine):
      inspector = inspect(engine)
      return inspector.get_table_names()

   def read_rds_tables(self, table_name, engine):
      table = pd.read_sql_table(table_name, engine)
      return table
      

# data_ext = DataExtractor()
# engine_obj = DatabaseConnector()
# engine = engine_obj.init_db_engine()
# table_names = data_ext.list_db_tables(engine)
# print(table_names)
# df_orders = data_ext.read_rds_tables ('legacy_users',engine) 
# print(df_orders)
# data_ext.clean_user_data()
# data_ext.standardise_phone_number()




