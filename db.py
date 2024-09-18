import sqlite3
import logging
import numpy as np
import pandas as pd
from typing import Dict
from datetime import datetime
from dataclasses import dataclass
import utils.ura_utils as utils
from sqlalchemy import create_engine

#Configure logger and set parameters
logger = logging.getLogger(__name__)

@dataclass
class SqliteManager:
    file:str = 'enriched.db'
    _conn = sqlite3.connect(file, check_same_thread= False)
    _cursor = _conn.cursor()

    def __enter__(self):
        return self
    
    def __exit__(self, type, value, traceback):
        logger.info("Closing Connection")
        self.connection.close()

    @property
    def connection(self):
        return self._conn
    
    @property
    def cursor(self):
        return self._cursor

    def create_table(self,schema_file_name:str,table_name:str)->None:
        #validate sql file
        if not schema_file_name.endswith(".sql"):
            schema_file_name = schema_file_name + ".sql"

        #Read sql file and execute
        try:
            with open(schema_file_name,'r') as f:
                self.cursor.executescript(f.read())
            logger.info(f"Created table {table_name} successfully.")
        except Exception as e:
            logger.error(e)
    
    def insert_data(self,table_name:str,data:pd.DataFrame)->None:
        try:
            logger.info(f"Inserting {len(data)} records into {table_name}...")
            data.to_sql(table_name,self.connection, if_exists = 'append',index = False)
            logger.info("Inserted data successfully.")
        except Exception as e:
            logger.error(e)

    def execute_query(self, sql_statement:str)->None:
        try:
            self.cursor.execute(sql_statement)
        except Exception as e:
            logger.error(e)
    
    def retrieve_all_from_table(self, table_name:str, columns_string:str)->pd.DataFrame:
        try:
            sql_statement = f"SELECT {columns_string} FROM {table_name}"
            logger.info("Retrieving all data...")
            data = pd.read_sql_query(sql_statement, self.connection)
            logger.info(f"Retrieved {len(data)} records successfully.")
            return data
        except Exception as e:
            logger.error(e)

    def query_to_df(self, sql_statement:str)->pd.DataFrame:
        try:
            logger.info("Retrieving data...")
            data = pd.read_sql_query(sql_statement, self.connection)
            logger.info("Storing data as a pandas DataFrame.")
            return data
        except Exception as e:
            logger.error(e)

    def update_data(self,data:pd.DataFrame):
        # Based on URA API guidelines, 
        # it would be wise to refresh the entire dataset on a daily basis
        # instead of performing upsert.
        # Leaving this function as a placeholder should there be any updates.
        pass

    def delete_data(self,table_name:str)->None:
        logger.info(f"Deleting from table: {table_name}")
        sql_statement = f"DELETE FROM {table_name}"
        self.cursor.execute(sql_statement)
        logger.info(f"Deleted data from table successfully.")

    def query_table(self, table_name:str, columns_map:Dict, district_no:int, project_name:str, json_format:bool= False)->pd.DataFrame:
        
        cursor = self.cursor
        #validate district number input type
        try:
            district_no = int(district_no)
        except TypeError as e:
            logger.error("District number must be of type integer.")
        
        #For simplicity, just convert project name to upper case to match project names in db
        project_name = project_name.upper()
        params = (district_no,project_name)
        try:
            #Prepare columns to read, since select * is a cardinal sin.
            columns_string = ','.join(list(columns_map.keys()))

            #Query parameters so we avoid SQL injections and robbers cannot break into my house to defile me.
            sql_statement = f"SELECT {columns_string} FROM {table_name} WHERE district = ? and project = ?"
            cursor.execute(sql_statement,params)
            fetched_data = cursor.fetchall()
            if not json_format:
                data = pd.DataFrame(
                    fetched_data,
                    columns= list(columns_map.keys())
                    )
            else:
                data = fetched_data
            return data
        except Exception as e:
            logger.error(e)




