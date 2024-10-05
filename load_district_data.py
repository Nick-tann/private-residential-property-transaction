import time
import json
import logging
import configparser
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from contextlib import contextmanager
from dotenv import dotenv_values
import utils.ura_utils as utils
import utils.mongo as mongo
from db import SqliteManager

###### CONFIG AND PARAMS ######

#Suppress chain assignment warning
pd.options.mode.chained_assignment = None

logger = logging.getLogger(__name__)
utils.set_logger()

mongo_config = configparser.ConfigParser()
mongo_config.read('config/mongo_config.ini')

#Get credentials from env
username = dotenv_values("config/mongo_cred.env")['username']
password = dotenv_values("config/mongo_cred.env")['password']

# Setup source/target db names
raw_database_name = "ura_data"
std_database_name = "district_data"

#Setup non-landed property type labels
non_landed_lst=['apartment','condominium']

#Setup columns list for which values should be lower case
lower_lst=['property_type','project']

#Retrieve the configuration of ura data
with open("mappings/ura_data_mappings.json","r") as f:
    ura_map=json.load(f)

#Setup mappings
ura_col_name_mappings=ura_map['column_name_map']
ura_col_type_mappings=ura_map['column_type_map']

#Create mongodb url string
db_url = mongo_config['URL']['urlPart1']+f"{username}:{password}"+mongo_config['URL']['urlPart2']

###### START ######

def main():
    #Create mongo client
    mongodb_manager=mongo.MongodbManager(db_url,server_api=ServerApi('1'))
    client=mongodb_manager.create_connection()

    #Connect to client
    ura_db = mongodb_manager.connect_database(client,raw_database_name)

    #Retrieve all data and parse into pandas dataframe
    full_data=[]
    for batch in list(range(1,5)):
        collection_name=f"ura_private_prop_trans_{str(batch)}"
        collection=mongodb_manager.connect_collection(ura_db,collection_name)

        #Retrieve all documents
        with utils.timer("Read all from collection."):
            data=mongodb_manager.read_all_from_collection(collection=collection)

        #Convert to dataframe
        df=pd.json_normalize(data,'transaction',['project','street','x','y','marketSegment'],errors='ignore')
        full_data.append(df)

    full_df=pd.concat(full_data)
    full_df.to_csv("full_data.csv")

    #Rename df
    full_df.rename(columns=ura_col_name_mappings,inplace=True)
    logger.info("Dataframe renamed.")

    #Convert df column types
    for col in full_df.columns:
        full_df[col]=full_df[col].astype(ura_col_type_mappings[col])
    logger.info("Data column types mapped.")


    #Drop duplicates
    full_df.drop_duplicates(keep= 'first', inplace= True, ignore_index= True)


    #Feature engineering
    full_df['contract_datetime']=pd.to_datetime(full_df['contract_date'],format="%m%y")
    full_df['price_psm'] = full_df['price']/full_df['area']
    full_df['contract_month']=full_df['contract_datetime'].dt.month
    full_df['contract_year']=full_df['contract_datetime'].dt.year
    full_df['contract_quarter']=np.ceil(full_df['contract_month']/3).astype(int)

    # Since day of contract is not available, we will keep contract_datetime in mm-yyyy format
    full_df['contract_datetime']=full_df['contract_datetime'].dt.strftime('%m-%Y')
    logger.info("Features created.")

    #Write data to enriched database
    property_df = full_df.drop('nett_price',axis = 1)
    property_columns_string = ','.join(property_df.columns)

    #Parameters
    sql_file_name = 'enriched_schema'
    db_name = 'enriched.db'
    enriched_table_name = 'property_transaction'

    #SQL statement
    sql_statement = f"SELECT {property_columns_string} FROM {enriched_table_name} LIMIT 10"

    with SqliteManager(db_name) as enriched_db:
        # enriched_db.create_table(sql_file_name,enriched_table_name)
        enriched_db.delete_data(enriched_table_name)
        enriched_db.insert_data(enriched_table_name,property_df)
        # property_df = enriched_db.retrieve_data(sql_statement)

    #Clear space
    del property_df

    #Set up connection to district database
    district_db = mongodb_manager.connect_database(client,std_database_name)

    # Loop through the districts
    for district_no in range(1,29):

        #Filter for specific district
        filtered_df = utils.filter_district(full_df,district_no)

        #Set property type labels to lower case
        filtered_df = utils.to_lower(filtered_df,lower_lst)

        #Filter non-landed property transactions
        non_landed_df=utils.filter_column(filtered_df,'property_type',non_landed_lst)

        #Get list of non-landed projects
        non_landed_project_lst=utils.get_unique_values(non_landed_df,'project')

        #Create dictionary with all non-landed projects as keys
        non_landed_json=dict()
        for proj in non_landed_project_lst:
            non_landed_json[proj]=non_landed_df[non_landed_df['project']==proj].values.tolist()

        #Filter landed property transactions
        landed_df=utils.filter_column(filtered_df,'property_type',non_landed_lst,is_in=False)

        #Get list of non-landed projects
        landed_project_lst=utils.get_unique_values(landed_df,'street')

        #Create dictionary with all landed streets as keys
        landed_json=dict()
        for street in landed_project_lst:
            landed_json[proj]=landed_df[landed_df['street']==street].values.tolist()

        #Prepare data format for inserting into mongodb
        district_data=[
            {
                'non_landed':non_landed_json
            },
            {
                'landed':landed_json
            }
        ]

        #Setup connection to std database collections
        collection_district_name = f"district_{district_no}"
        collection_district = mongodb_manager.connect_collection(district_db,collection_district_name)

        # Delete collection docs
        mongodb_manager.delete_all_docs_in_collection(collection_district)

        # Insert new docs
        mongodb_manager.insert_many_docs(collection_district,district_data)
        logger.info(f"Inserted transaction data for district {district_no} successfully.")

        #For better readability
        print("==================================================")

    #Add logging information

    # Set up connection to logging db
    curr_dt = datetime.now()
    log_database_name = "logging"
    log_collection_name = "district_logs"
    runid = mongodb_manager.count_docs(client,log_database_name,log_collection_name) + 1

    # Create BSON of 
    log_data = {
        "runid":runid,
        "created_date":curr_dt
        }

    mongodb_manager.add_logging_info(client,log_database_name,log_collection_name,log_data)

    logger.info("Inserted all district data successfully.")

if __name__ == "__main__":
    main()