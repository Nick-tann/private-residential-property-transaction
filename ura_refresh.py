import time
import configparser
import logging
import requests
from datetime import datetime
import utils.ura_utils as utils
from contextlib import contextmanager
from dotenv import dotenv_values
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
import utils.mongo as mongo

###### CONFIG AND PARAMS ######

#Set up config and credentials
ura_config = configparser.ConfigParser()
ura_config.read('config/ura_config.ini')

mongo_config = configparser.ConfigParser()
mongo_config.read('config/mongo_config.ini')

#Get credentials from env
username = dotenv_values("config/mongo_cred.env")['username']
password = dotenv_values("config/mongo_cred.env")['password']

#Create mongodb url string
db_url = mongo_config['URL']['urlPart1']+f"{username}:{password}"+mongo_config['URL']['urlPart2']

# Setup source db name
raw_database_name = "ura_data"

###### START ######

def main():
    logger = logging.getLogger(__name__)
    utils.set_logger()


    #Create mongo client
    mongodb_manager=mongo.MongodbManager(db_url,server_api=ServerApi('1'))

    #Connect to client
    client=mongodb_manager.create_connection()
    ura_db = mongodb_manager.connect_database(client,raw_database_name)

    #Get URA token
    payload={}
    with utils.timer("Retrieving Access Token"):
        headers=utils.get_token()

    #URA provides data in 4 batches
    batches = list(range(1,5))

    #For each batch, retrieve data and insert into mongodb.
    for batch in batches:
        batch=str(batch)
        url=ura_config['URL']['url']+batch

        response=utils.get_response(url=url,headers=headers,payload=payload)
        data=response.json()['Result']

        #If results are empty, raise an error
        if len(data)==0:
            raise Exception(f"Batch {batch} failed to download.")
        ## Write to respective mongo collection

        #Connect to collection
        collection_name=f"ura_private_prop_trans_{batch}"
        collection=mongodb_manager.connect_collection(ura_db,collection_name)

        # Delete collection docs
        mongodb_manager.delete_all_docs_in_collection(collection)

        #Insert new docs
        mongodb_manager.insert_many_docs(collection,data)


    #Add logging information

    # Set up connection to logging db
    curr_dt = datetime.now()
    log_database_name = "logging"
    log_collection_name = "refresh_logs"
    runid = mongodb_manager.count_docs(client,log_database_name,log_collection_name) + 1

    # Create BSON of 
    log_data = {
        "runid":runid,
        "created_date":curr_dt
        }


    mongodb_manager.add_logging_info(client,log_database_name,log_collection_name,log_data)
    logger.info("Loaded all batches into MongoDB.")

if __name__ == "__main__":
    main()
