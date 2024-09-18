# import os
import logging
import certifi
# import configparser
from dataclasses import dataclass
from typing import List,Optional,Dict
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pymongo.collection import Collection
from pymongo.database import Database
# from dotenv import dotenv_values

logger = logging.getLogger(__name__)
#Instantiate MongoDB class
@dataclass
class MongodbManager:
    url: str
    server_api:Optional[ServerApi] = None


    def create_connection(self) -> MongoClient:
        client = MongoClient(self.url,server_api = self.server_api,tlsCAFile=certifi.where())
        return client

    def connect_database(self,client:MongoClient,database_name:str) -> Database:
        return client[database_name]

    def test_client_connection(self,client:MongoClient) -> None:
        try:
            client.admin.command('ping')
            print("Pinged your deployment. You successfully connected to MongoDB!")
        except Exception as e:
            logger.error(e)
    

    def connect_collection(self,database:Database,collection_name:str) -> Collection:
        return database[collection_name]


    def insert_doc(self,collection:Collection,one_doc:Dict):
        try:
            collection.insert_one(one_doc)
            logger.info("Inserted one doc successfully.")
        except Exception as e:
            logger.error("Insert failed with error: ",e)


    def insert_many_docs(self,collection:Collection,docs:List):
        try:
            collection.insert_many(docs)
            logger.info(f"Inserted {len(docs)} docs successfully.")
        except Exception as e:
            logger.error("Insert many failed with error: ",e)


    def delete_all_docs_in_collection(self,collection:Collection):
        try:
            delete_result=collection.delete_many({})
            logger.info(f"Deleted {delete_result.deleted_count} docs in collection: {collection.name}")
        except Exception as e:
            logger.error(f"Delete all docs in {collection.name} failed.")


    def read_all_from_collection(self,collection:Collection,raw:bool=False):
        try:
            if raw:
                data=collection.find()
            else:
                data=list(collection.find())
                logger.info(f"Retrieved {len(data)} documents from {collection.name}.")
                return data
        except Exception as e:
            logger.error(f"Unable to read {collection.name} data.")
    
    # def read_from_district_collection(self,collection:Collection, project_name:str, non_landed:bool=True):
    #     try:
    #         if non_landed:
    #             data=collection.find()
    #         else:
    #             data=list(collection.find(project_name))
    #             logger.info(f"Retrieved {len(data)} documents from {collection.name}.")
    #             return data
    #     except Exception as e:
    #         logger.error(f"Unable to read {collection.name} data.")



    def count_docs(self,client:MongoClient,database_name:str,collection_name:str)->int:
        db = self.connect_database(client,database_name)
        collection = self.connect_collection(db,collection_name)
        cnt = collection.count_documents(filter = {})
        # logging.info(f"There are {cnt} documents in collection: {collection_name}.")
        return cnt


    def add_logging_info(self,client:MongoClient,log_database_name:str,collection_name:str,one_doc:Dict):
        log_db = self.connect_database(client,log_database_name)
        collection = self.connect_collection(log_db,collection_name)
        self.insert_doc(collection,one_doc)
        return

        
    ##Create new collection for logging
    ##Store update date and times for each batch
    ##New method to update one?
    ##method to retrieve update date and time

