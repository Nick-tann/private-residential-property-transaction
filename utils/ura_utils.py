import sys
import json
import time
import pathlib
import logging
import logging.config
import requests
import configparser
import pandas as pd
from typing import Dict
from dotenv import dotenv_values
from contextlib import contextmanager
from urllib3.exceptions import InsecureRequestWarning

logger = logging.getLogger(__name__)
#Setup logger
def set_logger() -> None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(logging.INFO)
    formatter = logging.Formatter('[%(levelname)s] %(name)s - %(asctime)s : %(message)s',datefmt = "%Y-%m-%dT%H:%M:%S%z")
    handler.setFormatter(formatter)
    root_logger.addHandler(handler)
    return

# def set_logger() -> None:
#     logger.setLevel(logging.INFO)

#     handler = logging.StreamHandler(sys.stdout)
#     handler.setLevel(logging.INFO)
#     #%(name)s -
#     formatter = logging.Formatter('[%(levelname)s] %(asctime)s : %(message)s',datefmt = "%Y-%m-%dT%H:%M:%S%z")
#     handler.setFormatter(formatter)
#     logger.addHandler(handler)
#     return

#timing code run and printing title
@contextmanager
def timer(title:str):
    print("\n")
    logger.info(f"Starting task: {title}")
    start=time.time()
    yield
    end=time.time()
    logger.info(f"Task: {title} completed in {(end-start):.1f}s")

#Configure display size to display more columns and rows
@contextmanager
def df_display(title:str):
    print(f"Displaying: {title}")
    pd.options.display.max_columns=None
    pd.options.display.max_rows=None
    print("Set display max size to none.")
    yield
    pd.options.display.max_columns=20
    pd.options.display.max_rows=60
    print("Set display max size back to default.")


#Return response from get request
def get_response(url:str,headers:Dict,payload:Dict) -> requests.Response:
    
    # Suppress the warnings from urllib3
    requests.packages.urllib3.disable_warnings(category=InsecureRequestWarning)

    return requests.get(url=url,headers=headers,data=payload,verify=False)

#Retrieve token from response
def retrieve_token(token_response:requests.Response) -> str:
    if token_response.status_code==200:
        try:
            token= json.loads(token_response.content.decode('utf-8'))['Result']
            logger.info("API call successful.")
            return token
        except:
            logger.error("Status code <200> but result format wrong.")
            raise
    else:
        logger.error(f"API call failed with status code: {token_response.status_code}")
        raise

# High-level function to make a get request, retrieve token from the response and add it to a header
def get_token() ->Dict:
    config = configparser.ConfigParser()
    config.read('config/ura_config.ini')
    refresh_token_url = config['URL']['token_url']
    payload={}
    token_headers={
        "AccessKey":dotenv_values("config/ura_cred.env")['AccessKey'],
        "User-Agent":dotenv_values("config/ura_cred.env")['User-Agent']
    }
    token_response = get_response(refresh_token_url,token_headers,payload)
    token = retrieve_token(token_response)
    token_headers["Token"]=token
    return token_headers

# Get district number from location
def location_to_district(location:str,postal_mapping:Dict) -> int:
    postal_district = postal_mapping[location]
    logger.info(f"Location belongs to postal district {postal_district}.")
    return int(postal_district)

# def district_to_batch(postal_district,cutoff_lst):
#     if not postal_district or postal_district==0 or postal_district>28:
#         logging.error(f"Postal district {postal_district} not found.")
#     else:
#         ind=0
#         while postal_district>cutoff_lst[ind]:
#             ind+=1
#         ind+=1
#         logging.info(f"Location belongs to batch {ind}.")
#         return ind

#Filter df by district number 
def filter_district(df:pd.DataFrame,district:int)->pd.DataFrame:
    filtered_df=df.loc[df['district']==district]
    logger.info(f"There are {len(filtered_df)} transactions in district {str(district)}.")
    return filtered_df

#Filter df by values in a column
def filter_column(df:pd.DataFrame,column_name:str,val_lst:list,is_in:bool=True)->pd.DataFrame:
    if type(val_lst)!=list:
        raise TypeError("Values need to be a list.")
    if not is_in:
        return df[~df[column_name].isin(val_lst)]
    else:
        return df[df[column_name].isin(val_lst)]
    
#Get distinct values in a column
def get_unique_values(df:pd.DataFrame,column_name:str):
    return list(df[column_name].unique())

#Lower case the values of cols in list or single col
def to_lower(df:pd.DataFrame,cols:list)->pd.DataFrame:
    copy_df=df.copy()
    if type(cols)!=list:
        raise TypeError("Columns need to be a list.")
    for col in cols:
        copy_df[col]=copy_df[col].str.lower()
    return copy_df