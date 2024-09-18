import os
import sqlite3
import logging
import json
import numpy as np
import pandas as pd
from datetime import datetime
import utils.ura_utils as utils
from db import SqliteManager


###### CONFIG AND PARAMS ######

#Parameters
sql_file_name = 'enriched_schema'
db_name = 'enriched.db'
enriched_table_name = 'property_transaction'

###### START ######

#Setup logging
logger = logging.getLogger(__name__)
utils.set_logger()

#Retrieve the configuration of ura data
with open("mappings/enriched_data_mappings.json","r") as f:
    enriched_map = json.load(f)

enriched_columns_str = ','.join(list(enriched_map.keys()))

trxn_cnt_datetime_sql_statement = "SELECT CONCAT(contract_year,'-Q',contract_quarter) AS contract_quarter_year,COUNT(*) AS transaction_count \
FROM property_transaction \
GROUP BY contract_year,contract_quarter \
ORDER BY contract_year, contract_quarter"

trxn_cnt_district_sql_statement = "SELECT district,COUNT(*) AS transaction_count \
FROM property_transaction \
GROUP BY district \
ORDER BY district"

with SqliteManager(db_name) as enriched_db:
    trxn_cnt_by_datetime_df = enriched_db.query_to_df(trxn_cnt_datetime_sql_statement)
    trxn_cnt_by_district_df = enriched_db.query_to_df(trxn_cnt_district_sql_statement)


trxn_cnt_by_datetime_df.to_csv("data/trxn_cnt_by_datetime.csv")
trxn_cnt_by_district_df.to_csv("data/trxn_cnt_by_district.csv")
