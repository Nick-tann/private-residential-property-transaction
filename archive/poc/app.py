import configparser
import json
import numpy as np
import pandas as pd
from datetime import datetime
from typing import Dict
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from contextlib import contextmanager
from dotenv import dotenv_values
from flask import Flask, jsonify, request, render_template
from pymongo import MongoClient
import utils.ura_utils as utils
import utils.mongo as mongo
from db import SqliteManager

#Setup parameters
std_database_name = "district_data"

#Initate flask
app = Flask(__name__)

#Parameters
sql_file_name = 'enriched_schema'
db_name = 'enriched.db'
enriched_table_name = 'property_transaction'

district_no = 2
project_name = "spottiswoode suites"

# API endpoints
@app.route('/')
def index():
    return render_template("index.html")


@app.route("/get_district_project_data/", methods=["GET"])
def get_data():
    district_no = request.args.get("district_no")
    project_name = request.args.get("project_name")
    #Retrieve the configuration of ura data
    with open("mappings/enriched_data_mappings.json","r") as f:
        enriched_map = json.load(f)
    with SqliteManager(db_name) as enriched_db:
        filtered_data = enriched_db.query_table(enriched_table_name, enriched_map, district_no, project_name, True)
    return jsonify(filtered_data)



if __name__ == "__main__":
    app.run(host = "0.0.0.0", port = 8080, debug=True)