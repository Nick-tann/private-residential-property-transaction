# private-residential-property-transaction
Retrieving data from URA using their publicly available API

## Overview:
In this project, I retrieve private property transactions data from URA and store them in MongoDB, which can be used downstream for analysis purposes.
MongoDB, a NoSQL database, was chosen since the URA data is retrieved as a list of dictionaries.

Below, I outline the project folder structure and relevant python scripts to maintain the data in MongoDB.
I also introduce some data engineering concepts and best practices to consolidate my learning from a year working as a Data Engineer.

## ETL Design  
< ura_refresh.py>  
The first consideration when performing ETL is storing data in its raw form from the source, to maintain a source of truth. To reduce any transformations of the source data, which is in the form of an array of dictionaries, I opted to use MongoDB, which can store the data as-is.

<load_district_data.py>  
Thereafter, the data can be further refined by feature engineering and type conversions to improve analytics capabilities. Data is stored in MongoDB by district to allow better and faster retrieval. It is also stored in an SQLite3 database for subsequent visualization and viewing.

<load_enriched_data.py>  
This step performs simple aggregation of the data to generate insights. (WIP)


### Project folder structure:
1. **config**  

This folder houses files containing credentials and access keys for connection to URA and MongoDB. The files are added to gitignore for obvious reasons.

2. **house-env**  

The virtual environment for this project, containing necessary dependencies.

3. **mappings**  

URA has kindly provided a list of postal districts table, mapping locations to their postal districts.
This folder contains a python script to ingest the pdf file of the table and create a dictionary mapping locations to the districts.
There are also json files containing mappings for the column names for the data retrieved from URA, as well as mappings for column data types.

4. **utils**  

This folder stores 3 key python scripts:   
    
i. _mongo.py_  
Contains the MongoDB_Manager class with methods to connect to MongoDB databases and collections, and perform CRUD operations.
 
ii. _ura_utils.py_  
Contains functions to handle the retrieval of data from URA, filter the dataset by districts and format them to be stored in MongoDB and SQLite3.

iii. _db.py_
Contains functions to connect to SQlite3 database and perform simple read/write functionality.

5. **ura_refresh.py**  

This script executes the methods in utils to retrieve ura data in batches and immediately store them in a raw layer database.
Note that batches here refers to the way URA pre-partitioned their data into 4 sets, which will be retrieved individually.
No transformation is done at this layer, so that the data can serve as a source of truth, to reflect exactly what URA provides.

6. **load_district_data.py**  
  
This script runs subsequently after ura_refresh, by retrieving the raw layer data from MongoDB in batches, filtering by district and inserting into MongoDB by postal district and type of property (Landed vs non-Landed). Also inserts dataset into SQLite database.

7. **load_enriched_data.py**

This script retrieves from SQLite and aggregates data, which can be visualized in app.py.

8. **app.py**

This script is a simple implementation of dash, a visualization tool by plotly. It is built on top of Flask, so some elements of Flask will be present as well, mainly for homepage.

### Notes
This project is still in its early stages; I have designed a simple MVP for the ETL process, which will no doubt have many improvements over time. I also plan to explore analytical use cases for the data, such as simple statistics and trending, and possibly advanced analytics such as predictions, creation of housing price indices, etc.