# private-residential-property-transaction
Retrieving data from URA using their publicly available API

## Overview:
In this project, I retrieve private property transactions data from URA and store them in MongoDB, which can be used downstream for analysis purposes.
MongoDB, a NoSQL database, was chosen since the URA data is retrieved as a list of dictionaries.

Below, I outline the project folder structure and relevant python scripts to maintain the data in MongoDB.
I also introduce some data engineering concepts and best practices to consolidate my learning from a year working as a Data Engineer.

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

This folder stores 2 key python scripts:   
    
i. _mongo.py_  
Contains the MongoDB_Manager class with methods to connect to MongoDB databases and collections, and perform CRUD operations.
 
ii. _ura_utils.py_  
Contains functions to handle the retrieval of data from URA, filter the dataset by districts and format them to be stored in MongoDB.

5. **ura_refresh.py**  

This script executes the methods in utils to retrieve ura data in batches and immediately store them in a raw layer database.
Note that batches here refers to the way URA pre-partitioned their data into 4 sets, which will be retrieved individually.
No transformation is done at this layer, so that the data can serve as a source of truth, to reflect exactly what URA provides.

6. **load_district_data.py**  
  
This script runs subsequently after ura_refresh, by retrieving the raw layer data from MongoDB in batches, filtering by district and inserting into MongoDB by postal district and type of property (Landed vs non-Landed).

### Notes
This project is still in its early stages; I have designed a simple MVP for the ETL process, which will no doubt have many improvements over time. I also plan to explore analytical use cases for the data, such as simple statistics and trending, and possibly advanced analytics such as predictions, creation of housing price indices, etc.

Lastly, I plan to pick up some front-end skills which could steer this project into an interface where users can interactively create dashboards for comparisons between multiple project transactions, and more. Also, will definitely not forget to test, be it unit testing or ETL testing.
