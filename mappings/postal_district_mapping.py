import json
import logging
import pandas as pd
import tabula
from dataclasses import dataclass


file_path = "List_Of_Postal_Districts.pdf"

#Create a class to read in a pdf and extract the postal mapping table
@dataclass
class Pdf_Table:
    file_path:str

    #Read pdf
    def read_pdf(self):
        return tabula.read_pdf(file_path,pages=1)

    #Extract table in the form of pandas dataframe
    def extract_table(self,reader):
        return reader[0].drop(reader[0].index[:1])
    

#Location column contains multiple values seperated by a comma
#We will convert each value to a row with the corresponding postal district no.
def transform_location(df):
    if 'General Location' not in df.columns:
        logging.error("Column: General Location not found in df.")
    else:
        df['gen_loc'] = df['General Location'].str.split(',')
        postal_df = df[['Postal District','gen_loc']].copy()
        postal_df = postal_df.explode('gen_loc')
        postal_df['gen_loc'] = postal_df['gen_loc'].str.strip()
    return postal_df

#Create a mapping dictionary from the transformed dataframe
def postal_mappings(postal_df):
    if 'Postal District' not in postal_df.columns:
        logging.error("Column: General Location not found in df.")
    else:
        postal_dict={}
        for _, row in postal_df.iterrows():
            postal_dict[row["gen_loc"]]=str(int(row['Postal District']))
    return postal_dict

#Special case for Beach road which has (part) behind)
def special_case(mappings):
    mappings['Beach Road']="6"
    del mappings['Beach Road (part)']
    return mappings


#Start
pdf_tab=Pdf_Table(file_path)
reader=pdf_tab.read_pdf()
df = pdf_tab.extract_table(reader)
postal_df = transform_location(df)
post_mappings = postal_mappings(postal_df)
post_mappings = special_case(post_mappings)

with open("ura_data_mappings.json","r") as fr:
    ura_mapping = json.load(fr)
    ura_mapping["postal_mappings"]=post_mappings

with open("ura_data_mappings.json","w") as fw:
    json.dump(ura_mapping,fw,indent=1)
