"""
This file contains the crawler for the Bloons TD 6 wiki. It will crawl the wiki and extract the tower data from the tables.
It will then navigate to each tower's page and extract the upgrade data.

The data will be stored in an elasticsearch database.  

All of the primary, military, magic, and support towers will be extracted from the wiki and stored in the database.
"""

import mechanicalsoup
import pandas as pd
import numpy as np
import redis
from elasticsearch import Elasticsearch, helpers
import warnings

# ignore deprecation warnings
warnings.filterwarnings("ignore")

def write_to_elastic(es, index, df):
    """
    Writes a dataframe to an elasticsearch index.
    """
    es.index(index=index, body=df.to_dict(orient='records'))
    

def get_tower_data(browser, redis, elasticsearch, url):
    print("Downloading tower page...")
    browser.open(url)
    
    tables = browser.get_current_page().find_all('table')
    upgrade_tables = tables[1:16]
    
    #TODO : write to the db
    
    

def scrape_towers(browser, r, es, url):
    print("Downloading main page...")
    browser.open(url)
    
    tables = browser.get_current_page().find_all('table')

    # Get the primary towers table
    primary_towers = tables[1]
    primary_towers_df = pd.read_html(str(primary_towers))[0]
    primary_towers_df.columns = ['Name', 'Description', 'Cost']
    # print(primary_towers_df)

    # Get the military towers table
    military_towers = tables[2]
    military_towers_df = pd.read_html(str(military_towers))[0]
    military_towers_df.columns = ['Name', 'Description', 'Cost']
    # print(military_towers_df)

    # Get the magic towers table
    magic_towers = tables[3]
    magic_towers_df = pd.read_html(str(magic_towers))[0]
    magic_towers_df.columns = ['Name', 'Description', 'Cost']
    # print(magic_towers_df)

    # Get the support towers table
    support_towers = tables[4]
    support_towers_df = pd.read_html(str(support_towers))[0]
    support_towers_df.columns = ['Name', 'Image', 'url' 'Description', 'Cost']
    # print(support_towers_df)
    
    get_tower_data(browser, r, es, "https://bloons.fandom.com/wiki/Engineer_Monkey_(BTD6)")
    


# Get the url for the wiki and open it with mechanicalsoup
url = "https://bloons.fandom.com/wiki/Bloons_TD_6"
browser = mechanicalsoup.StatefulBrowser()
r = redis.Redis()
r.flushall()

# Create an elasticsearch instance and connect to the database
ELASTIC_PASSWORD = "8vOlwiUOmMxYPLTWZS9djeMm"
CLOUD_ID = "94f867dd534743fdb83b68f61150408b:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ1NGQxMDVmOWE0YTE0OTkwOTJkYTNlYjEwYWY2MDI5ZCQ1YjZiNmJiN2VjNjY0MDU1YjA2NTY1NzdlZTJiOWZkMw=="

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic", ELASTIC_PASSWORD),
)

scrape_towers(browser, r, es, url)




    




