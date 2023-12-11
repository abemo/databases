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
from espandas import Espandas
import warnings

# ignore deprecation warnings
warnings.filterwarnings("ignore")

def write_to_elastic(es, index, df):
    """
    Writes a dataframe to an elasticsearch index.
    """
    es.index(index=index, body=df.to_dict(orient='records'))
    

def get_tower_data(browser, es, esp, url):
    print("Downloading tower page...")
    browser.open(url)
    
    tables = browser.get_current_page().find_all('table')
    upgrade_tables = tables[1:16]
    
    tower_data = {}  # TODO
    write_to_elastic(es, index='tower_index', df=pd.DataFrame([tower_data]))
    
    

def scrape_towers(browser, es, esp, url):
    print("Downloading main page...")
    browser.open(url)
    
    tables = browser.get_current_page().find_all('table')
    image_paths = []
    tower_urls = []

    # Get the primary towers table
    primary_towers = tables[1]
    primary_towers_df = pd.read_html(str(primary_towers), extract_links="all")[0]
    # print(primary_towers_dict)
    primary_urls = primary_towers.find_all('a')
    primary_urls = [img.get('href') for img in primary_urls if img]
    primary_urls = [img for img in primary_urls if img.startswith('/wiki')]
    for url in primary_urls: 
        tower_urls.append(url)

    # Get the military towers table
    military_towers = tables[2]
    military_towers_df = pd.read_html(str(military_towers))[0]
    military_urls = military_towers.find_all('a')
    military_urls = [img.get('href') for img in military_urls if img]
    military_urls = [img for img in military_urls if img.startswith('/wiki')]
    for url in military_urls: 
        tower_urls.append(url)
    # print(military_towers_df)

    # Get the magic towers table
    magic_towers = tables[3]
    magic_towers_df = pd.read_html(str(magic_towers))[0]
    magic_urls = magic_towers.find_all('a')
    magic_urls = [img.get('href') for img in magic_urls if img]
    magic_urls = [img for img in magic_urls if img.startswith('/wiki')]
    for url in magic_urls: 
        tower_urls.append(url)
    # print(magic_towers_df)

    # Get the support towers table
    support_towers = tables[4]
    support_towers_df = pd.read_html(str(support_towers))[0]
    support_urls = support_towers.find_all('a')
    support_urls = [img.get('href') for img in support_urls if img]
    support_urls = [img for img in support_urls if img.startswith('/wiki')]
    for url in support_urls: 
        tower_urls.append(url)
    # print(support_towers_df) 
    
    # print(tower_urls)
    for tower_url in tower_urls:
        get_tower_data(browser, es, tower_url)
    


# Get the url for the wiki and open it with mechanicalsoup
url = "https://bloons.fandom.com/wiki/Bloons_TD_6"
browser = mechanicalsoup.StatefulBrowser()

# Create an elasticsearch instance and connect to the database
ELASTIC_PASSWORD = "8vOlwiUOmMxYPLTWZS9djeMm"
CLOUD_ID = "94f867dd534743fdb83b68f61150408b:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ1NGQxMDVmOWE0YTE0OTkwOTJkYTNlYjEwYWY2MDI5ZCQ1YjZiNmJiN2VjNjY0MDU1YjA2NTY1NzdlZTJiOWZkMw=="

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic", ELASTIC_PASSWORD),
)

esp = Espandas()

scrape_towers(browser, es, esp, url)




    




