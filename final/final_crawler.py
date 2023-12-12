"""
This file contains the crawler for the Bloons TD 6 wiki. It will crawl the wiki and extract the tower data from the tables.
It will then navigate to each tower's page and extract the upgrade data.

The data will be stored in an elasticsearch database.  

All of the primary, military, magic, and support towers will be extracted from the wiki and stored in the database.
"""

import mechanicalsoup
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers

# ignore deprecation warnings
import warnings
warnings.filterwarnings("ignore")
    

def get_tower_data(browser, es, url):
    print("Downloading tower page...")
    browser.open('https://bloons.fandom.com'+url)
    
    tables = browser.get_current_page().find_all('table')
    upgrade_tables = tables[1:16]
    for table in upgrade_tables:
        upgrade_df = pd.read_html(str(table))[0]
        upgrade_df = upgrade_df.dropna()
        
        if not upgrade_df.empty:
            name = upgrade_df.iloc[0, 0]
            description = upgrade_df.iloc[0, 1]
            cost = upgrade_df.iloc[0, 2]
            es.index(index="upgrades-index", body={"name": name, "description": description, "cost": cost})
    

def scrape_towers(browser, es, url):
    print("Downloading main page...")
    browser.open(url)
    
    tables = browser.get_current_page().find_all('table')
    tower_urls = []

    for idx in range(1, 5):
        tower_table = tables[idx]
        tower_df = pd.read_html(str(tower_table))[0]
        tower_urls.extend([img.get('href') for img in tower_table.find_all('a', href=True) if img.get('href').startswith('/wiki')])
        
        for jdx in range(5):
            tower = tower_df.iloc[jdx]
            name = tower.iloc[0]
            description = tower.iloc[1]
            cost = tower.iloc[2]
            es.index(index="tower-index", body={"Name": name, "Description": description, "Cost": cost})
    
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

scrape_towers(browser, es, url)




    




