import mechanicalsoup
import redis
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers
from neo4j import GraphDatabase

class Neo4JConnector:
    def __init__(self, uri, user, password):
        self._driver = GraphDatabase.driver(uri, auth=(user, password))
        
    def close(self):
        self._driver.close()

    def print_greeting(self, message):
        with self._driver.session() as session:
            greeting = session.execute_write(self._create_and_return_greeting, message)
            print(greeting)
            
    def add_links(self, page, links):
        with self._driver.session() as session:
            session.execute_write(self._create_links, page, links)
    
    @staticmethod
    def _create_and_return_greeting(tx, message):
        result = tx.run("CREATE (a:Greeting) " 
                        "SET a.message = $message " 
                        "RETURN a.message + ', from node ' + id(a)", message=message) 
        return result.single()[0]
    
    @staticmethod
    def _create_links(tx, page, links):
        for link in links:
            tx.run("CREATE (:Page { url: $link }) -[:LINKS_TO]-> (:Page { url: $page })", link=str(link), page=str(page))
        

neo4j_connector = Neo4JConnector("bolt://127.0.0.1:7689", "neo4j", "db_is_awesom3")
# connector.print_greeting("Hello, World!")
# neo4j_connector.add_links(page, links)

def write_to_elastic(es, url, html):
    es.index(index="webcrawler-index", body={"html": html})
    
    
def crawl(browser, r, es, url, neo4j_connector):
    print("Downloading page...")
    browser.open(url)

    a_tags = browser.page.find_all('a')
    hrefs = [link.get('href') for link in a_tags]
    
    rawHTMLstring = str(browser.page)
    write_to_elastic(es, url, rawHTMLstring)

    wikipedia_domain = "https://en.wikipedia.org"
    print('Parsing webpage for links...')
    links = [wikipedia_domain + href for href in hrefs if href and href.startswith("/wiki/")]

    print('Saving links to Redis...')
    r.lpush('links', *links)
    new_link = r.rpop
    print(new_link)
    
    neo4j_connector.add_links(url, links)


browser = mechanicalsoup.StatefulBrowser()
r = redis.Redis()
r.flushall()

ELASTIC_PASSWORD = "8vOlwiUOmMxYPLTWZS9djeMm"
CLOUD_ID = "94f867dd534743fdb83b68f61150408b:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQ1NGQxMDVmOWE0YTE0OTkwOTJkYTNlYjEwYWY2MDI5ZCQ1YjZiNmJiN2VjNjY0MDU1YjA2NTY1NzdlZTJiOWZkMw=="

es = Elasticsearch(
    cloud_id=CLOUD_ID,
    basic_auth=("elastic", ELASTIC_PASSWORD),
)
# es.indices.create(index="my-index")

start_url = "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", start_url)
# r.lpush("links", start_url)
# r.lpush("links", start_url)

idx = 0
while link := r.rpop('links'): 
    if "jesus" in str(link).lower():
        print("Found Jesus!")
        break
    crawl(browser, r, es, link, neo4j_connector)
    # write_to_elastic(es, idx, "rawHTMLstring")
    idx += 1
    
    
neo4j_connector.close()
