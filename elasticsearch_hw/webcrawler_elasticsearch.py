import mechanicalsoup
import redis
import pandas as pd
import numpy as np
from elasticsearch import Elasticsearch, helpers
import configparser
import logging
import os, sys


def write_to_elastic(es, url, html):
    # print("="*20)
    # print(url.decode(encoding="utf-8"))
    # print("="*20)
    # s = b'hello world'.decode(encoding="utf-8")
    es.index(index="webcrawler-index", body={"html": html})
    
    
    
def crawl(browser, r, es, url):
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


# tracer = logging.getLogger('elasticsearch')
# tracer.setLevel(logging.CRITICAL) # or desired level
# tracer.addHandler(logging.FileHandler('indexer.log'))
# tracer.removeHandler(sys.stderr)
# print(tracer)

start_url = "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", start_url)
# r.lpush("links", start_url)
# r.lpush("links", start_url)

idx = 0
while link := r.rpop('links'): 
    if "jesus" in str(link).lower():
        print("Found Jesus!")
        break
    crawl(browser, r, es, link)
    # write_to_elastic(es, idx, "rawHTMLstring")
    idx += 1
    




