import mechanicalsoup
import redis
import pandas as pd
import numpy as np

def crawl(browser, r, url):
    print("Downloading page...")
    browser.open(url)

    a_tags = browser.page.find_all('a')
    hrefs = [link.get('href') for link in a_tags]
    
    # could change filtering based on the site
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

start_url = "https://en.wikipedia.org/wiki/Redis"
r.lpush("links", start_url)

while link := r.rpop('links'): 
    if "jesus" in str(link).lower():
        print("Found Jesus!")
        break
    crawl(browser, r, link)

