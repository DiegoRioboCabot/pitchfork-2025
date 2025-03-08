import os
import pandas as pd
import datetime as dt
import concurrent.futures 

from pathlib import Path
from scraper import db, sitemap, album, author

def multithread_scrape_year(year):
    try:
        print(f'Scraping year {year}')
        sitemap.scrape_sitemap_year(get_connection, year, timeout=2)
        print(f"Completed scraping for {year}")
    except Exception as e:
        print(f"Error scraping {year}: {e}")

def multithread_scrape_album(url_tuple):
    try:
        print(f'Scraping album {url_tuple[1]}')
        album.scrape_album_review(get_connection, *url_tuple, timeout=2)
    except Exception as e:
        print(f"Error scraping {url_tuple[1]}: {e}")

def multithread_scrape_author(info):
    a, id, u = info
    try:
        print(f'Scraping author {a} inside {u}')
        author.scrape_authors_page(get_connection, *info, timeout=2)
    except Exception as e:
        print(f"Error scraping {a}: {e}")

def execute_multi_thread_func(func, params_list):
    with concurrent.futures.ThreadPoolExecutor(max_workers=int(os.cpu_count()*2.5)) as executor:
        executor.map(func, params_list)

#Set up database
filename = dt.datetime.now().strftime('Pitchfork_Album_Reviews_%Y_%m_%d.db')
get_connection = db.initialize_database(filename, filepath='data', hard_reset=True)

# Scrape Sitemap
current_year = dt.datetime.now().year
execute_multi_thread_func(multithread_scrape_year, list(range(1999, current_year + 1 )))

#Scrape Album Reviews
query_review_album_urls = """
SELECT url_id, url 
FROM urls 
WHERE is_review = 1 AND is_album = 1
"""

query_failed_urls = """
SELECT url_id, url
FROM urls u 
WHERE 
   u.is_album = 1 AND
   u.url_id IN (
   SELECT se.url_id 
   FROM scraping_events se 
   WHERE process = "Couldn't stablish connection")
"""

urls = pd.read_sql(query_review_album_urls, get_connection(), index_col='url_id')['url']
execute_multi_thread_func(multithread_scrape_album, [(i,v) for i,v in urls.items()])

#Normally there are 0 failed urls after the first attempt. But just to be sure
retries = 10
failed_album_urls = pd.read_sql(query_failed_urls, get_connection(), index_col='url_id')['url']
while len(failed_album_urls) and (retries > 0):
    execute_multi_thread_func(multithread_scrape_album, [(i,v) for i,v in failed_album_urls.items()])
    failed_album_urls = pd.read_sql(query_failed_urls, get_connection(), index_col='url_id')['url']
    retries -= 1

#Scrape author's Biography page
query_author_bio_pages = """
SELECT a.author_id, u.url_id, u.url
FROM authors a
JOIN urls u ON a.url_id = u.url_id
"""
authors = pd.read_sql(query_author_bio_pages, get_connection(), index_col='author_id')
execute_multi_thread_func(multithread_scrape_author, [(row.url_id, row.url, author_id) for author_id, row in authors.iterrows()])

#Execute the .sql scripts in the /sql_scripts folder
sql_scripts_folder = Path.cwd() / 'sql_scripts'

for script in sql_scripts_folder.iterdir():
    if script.suffix.lower() == '.sql': 
        if 'Create Tables' in script.name:
            continue
        db.execute_script(get_connection, script)
