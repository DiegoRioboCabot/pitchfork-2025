
import os

import datetime as dt
import concurrent.futures 

from scraper import db, sitemap, album

def multithread_scrape_single_album(url_raw):
    try:
        print(f'Scraping album {url_raw}')
        url = sitemap.parse_album_url(get_connection, url=url_raw[0], timeout=2)
        if url is None:
            return
        print(f'Inserting {url} into the db')
        db.insert_named_tuple(get_connection, url)
        print(f'Scraping detailed album info for {url_raw}')
        album.scrape_album_review(get_connection, url_id=url.url_id, url=url.url, timeout=2)
    except Exception as e:
        print(f"Error scraping {url_raw}: {e}")

def execute_multi_thread_func(func, params_list):
    with concurrent.futures.ThreadPoolExecutor(max_workers=int(os.cpu_count()*2.5)) as executor:
        executor.map(func, params_list)

# See github issue https://github.com/DiegoRioboCabot/pitchfork-2025/issues/2
unreachable_urls = [
'https://pitchfork.com/reviews/albums/21636-crab-day/',
'https://pitchfork.com/reviews/albums/21680-sorrow-a-reimagining-of-goreckis-3rd-symphony/',
'https://pitchfork.com/reviews/albums/21681-singing-saw/',
'https://pitchfork.com/reviews/albums/21703-yyy-ep/',
'https://pitchfork.com/reviews/albums/21829-layers/',
'https://pitchfork.com/reviews/albums/22119-kuiper/',
'https://pitchfork.com/reviews/albums/22121-drankin-drivin/',
'https://pitchfork.com/reviews/albums/22126-gqom-oh-x-crudo-volta-mixtape/',
'https://pitchfork.com/reviews/albums/22139-boy-king/',
'https://pitchfork.com/reviews/albums/22158-four-meditations-sound-geometries/',
'https://pitchfork.com/reviews/albums/22284-2007-2011/',
'https://pitchfork.com/reviews/albums/22291-end-of-the-century/',
'https://pitchfork.com/reviews/albums/22346-the-ecm-recordings/',
'https://pitchfork.com/reviews/albums/22368-blue-mountain/',
'https://pitchfork.com/reviews/albums/22374-metal-box/',
'https://pitchfork.com/reviews/albums/22378-integrity-blues/',
'https://pitchfork.com/reviews/albums/22388-electronic-music-from-the-seventies-and-eighties/',
'https://pitchfork.com/reviews/albums/22414-strands/',
'https://pitchfork.com/reviews/albums/22430-rr7349/',
'https://pitchfork.com/reviews/albums/22479-motor-earth/',
'https://pitchfork.com/reviews/albums/22503-the-violent-sleep-of-reason/',
'https://pitchfork.com/reviews/albums/22541-say-yes-a-tribute-to-elliott-smith/',
'https://pitchfork.com/reviews/albums/22562-goodbye-terrible-youth/',
'https://pitchfork.com/reviews/albums/22578-lady-wood/',]

#Set up database
# filename = dt.datetime.now().strftime('Pitchfork_Album_Reviews_%Y_%m_%d.db')
filename = dt.datetime.now().strftime('Pitchfork_Album_Reviews_2025_03_04.db')
get_connection = db.initialize_database(filename, filepath='data', hard_reset=False)

execute_multi_thread_func(multithread_scrape_single_album, unreachable_urls)

#TODO
# must get the new authors in order to scrape their bios too.