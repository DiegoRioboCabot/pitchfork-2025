
import traceback
import requests as r
import datetime as dt
from dateutil import parser
from bs4 import BeautifulSoup

from . import db
from . import general
from . import album
from .types import SQLite3ConnectionGenerator, URL
from typing import List, Tuple, Optional

__all__ = [
    'get_weekly_urls_in_a_year', 'get_urls_inside_a_weekly_url', 
    'scrape_sitemap_year', 'scrape_sitemap_year_range',]

## Sitemap Scraping
def get_album_url_dates(soup:Optional[BeautifulSoup]):
    json_ld = album.extract_json_linked_data_album(soup)
    d = parser.isoparse(json_ld['datePublished'])
    return d.year, d.month, d.isoweekday()

def get_weekly_urls_in_a_year(get_connection:SQLite3ConnectionGenerator, year:int, timeout:float=0.5) -> List[str]:
    """
    Retrieves all weekly sitemap URLs for a given year from Pitchfork's sitemap.

    This function fetches the sitemap for the specified `year` and extracts 
    all URLs listed within it.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        year (int): 
            The year for which to retrieve weekly sitemap URLs.
        timeout (float, optional): 
            The timeout in seconds before retrying a failed request. 
            Defaults to `0.5`.

    Returns:
        List[str]: 
            A list of sitemap URLs for each week of the given year. 
            Returns an empty list if the request fails.

    Example:
        ```python
        weekly_urls = get_weekly_urls_in_a_year(get_connection, 2024)
        print(weekly_urls)  
        # Example output: ["https://pitchfork.com/sitemap.xml?year=2024&month=1&week=1", ...]
        ```

    Notes:
        - Calls `general.parse_url()` to fetch the sitemap page.
        - If the request fails, the function returns an **empty list** instead of `None`.
        - The function extracts all `<loc>` elements, which contain the URLs.
    """

    url = f'https://pitchfork.com/sitemap.xml?year={year}' # This URL right here must be added to the DB.

    # is_new, url_id = blah blah
    # insert_named_tuple(URL(blah blah))

    soup = general.parse_url(get_connection, timeout=timeout, url=url)
    if soup is None:
        return []
    return [url.text for url in soup.find_all('loc')]

def get_urls_inside_a_weekly_url(get_connection:SQLite3ConnectionGenerator, weekly_url:str, timeout:float=0.5) -> None:
    """
    This function fetches a **weekly sitemap page** from Pitchfork, extracts 
    all URLs listed within it, and converts them into `URL` namedtuples 
    that will be inserted into the database.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        weekly_url (str): 
            The sitemap URL for a specific week.
        timeout (float, optional): 
            The timeout in seconds before retrying a failed request. 
            Defaults to `0.5`.

    Returns: None

    Notes:
        - Calls `general.parse_url()` to fetch and parse the sitemap.
        - Uses `general.get_url_id()` to assign a unique ID to each extracted URL.s
    """

    soup = general.parse_url(get_connection, timeout=timeout, url=weekly_url)
    if not soup:
        return 

    urls_raw = [u.text for u in soup.find_all('loc')]
    urls = [URL(general.get_url_id(u), u) for u in urls_raw]
    db.insert_named_tuples(get_connection, urls)

def parse_album_url(get_connection:SQLite3ConnectionGenerator, url:str, timeout:float=0.5) -> URL:
        soup = general.parse_url(get_connection, timeout=timeout, url=url, format='html.parser')
        return None if (soup is None) else URL(general.get_url_id(url), url)

def scrape_sitemap_year(get_connection:SQLite3ConnectionGenerator, year:int, timeout:float=0.5) -> None:
    """
    This function retrieves all weekly sitemap URLs for the specified `year`, 
    extracts the URLs listed within them, and inserts them into the database.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        year (int): 
            The year to scrape from the sitemap.
        timeout (float, optional): 
            The timeout in seconds before retrying a failed request. 
            Defaults to `0.5`.

    Returns:
        None: This function does not return a value.

    Process:
        - Calls `get_weekly_urls_in_a_year()` to get all weekly sitemap URLs.
        - Calls `get_urls_inside_a_weekly_url()` for each weekly URL to extract page URLs.
        - Logs any exceptions that occur during the scraping process.

    Example:
        ```python
        scrape_sitemap_year(get_connection, 2024)
        ```

    Notes:
        - If an error occurs, it logs the exception using `db.log_event()`.
        - Uses **list concatenation (`urls += ...`)** to collect URLs before insertion.
        - The function **does not return** any data; it writes directly to the database.

    Raises:
        Exception: Any unexpected error is caught and logged.
    """

    try:
        for weekly_url in get_weekly_urls_in_a_year(get_connection, year, timeout=timeout):
            get_urls_inside_a_weekly_url(get_connection, weekly_url, timeout=timeout)
    except Exception as e:
        message = traceback.format_exc()
        db.log_event(get_connection, process=f"Error scraping year {year}", success=0, message=message)
