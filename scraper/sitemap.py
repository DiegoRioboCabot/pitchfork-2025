
import traceback
import requests as r
import datetime as dt

from . import db
from . import general
from .types import SQLite3ConnectionGenerator, URL
from typing import List, Tuple

__all__ = [
    'get_url_dates', 'get_url_attributes', 
    'get_weekly_urls_in_a_year', 'get_urls_inside_a_weekly_url', 
    'scrape_sitemap_year', 'scrape_sitemap_year_range',]

## Sitemap Scraping
def get_url_dates(url:str) -> List[str]:
    """
    Extracts year, month, and week values from a URL's query parameters.

    This function assumes that the URL contains parameters in the format:
    `?year=YYYY&month=MM&week=W` and returns them as a list of integers.

    Args:
        url (str): 
            The URL string containing query parameters.

    Returns:
        List[int]: 
            A list containing `[year, month, week]` as integers.

    Example:
        ```python
        get_url_dates("https://example.com/sitemap.xml?year=2024&month=5&week=3")
        # Returns: [2024, 5, 3]

        get_url_dates("https://example.com/page?year=2021&month=12&week=1")
        # Returns: [2021, 12, 1]
        ```

    Notes:
        - The function **assumes** that the query parameters always follow the 
          format `?year=YYYY&month=MM&week=W`.
        - If the URL does not contain parameters, an empty list may be returned.
        - Extracts the **last numeric value** for each parameter, assuming the correct format.

    Raises:
        ValueError: If the extracted values cannot be converted to integers.
    """
    params = url.split('?')[-1].split('&')
    return [int(p.split('=')[-1]) for p in params] # Returns [year, month, week]

def get_url_attributes(url:str) -> Tuple[bool]:
    """
    Determines the type of content referenced by a given URL.

    This function checks whether a URL belongs to a 
    **track review, album review, author (staff), or artist** 
    by looking for specific path segments.

    Args:
        url (str): 
            The URL string to analyze.

    Returns:
        Tuple[bool, bool, bool, bool]: 
            A tuple `(is_review, is_album, is_author, is_artist)`, where:
            - `is_review` (`bool`): `True` if the URL contains `"/reviews/"`.
            - `is_album` (`bool`): `True` if the URL contains `"/albums/"`.
            - `is_author` (`bool`): `True` if the URL contains `"/staff/"` (denoting an author).
            - `is_artist` (`bool`): `True` if the URL contains `"/artists/"`.

    Example:
        ```python
        get_url_attributes("https://pitchfork.com/reviews/albums/album-name/")
        # Returns: (True, True, False, False)

        get_url_attributes("https://pitchfork.com/staff/author-name/")
        # Returns: (False, False, True, False)

        get_url_attributes("https://pitchfork.com/artists/artist-name/")
        # Returns: (False, False, False, True)
        ```

    Notes:
        - A URL **can match multiple attributes at once** (e.g., `/reviews/albums/`).
        - This function **does not validate** whether the URL is real or functional.
        - The path matching is **case-sensitive** (expects lowercase URLs).
    """

    is_review = r'/reviews/' in url
    is_album = r'/albums/' in url
    is_author = r'/staff/' in url
    is_artist = r'/artists/' in url

    return is_review, is_album, is_author, is_artist

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
    soup = general.parse_url(get_connection, timeout=timeout, url=f'https://pitchfork.com/sitemap.xml?year={year}')
    if soup is None:
        return []
    return [url.text for url in soup.find_all('loc')]

def get_urls_inside_a_weekly_url(get_connection:SQLite3ConnectionGenerator, weekly_url:str, timeout:float=0.5) -> URL:
    """
    This function fetches a **weekly sitemap page** from Pitchfork, extracts 
    all URLs listed within it, and converts them into `URL` namedtuples 
    with metadata such as year, month, and week.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        weekly_url (str): 
            The sitemap URL for a specific week.
        timeout (float, optional): 
            The timeout in seconds before retrying a failed request. 
            Defaults to `0.5`.

    Returns:
        List[URL]: 
            A list of `URL` namedtuples, each containing:
            - `url_id`: A unique ID for the URL.
            - `url`: The extracted URL.
            - `year, month, week`: Extracted from the sitemap URL.
            - `is_review, is_album, is_author, is_artist`: Derived from URL attributes.

    Example:
        ```python
        urls = get_urls_inside_a_weekly_url(get_connection, "https://pitchfork.com/sitemap.xml?year=2024&month=5&week=3")
        print(urls)
        # Example output: [URL(101, "https://pitchfork.com/reviews/album-name", 2024, 5, 3, True, False, False, False), ...]
        ```

    Notes:
        - Calls `general.parse_url()` to fetch and parse the sitemap.
        - Uses `get_url_dates()` to extract `year`, `month`, and `week` from `weekly_url`.
        - Uses `general.get_url_id()` to assign a unique ID to each extracted URL.
        - Uses `get_url_attributes()` to determine the content type (review, album, author, artist).
        - If the request fails, the function returns an **empty list** instead of `None`.

    """

    soup = general.parse_url(get_connection, timeout=timeout, url=weekly_url)
    if not soup:
        return []

    dates = get_url_dates(weekly_url)
    urls_raw = [u.text for u in soup.find_all('loc')]
    return [URL(general.get_url_id(u), u, *dates, *get_url_attributes(u)) for u in urls_raw]

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
        - Inserts extracted URLs into the database using `db.insert_named_tuples()`.
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

    urls = []
    try:
        for weekly_url in get_weekly_urls_in_a_year(get_connection, year, timeout=timeout):
            urls += get_urls_inside_a_weekly_url(get_connection, weekly_url, timeout=timeout)
        db.insert_named_tuples(get_connection, urls)
    except Exception as e:
        message = traceback.format_exc()
        db.log_event(get_connection, process=f"Error scraping year {year}", success=0, message=message)
