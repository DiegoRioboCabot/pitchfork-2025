
import time
import traceback
import requests as r
from bs4 import BeautifulSoup

from . import db
from . import globals as g
from .types import SQLite3ConnectionGenerator, URL

from typing import List, Dict, Any, Optional, Union, Tuple

__all__ = ['get_url_id','parse_url','insert_failed_url', 'get_tree_of_keys']


def get_url_id(url:str, return_isnew:bool=False) -> Union[int, Tuple[bool, int]]:
    """
    This function checks whether the provided `url` already has an assigned ID in 
    `g.urls_dict`. If not, it assigns a new unique ID and updates the dictionary. 
    It ensures thread safety using `g.lock`.

    Args:
        url (str): 
            The URL to retrieve or assign an ID for.
        return_isnew (bool, optional): 
            If `True`, returns a tuple `(is_new, url_id)`, where `is_new` indicates 
            whether the URL was newly assigned. Defaults to `False`.

    Returns:
        Union[int, Tuple[bool, int]]: 
            - If `return_isnew=False`: Returns the `url_id` (int).
            - If `return_isnew=True`: Returns a tuple `(is_new, url_id)`, where:
                - `is_new (bool)`: `True` if the URL was newly added, `False` otherwise.
                - `url_id (int)`: The unique ID assigned to the URL.

    Thread Safety:
        - Uses `g.lock` to ensure that updates to `g.urls_dict` and `g.urls_id_counter` 
          are atomic and avoid race conditions in multithreaded environments.

    Example:
        ```python
        url_id = get_url_id("https://example.com")  
        # Returns: 42 (example output)

        is_new, url_id = get_url_id("https://example.com", return_isnew=True)
        # Returns: (False, 42) if the URL was already assigned
        #          (True, 43) if it was newly assigned
        ```
    """
    with g.lock:
        is_new = url not in g.urls_dict
        if is_new:
            url_id = g.urls_id_counter
            g.urls_dict[url] = url_id
            g.urls_id_counter += 1
        else:
            url_id = g.urls_dict[url]
        return (is_new, url_id) if return_isnew else url_id

def insert_failed_url(get_connection:SQLite3ConnectionGenerator, url_id:int, url:str) -> None:
    """
    This function records a failed URL by inserting it into the `urls` table 
    with default `None` values for all other columns.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        url_id (int): 
            The unique identifier for the failed URL.
        url (str): 
            The URL string that failed.

    Returns:
        None: This function does not return a value.

    Notes:
        - Uses `insert_named_tuple` to insert the record.
        - The remaining fields of the `URL` namedtuple are set to `None`.
        - This ensures that failed URLs are logged for later retry or analysis.

    Example:
        ```python
        insert_failed_url(get_connection, 123, "https://example.com/failure")
        ```
    """
    db.insert_named_tuple(get_connection, URL(url_id, url, None, None, None, None, None, None, None))

def parse_url(
    get_connection: SQLite3ConnectionGenerator, 
    url: str, 
    num_retrys: int = 20, 
    timeout: float = 0.75, 
    format: str = "xml",
    ) -> BeautifulSoup | None:
    """
    This function attempts to retrieve a webpage using an HTTP GET request. If 
    the request fails, it retries up to `num_retrys` times with a delay of 
    `timeout` seconds between attempts. The function logs failed attempts 
    and inserts failed URLs into the database when necessary.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        url (str): 
            The URL to fetch and parse.
        num_retrys (int, optional): 
            The number of times to retry fetching the URL before giving up. 
            Defaults to `20`.
        timeout (float, optional): 
            The time (in seconds) to wait before retrying a failed request. 
            Defaults to `0.75`.
        format (str, optional): 
            The parser format for `BeautifulSoup` (e.g., `"xml"`, `"html.parser"`). 
            Defaults to `"xml"`.

    Returns:
        Optional[BeautifulSoup]: 
            - A `BeautifulSoup` object if the page is successfully fetched.
            - `None` if all attempts fail.

    Notes:
        - Uses **a custom User-Agent** to avoid request blocking.
        - If the URL is new, it is logged in the database on failure.
        - **Logs failures** using `log_event` with process `"Connection failed"`.
        - **Handles non-200 status codes** and logs them as errors.

    Example:
        ```python
        soup = parse_url(get_connection, "https://example.com", format="html.parser")
        if soup:
            print(soup.prettify())
        ```
    """
    page = None 
    header = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_5) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/50.0.2661.102 Safari/537.36'}

    is_new, url_id = get_url_id(url, return_isnew=True)
    
    for _ in range(num_retrys):
        try:
            page = r.get(url, headers=header)
            if page.status_code == 200:
                break
        except:
            if is_new:
                insert_failed_url(get_connection, url_id, url)
            message = traceback.format_exc()
            db.log_event(get_connection, url_id=url_id, process='Connection failed', success=0, message=message)

            time.sleep(timeout)

    if page is None or page.status_code != 200:
        db.log_event(get_connection, url_id=url_id, process='Connection failed', success=0, message=page.status_code if page else "No Response")
        return None
    
    return BeautifulSoup(page.content, features=format)

def dict_lookup(data_dict: Optional[Dict[str, Any]], keys_tree: List[str]) -> Optional[Any]:
    """
    This function traverses a dictionary tree using a list of keys and returns 
    the final value if all keys exist. If any key is missing, it returns `None`.

    Args:
        data_dict (Optional[Dict[str, Any]]): 
            A dictionary where keys are strings and values can be of any type. 
            If `None` is passed, the function returns `None`.
        keys_tree (List[str]): 
            A list of keys representing the path to traverse in the dictionary.

    Returns:
        Optional[Any]: 
            - The value found at the end of the key path if all keys exist.
            - `None` if `data_dict` is `None`, empty, or any key in the path is missing.

    Example:
        ```python
        data = {"user": {"profile": {"name": "Alice"}}}

        get_tree_of_keys(data, ["user", "profile", "name"])  # Returns "Alice"
        get_tree_of_keys(data, ["user", "profile", "age"])   # Returns None
        get_tree_of_keys(data, ["settings", "theme"])        # Returns None
        get_tree_of_keys(None, ["any", "key"])               # Returns None
        ```

    Notes:
        - If `data_dict` is `None` or empty, the function immediately returns `None`.
        - If `keys_tree` is empty, the function returns `None`.
        - The function iterates through `keys_tree` and safely accesses dictionary values.

    """
    if (data_dict is None) or (len(keys_tree) == 0) or (not data_dict):
        return None
    
    result = data_dict
    for key in keys_tree:
        if key in result:
            result = result[key]
        else:
            return None
    return result
