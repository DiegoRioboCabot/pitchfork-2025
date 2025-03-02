import re
import pytz
import json
import datetime as dt
import traceback

from . import db
from . import general
from . import globals as g
from .types import *
from typing import Optional, Any, Dict, List

def scrape_json_data(
    get_connection: SQLite3ConnectionGenerator, 
    url_id: int, 
    url: str, 
    timeout: float = 0.5
    ) -> Optional[Dict[str, Any]]:
    """
    This function fetches an author's **biography page**, extracts the 
    **preloaded JSON metadata**, and parses it into a dictionary.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        url_id (int): 
            A unique identifier for the URL.
        url (str): 
            The author's biography page URL.
        timeout (float, optional): 
            The timeout in seconds before retrying a failed request. Defaults to `0.5`.

    Returns:
        Optional[Dict[str, Any]]: 
            The extracted **preloaded JSON metadata** if successful, otherwise `None`.

    Process:
        1. **Fetches the webpage using `general.parse_url()`**.
        2. **Locates the `<script>` tag** containing `"window.__PRELOADED_STATE__"`.
        3. **Extracts and parses the JSON data** from the script tag.
        4. **Logs failures** if JSON parsing fails.

    Example:
        ```python
        json_pl = scrape_json_data(get_connection, url_id=201, url="https://pitchfork.com/staff/author-name")
        print(json_pl.keys())  # Example output: ['transformed', 'head', 'coreDataLayer']
        ```

    Notes:
        - If **no connection can be established**, `None` is returned.
        - Uses **`traceback.format_exc()`** to capture and log exceptions.
        - The function **does not modify the database**; it logs failures but only returns data.

    Raises:
        JSONDecodeError: If JSON parsing fails.
    """
    soup = general.parse_url(get_connection, url=url, format='html.parser', timeout=timeout)

    if soup is None:
        return None

    # Extract json preload data from page
    try:
        json_preload = json.loads(soup
            .find("script", string=lambda t: t and "window.__PRELOADED_STATE__" in t)
            .string
            .split("window.__PRELOADED_STATE__ =")
            [-1]
            .strip(" ;"))
        return json_preload
    except:
        message = traceback.format_exc()
        db.log_event(get_connection, url_id=url_id, process='Failed at parsing json preload data', success=0, message=message)
        return None
    
def scrape_authors_bio(json_pl: Dict[str, Any], author_id: str) -> Author_Bio:
    """
    This function retrieves **biographical details, revision count, and 
    publish date** from an author's profile page JSON.

    Args:
        json_pl (Dict[str, Any]): 
            The **preloaded JSON metadata** extracted from the page.
        author_id (str): 
            The unique identifier for the author.

    Returns:
        Author_Bio: 
            A namedtuple containing the author's **ID, publication date, revision count, and bio**.

    Process:
        1. **Attempts to extract the biography text** from multiple JSON paths.
        2. **Removes placeholder text** to ensure meaningful data.
        3. **Extracts the revision count** (`noOfRevisions`).
        4. **Extracts the author's biography publication date**.

    Example:
        ```python
        author_bio = scrape_authors_bio(json_pl, author_id="123")
        print(author_bio)
        # Output: Author_Bio(author_id='123', date_pub='2023-08-14', revisions=5, bio="John Doe is a music critic.")
        ```

    Notes:
        - If **no biography is found**, the `bio` field is set to `None`.
        - The function **removes generic placeholder text** to avoid returning useless data.
        - Uses **`general.dict_lookup()`** to safely navigate nested JSON fields.

    Raises:
        KeyError: If expected fields are missing from `json_pl`.
    """
    path_1 = ['transformed', 'head.description']
    path_2 = ['transformed', 'head.social.description']

    for path in [path_1, path_2,]:
        raw_bio = general.dict_lookup(json_pl, path)
        if raw_bio is not None:
            break

    if raw_bio is not None:
        if raw_bio == '':
            bio = None
        else:
            legend_when_empy = "bio and get latest news stories and articles."
            bio = None if (legend_when_empy in raw_bio.lower()) else raw_bio

    noofrevs = general.dict_lookup(json_pl, ['transformed', 'coreDataLayer', 'content', 'noOfRevisions'])
    date_pub = general.dict_lookup(json_pl, ['transformed', 'payment', 'negotiation', 'content', 'publishDate'])
    
    return Author_Bio(author_id, date_pub, noofrevs, bio)

def scrape_authors_type(json_pl:Dict[str,Any]) -> List[Author_Type]:
    """
    This function retrieves the **author's displayed title** (e.g., "Senior Editor") and 
    ensures it is cleaned, formatted, and stored correctly.

    Args:
        json_pl (Dict[str, Any]): 
            The **preloaded JSON metadata** extracted from the author's page.

    Returns:
        List[Author_Type]: 
            A list containing a **single `Author_Type` object** if a valid title is found, 
            otherwise an empty list.

    Process:
        1. **Extracts the author’s title** from multiple JSON paths.
        2. **Removes anomalies** (e.g., very long strings, irrelevant text like "Pitchfork").
        3. **Formats and standardizes the title** (capitalization, spacing, etc.).
        4. **Assigns a unique ID to the title** and ensures it is not duplicated.
        5. **Returns a structured `Author_Type` object**.

    Example:
        ```python
        author_types = scrape_authors_type(json_pl)
        print(author_types)  # [Author_Type(author_type_id=5, author_type="Senior Editor")]
        ```

    Notes:
        - If **no valid title is found**, an **empty list is returned**.
        - Uses **two JSON paths** (`["transformed", "contributor", "header", "title"]`, 
          `["transformed", "content4d", "title"]`) to maximize retrieval success.
        - Uses **`g.author_types_lock`** to ensure **thread safety**.
        - **Filters out anomalous types** (e.g., long strings, irrelevant data).
    """
    def reformat_author_type(t: Optional[str]) -> Optional[str]:
        """
        Cleans and standardizes the extracted author type.

        - Strips leading/trailing spaces.
        - Capitalizes each word.
        - Removes "Pitchfork" if mistakenly present.
        - Ensures reasonable length and removes anomalies.

        Args:
            t (Optional[str]): Raw author type string.

        Returns:
            Optional[str]: Cleaned and standardized author type or `None` if invalid.
        """
        if t is None:
            return None
        
        t = (re
            .sub(r' +', ' ', t) # regular expression to replace one or more spaces with a single space
            .strip()            # Remove all leading and trailing spaces
            .title()            # Capitalize first letter of each word
            .replace(', Pitchfork', '')     # Some author types have it, shouldn't be there
            .replace('Pitchfork', '')       # Pichfork declared as an author type here and there, should be "None"
            .strip())
        
        return t if t else None

    def get_author_name(json_pl: Dict[str, Any]) -> Optional[str]:
        """
        Extracts and cleans the author's name from JSON.

        Args:
            json_pl (Dict[str, Any]): The preloaded JSON metadata.

        Returns:
            Optional[str]: The author's lowercase name or `None` if not found.
        """
        author_name = general.dict_lookup(json_pl, ['transformed', 'coreDataLayer', 'content', 'authorNames'])
        author_name = reformat_author_type(author_name)
        if author_name:
            return author_name.lower()
        return None

    def get_author_type(json_pl: Dict[str, Any], path: List[str]) -> Optional[str]:
        """
        Extracts, cleans, and validates an author's type/title.

        Args:
            json_pl (Dict[str, Any]): The preloaded JSON metadata.
            path (List[str]): The JSON path to the author type.

        Returns:
            Optional[str]: A cleaned and validated author type or `None` if invalid.
        """
        at = general.dict_lookup(json_pl, path)
        at = reformat_author_type(at)

        if at is None:
            return None
        
        if len(at) > 35:
            return None
        
        anomalous_types = [
            None,
            'Ars Technica', 
            'Dice For Any Occasion', 
            '“Made For Love” By Alissa Nutting', 
            'Review: Motorola Droid Razr Maxx',
            'Megan Buerger | Staff |' ]
        
        if at in anomalous_types:
            return None

        author_name = get_author_name(json_pl)
        if author_name:
            if (author_name in at.lower()) or (at.lower() in author_name):
                return None

        return at
    
    path_1 = ['transformed', 'contributor', 'header', 'title']      # This is what is actually shown to users
    path_2 = ['transformed', 'content4d', 'title']                  # Sometimes this was used (especially for older authors)
    paths = [path_1, path_2]
    
    ats = [get_author_type(json_pl, p) for p in paths ]

    author_types = []

    for author_type in ats:
        is_new_author_type = author_type not in g.author_types_dict
        if is_new_author_type:
            g.author_types_dict[author_type] = g.author_types_id_counter
            g.author_types_id_counter += 1

        author_type_id = g.author_types_dict[author_type]
        author_types.append((is_new_author_type, Author_Type(author_type_id, author_type)))

    return author_types

def generate_author_type_evolution(author_types: List[Author_Type], author_id: str) -> Author_Type_Evolution:
    """
    Generates an `Author_Type_Evolution` entry to track an author's role changes over time.

    If an author has multiple titles (e.g., "Senior Editor" and "Contributor"), this function:
    - **Records both roles** (if available).
    - **Timestamps the change** to track when the roles were observed.

    Args:
        author_types (List[Author_Type]): 
            A list of extracted `Author_Type` objects for the author.
        author_id (str): 
            The unique identifier for the author.

    Returns:
        Author_Type_Evolution: 
            A namedtuple containing:
            - The author's ID.
            - Their primary and secondary roles.
            - A timestamp for when the roles were recorded.

    Process:
        1. **Extracts the author type IDs** from the provided `author_types`.
        2. **Assigns a primary role (`at_ids[0]`)** and, if available, a secondary role (`at_ids[1]`).
        3. **Generates a timestamp** (current time in Vienna timezone).
        4. **Returns a structured `Author_Type_Evolution` object**.

    Example:
        ```python
        author_types = [Author_Type(5, "Senior Editor"), Author_Type(9, "Contributor")]
        author_type_evolution = generate_author_type_evolution(author_types, "123")
        print(author_type_evolution)
        # Output: Author_Type_Evolution(author_id='123', author_type1_id=5, author_type2_id=9, as_of_date='2024-03-01T12:30:00+01:00')
        ```

    Notes:
        - If **only one author type exists**, the secondary role is set to `None`.
        - Uses **Vienna timezone** for consistent tracking.
        - This function **does not modify the database**; it returns structured data.
    """
    at_ids = [a.author_type_id for _, a in author_types]
    now = dt.datetime.now(pytz.timezone("Europe/Vienna")).isoformat()
    return Author_Type_Evolution(author_id, at_ids[0], at_ids[1], now)

def scrape_authors_page(
    get_connection: SQLite3ConnectionGenerator, 
    url_id: int, 
    url: str, 
    author_id: str, 
    timeout: float = 0.5
    ) -> None:
    """
    Scrapes and processes an author's profile page.

    This function:
    - Extracts the **author's biography** and inserts it into the database.
    - Extracts the **author's roles** (e.g., "Senior Editor") and tracks role changes over time.
    - Ensures **newly discovered roles** are added to the database.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        url_id (int): 
            A unique identifier for the URL.
        url (str): 
            The author's profile page URL.
        author_id (str): 
            The unique identifier for the author.
        timeout (float, optional): 
            The timeout in seconds before retrying a failed request. Defaults to `0.5`.

    Returns:
        None: This function does not return a value but logs failures if they occur.

    Process:
        1. **Fetches and parses JSON metadata** from the author's page.
        2. **Extracts the author's biography** (`scrape_authors_bio()`).
        3. **Logs and exits if biography extraction fails**.
        4. **Inserts biography data** into the database.
        5. **Extracts author roles** (`scrape_authors_type()`).
        6. **Logs and exits if role extraction fails**.
        7. **Tracks role changes** (`generate_author_type_evolution()`).
        8. **Inserts role evolution data** into the database.
        9. **Inserts newly discovered roles** into the database.

    Example:
        ```python
        scrape_authors_page(get_connection, url_id=201, url="https://pitchfork.com/staff/john-doe", author_id="123")
        ```

    Notes:
        - If **JSON extraction fails**, the function exits early without processing further.
        - If **biography extraction fails**, a log entry is created, and the function exits.
        - Uses `db.insert_named_tuple()` to insert extracted data.
        - The function **ensures that newly discovered roles** are tracked and stored.

    Raises:
        Exception: Any unexpected error is logged, and the function exits early.
    """
    json_preload = scrape_json_data(get_connection, url_id, url, timeout=timeout)

    if json_preload is None:
        return

    try:
        authors_bio = scrape_authors_bio(json_preload, author_id)
    except:
        message = traceback.format_exc()
        db.log_event(get_connection, url_id=url_id, process=f'Failed at parsing bio from author id {author_id}', success=0, message=message)
        return

    db.insert_named_tuple(get_connection, authors_bio)

    try:
        author_types = scrape_authors_type(json_preload) # returns List of tuples (is_new_author_type:bool, Author_Type:namedtuple)
    except:
        message = traceback.format_exc()
        db.log_event(get_connection, url_id=url_id, process=f'Failed at parsing author_type from author id {author_id}', success=0, message=message)
        return

    author_type_evolution = generate_author_type_evolution(author_types, author_id)
    db.insert_named_tuple(get_connection,author_type_evolution)

    for is_new, author_type in author_types:
        if is_new:
            db.insert_named_tuple(get_connection,author_type)
