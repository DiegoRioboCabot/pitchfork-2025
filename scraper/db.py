from __future__ import annotations
import time
import sqlite3
import traceback
import pandas as pd

from pathlib import Path

from . import globals as g
from .types import *
from typing import Dict, Any, Optional, List, Optional, Tuple

functions = [
    'initialize_database', 'execute_command', 'execute_script', 
    'insert_named_tuple', 'insert_named_tuples', 'log_event']


__all__ = functions

DB_TABLES = {
    'entities' : '(entity_id INTEGER, entity TEXT)',
    'genres' : '(genre_id INTEGER, genre TEXT)',
    'keywords' : '(keyword_id INTEGER, keyword TEXT)',
    'labels' : '(label_id INTEGER, label TEXT)',
    'urls' : '(url_id INTEGER, url TEXT, year INTEGER, month INTEGER, week INTEGER, is_review INTEGER, is_album INTEGER, is_author INTEGER, is_artist)',
    'albums' : '(album_id TEXT, album TEXT, publisher TEXT, release_year INTEGER, pitchfork_score INTEGER, is_best_new_music INTEGER, is_best_new_reissue INTEGER)',
    'artists' : '(artist_id INTEGER, artist TEXT, url_id TEXT)',
    'authors' : '(author_id TEXT, author TEXT, url_id TEXT)',
    'author_bios' : '(author_id TEXT, date_pub TEXT, revisions INTEGER, bio TEXT)',
    'author_types' : '(author_type_id INTEGER, author_type TEXT)',
    'author_type_evolution' : '(author_id TEXT, author_type1_id INTEGER, author_type2_id INTEGER, as_of_date TEXT)',
    'reviews' : '(review_id TEXT, revisions INTEGER, url_id TEXT, body TEXT, description TEXT, date_pub TEXT, date_mod TEXT)',
    'review_albums' : '(review_id TEXT, album_id TEXT)',
    'review_labels' : '(review_id TEXT, label_id INTEGER)',
    'review_artists' : '(review_id TEXT, artist_id INTEGER)',
    'review_authors' : '(review_id TEXT, author_id TEXT)',
    'review_keywords' : '(review_id TEXT, keyword_id INTEGER, score REAL)',
    'review_entities' : '(review_id TEXT, entity_id INTEGER, score REAL)',
    'review_artist_genres' : '(review_id TEXT, artist_id INTEGER, genre_id INTEGER)',
    'scraping_events': '(timestamp TEXT, url_id INTEGER, process TEXT, success INTEGER, message TEXT)',
    'metadata' : '(table_name TEXT, column_name TEXT, is_primary_key INTEGER, is_foreign_key INTEGER, description TEXT)',}


def __reset_tables(get_connection: SQLite3ConnectionGenerator, tables: Dict[str, str]) -> None:
    """
    Drops and recreates all tables in the SQLite database.

    This function removes all existing tables specified in the `tables` dictionary
    and recreates them using the provided schema definitions.

    Args:
        get_connection (Callable[[], sqlite3.Connection]): 
            A function that returns a new SQLite connection object.
        tables (dict[str, str]): 
            A dictionary where:
            - Keys are table names (str).
            - Values are the SQL schema definitions (str) that follow the `CREATE TABLE` format.

    Example:
        ```python
        tables = {
            "users": "(id INTEGER PRIMARY KEY, name TEXT, age INTEGER)",
            "orders": "(id INTEGER PRIMARY KEY, user_id INTEGER, amount REAL)"
        }

        __reset_tables(get_connection, tables)
        ```

    Notes:
        - This function **irreversibly deletes** all data in the specified tables.
        - Ensure `get_connection()` provides an **open SQLite connection**.
        - The function executes raw SQL, so avoid passing untrusted table names.

    Returns:
        None: This function does not return a value.
    """
    con = get_connection()
    cur = con.cursor()
    for name, cols in tables.items():
        cur.execute(f"DROP TABLE IF EXISTS {name}")  # Delete existing table
        cur.execute(f"CREATE TABLE {name} {cols};")  # Recreate with new schema
    con.close()  # Ensure connection is closed properly

def __create_index_if_missing(
    get_connection:SQLite3ConnectionGenerator, 
    index_name:str, 
    table_name:str, 
    columns:Tuple[str]) -> None:
    """
    Creates an index on a table if it does not already exist.

    This function checks whether the specified index exists in the SQLite database.
    If the index is missing, it creates a new index on the specified table and columns.

    Args:
        get_connection (Callable[[], sqlite3.Connection]): 
            A function that returns a new SQLite connection object.
        index_name (str): 
            The name of the index to be created.
        table_name (str): 
            The name of the table where the index should be applied.
        columns (tuple[str, ...]): 
            A tuple of column names (as strings) to include in the index.

    Example:
        ```python
        __create_index_if_missing(get_connection, "idx_users_name", "users", ("name", "email"))
        ```

    Notes:
        - This function **ensures the index is not duplicated** before creating it.
        - Using **parameterized queries** prevents SQL injection risks.
        - Index creation **should be done carefully** for large tables, as it can impact performance.

    Returns:
        None: This function does not return a value.
    """
    with get_connection() as con:
        cur = con.cursor()
        cur.execute("SELECT name FROM sqlite_master WHERE type='index' AND name=?;", (index_name,))

        if not cur.fetchone():
            cur.execute(f"CREATE INDEX {index_name} ON {table_name} ({columns});")
            con.commit()

def __create_indexes(get_connection:SQLite3ConnectionGenerator) -> None:
    """
    Creates missing indexes in the SQLite database.

    This function iterates over a predefined list of index specifications 
    and ensures that each index exists in the database. If an index is missing, 
    it is created using `__create_index_if_missing`.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection object.

    Notes:
        - Indexes improve query performance by allowing faster lookups.
        - This function does not drop or modify existing indexes.
        - Multi-column indexes are included where necessary.

    Returns:
        None: This function does not return a value.

    """
    index_list = [
        ("idx_urls", "urls", "url_id"),
        ("idx_albums", "albums", "album_id"),
        ("idx_artists", "artists", "artist_id"),
        ("idx_author_bios", "author_bios", "author_id"),
        ("idx_author_type_evolution", "author_type_evolution", "author_id, as_of_date"),
        ("idx_author_types", "author_types", "author_type_id"),
        ("idx_authors", "authors", "author_id"),
        ("idx_entities", "entities", "entity_id"),
        ("idx_genres", "genres", "genre_id"),
        ("idx_keywords", "keywords", "keyword_id"),
        ("idx_labels", "labels", "label_id"),
        ("idx_review_albums", "review_albums", "review_id, album_id"),
        ("idx_review_artist_genres", "review_artist_genres", "review_id, artist_id, genre_id"),
        ("idx_review_artists", "review_artists", "review_id, artist_id"),
        ("idx_review_authors", "review_authors", "review_id, author_id"),
        ("idx_review_entities", "review_entities", "review_id, entity_id"),
        ("idx_review_labels", "review_labels", "review_id, label_id"),
        ("idx_reviews", "reviews", "review_id"),
        ("idx_scraping_events", "scraping_events", "timestamp"),
        ("idx_metadata", "metadata", "table_name, column_name")]

    for index in index_list:
        __create_index_if_missing(get_connection, *index)

def __create_null_types(get_connection:SQLite3ConnectionGenerator) -> None:
    """
    Inserts placeholder "null" rows into database tables.

    This function initializes special "null" rows with `0` or `None` values 
    for specific tables in the database. These null-type rows ensure 
    that foreign key relationships always have a default reference.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection object.

    Notes:
        - This function ensures that all database tables have a **safe default** 
          reference to prevent foreign key errors.
        - The function uses `insert_named_tuples` to insert the data.

    Returns:
        None: This function does not return a value.
    """

    n = (0,None)
    null_types = [
        Label(*n), Genre(*n), Keyword(*n), Entity(*n), Artist(*n, None), Author_Type(*n), 
        # For some articles, Pitchfork has given "no author" an ID.
        # I don't know if that's relevant or not (yet). But I might as well track it
        Author(0, 'Pitchfork_no_id', None), 
        Author('592604b17fd06e5349102f34', 'Pitchfork_with_id', None), 
        URL(0, None, None, None, None, None, None, None, None)]
    insert_named_tuples(get_connection, null_types, log=True)

def __initialize_globals(get_connection:SQLite3ConnectionGenerator) -> None:
    """
    Initializes global variables by loading database records into sets and dictionaries.

    This function reads data from the SQLite database and populates global variables
    (`g.albums_set`, `g.authors_set`, etc.) for efficient lookups during scraping.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection object.

    Notes:
        - This function **reduces database queries** by preloading lookup tables into memory.
        - It **stores album IDs, author IDs, and other data** in global sets/dictionaries.
        - Improves performance by allowing **fast in-memory lookups** instead of repeated SQL queries.

    Global Variables Set:
        - `g.albums_set (Set[int])`: Set of all `album_id`s.
        - `g.authors_set (Set[int])`: Set of all `author_id`s.
        - `g.urls_dict (Dict[str, int])`: Maps `url` to `url_id`.
        - `g.artists_dict (Dict[str, int])`: Maps `artist` to `artist_id`.
        - `g.labels_dict (Dict[str, int])`: Maps `label` to `label_id`.
        - `g.genres_dict (Dict[str, int])`: Maps `genre` to `genre_id`.
        - `g.keywords_dict (Dict[str, int])`: Maps `keyword` to `keyword_id`.
        - `g.entities_dict (Dict[str, int])`: Maps `entity` to `entity_id`.
        - `g.author_types_dict (Dict[str, int])`: Maps `author_type` to `author_type_id`.

    Returns:
        None: This function does not return a value.
    """

    with get_connection() as con:
        g.albums_set = set(pd.read_sql("SELECT album_id FROM albums", con)['album_id'].unique())
        g.authors_set = set(pd.read_sql("SELECT author_id FROM authors", con)['author_id'].unique())
        g.urls_dict = pd.read_sql("SELECT url_id, url FROM urls", con, index_col='url')['url_id'].to_dict()
        g.artists_dict = pd.read_sql("SELECT artist_id, artist FROM artists", con, index_col='artist')['artist_id'].to_dict()
        g.labels_dict = pd.read_sql("SELECT label_id, label FROM labels", con, index_col='label')['label_id'].to_dict()
        g.genres_dict = pd.read_sql("SELECT genre_id, genre FROM genres", con, index_col='genre')['genre_id'].to_dict()
        g.keywords_dict = pd.read_sql("SELECT keyword_id, keyword FROM keywords", con, index_col='keyword')['keyword_id'].to_dict()
        g.entities_dict = pd.read_sql("SELECT entity_id, entity FROM entities", con, index_col='entity')['entity_id'].to_dict()
        g.author_types_dict = pd.read_sql("SELECT author_type_id, author_type FROM author_types", con, index_col='author_type')['author_type_id'].to_dict()

def __check_filepath(filepath: Union[str, Path, None] = None) -> Path:
    """
    Ensures that the given file path exists, creating necessary directories if missing.

    Args:
        filepath (Union[str, Path, None], optional): 
            - If `None`, returns the current working directory.
            - If a string or `Path`, ensures the path exists and returns it.

    Returns:
        Path: A `Path` object representing the absolute path.

    Behavior:
        - If `filepath` is `None`, the function returns the current working directory.
        - If `filepath` is provided, it is treated as a subdirectory under `cwd()`.
        - If the directory does not exist, it is created with `mkdir(parents=True, exist_ok=True)`.
    """
    if filepath is None:
        return Path.cwd()
    
    filepath = Path.cwd() / filepath
    if not filepath.exists():
        filepath.mkdir(parents=True, exist_ok=True)

    return filepath

def initialize_database(db_name: str, filepath: Union[str | Path | None] = None, hard_reset: bool = False) -> SQLite3ConnectionGenerator:
    """
    Initializes an SQLite database and returns a connection generator.

    This function sets up an SQLite database, enabling Write-Ahead Logging (WAL) mode
    and optimizing settings for performance. If `hard_reset` is enabled, the database
    tables and indexes are recreated from scratch.

    Args:
        db_name (str): 
            The name of the SQLite database file.
        filepath (Union[str, Path, None], optional): 
            The directory where the database file is stored. Defaults to `None`, which
            means the current working directory.
        hard_reset (bool, optional): 
            If `True`, resets all tables, indexes, and inserts null types. 
            Defaults to `False`.

    Returns:
        SQLite3ConnectionGenerator: 
            A function that returns an open SQLite connection.

    Notes:
        - The database connection uses:
            - **WAL mode (`PRAGMA journal_mode=WAL;`)** for concurrent reads/writes.
            - **`PRAGMA synchronous=FULL;`** for durability (ensuring database safety).
            - **`PRAGMA temp_store=MEMORY;`** for improved performance.
        - If `hard_reset=True`, it:
            1. **Resets all tables** (`__reset_tables`).
            2. **Creates indexes** (`__create_indexes`).
            3. **Inserts default null values** (`__create_null_types`).
        - Otherwise, it **initializes global variables** (`__initialize_globals`).

    Example:
        ```python
        get_connection = initialize_database("my_database.db", hard_reset=True)
        con = get_connection()  # Get a database connection
        ```
    """
    filepath = __check_filepath(filepath)

    file = filepath / db_name
    hard_reset = hard_reset or (not file.exists())

    def get_connection():
        con = sqlite3.connect(file, check_same_thread=False)
        con.execute("PRAGMA journal_mode=WAL;")
        con.execute("PRAGMA synchronous=FULL;")
        con.execute("PRAGMA temp_store=MEMORY;")
        return con

    if hard_reset:
        __reset_tables(get_connection, DB_TABLES)
        __create_indexes(get_connection)
        __create_null_types(get_connection)
    else:
        __initialize_globals(get_connection)

    return get_connection

def execute_script(get_connection:SQLite3ConnectionGenerator, script_path: Union[str | Path]) -> None:
    """
    Read an SQL script from the given file path and execute it 
    using an SQLite connection.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        script_path (Union[str, Path]): 
            The file path of the SQL script to be executed.

    Returns:
        None: This function does not return a value.

    Notes:
        - The SQL script is read as UTF-8 to ensure compatibility.
        - Uses `executescript` to execute multiple SQL statements in a single call.
        - Automatically commits changes after execution.

    Example:
        ```python
        execute_script(get_connection, "create_my_view.sql")
        ```

    Raises:
        FileNotFoundError: If the script file does not exist.
        sqlite3.DatabaseError: If an error occurs while executing the SQL script.
    """

    script_file = Path(script_path)
    if not script_file.exists():
        raise FileNotFoundError(f"SQL script not found: {script_path}")

    with get_connection() as con:
        cur = con.cursor()
        sql_script = script_file.read_text(encoding="utf-8")  
        cur.executescript(sql_script)
        con.commit() 

def execute_command(
    get_connection: SQLite3ConnectionGenerator, 
    cmd: str, 
    row: Optional[DatabaseRow] = None, 
    delay: float = 0.2) -> None:
    """
    This function runs an SQL command using an SQLite connection, optionally 
    with a database row for parameterized queries. If execution fails, it 
    retries after a short delay.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        cmd (str): 
            The SQL command to be executed.
        row (Optional[DatabaseRow], optional): 
            A namedtuple representing a database row, used for parameterized queries. 
            Defaults to `None`.
        delay (float, optional): 
            The time (in seconds) to wait before retrying if an error occurs. 
            Defaults to `0.2`.

    Returns:
        None: This function does not return a value.

    Notes:
        - Uses **parameterized queries** (`cur.execute(cmd, row)`) to prevent SQL injection.
        - **Automatically retries** in case of transient database errors.
        - Commits changes after a successful execution.
        - Uses `with get_connection()` to ensure proper resource cleanup.

    Example:
        ```python
        execute_command(get_connection, "INSERT INTO users (id, name) VALUES (?, ?)", (1, "Alice"))
        ```

    Raises:
        sqlite3.DatabaseError: If a persistent SQL error occurs.
    """
    while True:
        with get_connection() as con:
            try:
                cur = con.cursor()
                if row is None:
                    cur.execute(cmd)
                else:
                    cur.execute(cmd, row)
                con.commit()
                return
            except sqlite3.DatabaseError as e:
                time.sleep(delay)

def insert_named_tuple(
    get_connection: SQLite3ConnectionGenerator, 
    row: Optional[DatabaseRow], 
    log: bool = True) -> None:
    """
    Inserts a namedtuple record into the corresponding database table.

    This function dynamically generates an SQL `INSERT` statement based on the namedtuple 
    structure, executes it, and logs any failures if insertion fails.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        row (Optional[DatabaseRow]): 
            A namedtuple representing a database row. The namedtuple's class name 
            is used as the table name, and its fields are used as column names.
        log (bool, optional): 
            If `True`, logs any failures to the database. Defaults to `True`.

    Returns:
        None: This function does not return a value.

    Notes:
        - The function **constructs the SQL query dynamically** using the namedtuple's attributes.
        - Uses **parameterized queries** to prevent SQL injection risks.
        - If an error occurs, it **logs the error message into the database** if `log=True`.

    Example:
        ```python
        user = User(id=1, name="Alice")
        insert_named_tuple(get_connection, user)
        ```

    Raises:
        sqlite3.DatabaseError: If a persistent SQL error occurs.
    """
    if row is None:
        return

    table = row.__class__.__name__  # Get table name from namedtuple class
    fields = row._fields            # Extract column names
    field_placeholders = ', '.join(['?'] * len(fields))  # Create parameterized placeholders

    insert_cmd = f"INSERT INTO {table} ({', '.join(fields)}) VALUES ({field_placeholders});"

    try:
        execute_command(get_connection, insert_cmd, tuple(row))  # Execute insertion
    except sqlite3.DatabaseError:
        if log:
            message = traceback.format_exc()
            log_event(
                get_connection, 
                process=f"Failed at inserting data into {table}", 
                success=0, 
                message=message)

def insert_named_tuples(
    get_connection: SQLite3ConnectionGenerator, 
    rows: List[Optional[DatabaseRow]], 
    log: bool = True) -> None:
    """
    This function iterates over a list of namedtuple rows and inserts them into the database
    using `insert_named_tuple`. It skips `None` values and logs failures if `log=True`.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        rows (List[Optional[DatabaseRow]]): 
            A list of namedtuples representing database rows. Each namedtuple's class name 
            is used as the table name, and its attributes are used as column names.
        log (bool, optional): 
            If `True`, logs any failures to the database. Defaults to `True`.

    Returns:
        None: This function does not return a value.

    Notes:
        - Uses `insert_named_tuple` for each row.
        - Skips `None` values to prevent errors.
        - If `rows` is empty, the function exits early.

    Example:
        ```python
        users = [
            User(id=1, name="Alice"),
            User(id=2, name="Bob"),]
        insert_named_tuples(get_connection, users)
        ```
    """
    for row in rows:
        if row is not None:
            insert_named_tuple(get_connection, row, log=log)
        
def log_event(get_connection: SQLite3ConnectionGenerator, **kwargs: Any) -> None:
    """
    This function creates an instance of the `scraping_events` namedtuple, 
    automatically assigning a timestamp, and inserts it into the database.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        **kwargs (Any): 
            Additional fields to populate the `scraping_events` record. These 
            should match the namedtuple attributes for `scraping_events`.

    Returns:
        None: This function does not return a value.

    Notes:
        - The function assigns `scraping_events.default_timestamp()` automatically.
        - Uses `insert_named_tuple` to insert the event into the database.
        - Ensure that `kwargs` contains the correct field names for `scraping_events`.

    Example:
        ```python
        log_event(get_connection, process="Scraper Start", success=1, message="Scraping initiated.")
        ```
    """
    event = scraping_events(timestamp=scraping_events.default_timestamp(), **kwargs)
    insert_named_tuple(get_connection, event)
