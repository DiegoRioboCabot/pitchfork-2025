import json
import traceback

from dateutil import parser
from bs4 import BeautifulSoup

from . import db
from . import general
from . import globals as g
from .types import *
from typing import Dict, Tuple, Any, Optional, List, Callable

__all__ = [
'extract_json_preload_data', 'extract_json_linked_data_album'
'scrape_review_data', 'scrape_authors_data', 
'scrape_albums_data', 'scrape_artists_data', 
'scrape_entities_data', 'scrape_keywords_data', 
'scrape_json_data', 'scrape_section', 'scrape_album_review', ]

## Album Review Scraping
def extract_json_preload_data(soup:Optional[BeautifulSoup]) -> Optional[Dict[str,Any]]:
    """
    Extracts and filters JSON preload data from a `BeautifulSoup` object.

    Args:
        soup (Optional[BeautifulSoup]): 
            A `BeautifulSoup` object representing the parsed HTML of a page.

    Returns:
        Optional[Dict[str, Any]]: 
            A dictionary containing the extracted JSON data with selected keys.
            Returns `None` if extraction fails.

    Process:
        - Finds a `<script>` tag containing `"window.__PRELOADED_STATE__"`.
        - Extracts and loads the JSON content.
        - Keeps only **relevant keys**.
        - Removes **irrelevant subkeys** from the `"review"` section.

    Example:
        ```python
        soup = BeautifulSoup(html_content, "html.parser")
        preload_data = extract_json_preload_data(soup)
        if preload_data:
            print(preload_data.keys())
        ```

    Notes:
        - The function **expects** `window.__PRELOADED_STATE__` to be in a `<script>` tag.
        - If the JSON extraction or parsing fails, the function returns `None`.
        - Some **keys are explicitly excluded** from the `"review"` section for optimization.

    Raises:
        KeyError: If expected keys are missing from the extracted JSON.
        JSONDecodeError: If the extracted data is not valid JSON.
    """

    try:
        data = json.loads(soup
            .find("script", string=lambda t: t and "window.__PRELOADED_STATE__" in t)
            .string
            .split("window.__PRELOADED_STATE__ =")
            [-1]
            .strip(" ;"))
    except:
        return None

    keys_to_keep = [
    'coreDataLayer', 'review', 'content4d',
    'head.canonicalUrl', 'head.hreflang', 'head.description', 'head.title', 
    'head.promo.dek', 'head.social.opinion','head.jsonld', 'head.contentID', 
    'head.firstPublishDate', 'head.modifiedDate',  'head.hasSponsoredContent',]

    keys_to_exclude = ["recircs", "recircRelated", "recircMostPopular", "offers", "newsletterModules", "showBookmark", "showLocalisedOffers", "summaryProps", "tagCloud"]

    data = {k:data['transformed'][k] for k in keys_to_keep} 
    review_data = data['review']
    review_data = {k:review_data[k] for k in review_data if k not in keys_to_exclude}
    data['review'] = review_data

    return data

def extract_json_linked_data_album(soup: Optional[BeautifulSoup]) -> Dict[str,Any]:
    """
    This function looks for a `<script>` tag of type `"application/ld+json"`
    and attempts to parse its JSON content.

    Args:
        soup (Optional[BeautifulSoup]): 
            A `BeautifulSoup` object representing the parsed HTML of a page.

    Returns:
        Optional[Dict[str, Any]]: 
            A dictionary containing the extracted JSON-LD data, or en empty dictionary 
            if the data is missing or cannot be parsed.

    Process:
        - Extracts and parses the JSON content from the tag.
        - Returns the structured data if available.

    Example:
        ```python
        soup = BeautifulSoup(html_content, "html.parser")
        album_data = extract_json_linked_data_album(soup)
        if album_data:
            print(album_data["@type"])
        ```

    Notes:
        - JSON-LD (Linked Data) is commonly used for structured metadata in **album reviews**.
        - If the `<script>` tag is missing or invalid, the function returns an empty dictionary.
        - The function does **not** validate the extracted JSON schema.

    Raises:
        JSONDecodeError: If the extracted content is not valid JSON.
    """

    data = {}
    tag = soup.find("script", type="application/ld+json")
    if tag:
        data = json.loads(tag.string)
    return data

def scrape_review_data(
        json_pl:Dict[str,Any], 
        json_ld:Dict[str,Any]) -> Tuple[List[Optional[Review]], List[Label], List[URL], List[Review_Labels]]:
    """
    This function retrieves review metadata from two JSON structures:
    - **`json_pl` (preloaded JSON):** Contains page metadata and review details.
    - **`json_ld` (linked data JSON):** Provides structured review information.

    Args:
        json_pl (Dict[str, Any]): 
            The **preloaded JSON data** extracted from the page.
        json_ld (Dict[str, Any]): 
            The **structured JSON-LD data** extracted from the page.

    Returns:
        Tuple[List[Optional[Review]], List[Label], List[URL], List[Review_Labels]]: 
            - `List[Optional[Review]]`: A list containing the **Review** namedtuple.
            - `List[Label]`: A list of **new labels** extracted from the review.
            - `List[URL]`: A list of **new URLs** found in the review.
            - `List[Review_Labels]`: A list of **Review-Label relationships**.

    Process:
        1. **Extracts the review URL** from `json_ld` and assigns a unique ID.
        2. **Creates a `Review` namedtuple** using metadata from `json_pl` and `json_ld`.
        3. **Extracts labels** from `json_pl` and ensures they are stored uniquely.
        4. **Links the review with its labels** using `Review_Labels`.

    Example:
        ```python
        review_data, new_labels, new_urls, review_labels = scrape_review_data(json_pl, json_ld)
        print(review_data)      # [Review(...)]
        print(new_labels)       # [Label(1, "Indie Rock"), Label(2, "Experimental")]
        print(new_urls)         # [URL(123, "https://pitchfork.com/reviews/...", ...)]
        print(review_labels)    # [Review_Labels(review_id=1, label_id=1), ...]
        ```

    Notes:
        - **Thread safety**: Uses `g.lock` to prevent race conditions when modifying `g.labels_dict`.
        - If the label field is missing, it defaults to `[None]`.
        - The `datePublished` and `dateModified` fields are parsed into ISO 8601 format.
        - The function **does not modify the database** directly; it returns processed data.

    Raises:
        KeyError: If expected fields are missing from `json_pl` or `json_ld`.
        ValueError: If date parsing fails.
    """
    
    new_urls = []
    review_url = json_ld['url']

    is_new_url, url_id = general.get_url_id(review_url, return_isnew=True)
    if is_new_url:
        new_urls.append(URL(url_id, review_url, None, None, 1, 1, 0, 0))

    review = Review(
        review_id = general.dict_lookup(json_pl, ['coreDataLayer', 'content', 'contentId']),
        revisions = int(general.dict_lookup(json_pl, ['coreDataLayer', 'content', 'noOfRevisions'])),
        url_id = url_id,
        body = json_ld.get('reviewBody'),
        description = json_pl['head.description'],
        date_mod = parser.isoparse(json_ld['dateModified']).isoformat(),
        date_pub = parser.isoparse(json_ld['datePublished']).isoformat())

    labels = general.dict_lookup(json_pl, ['review', 'multiReviewHeaderProps', 'infoSliceFields', 'label'])
    labels = labels.split(' / ') if labels else [None]

    new_labels = []
    review_labels = []
    for label in labels:
        with g.lock:  # 🔒 Ensure only one thread modifies `g.labels_dict`
            if label not in g.labels_dict:
                label_id = g.labels_id_counter
                g.labels_dict[label] = label_id
                g.labels_id_counter += 1
                new_labels.append(Label(label_id, label))
            else:
                label_id = g.labels_dict[label]

        review_labels.append(Review_Labels(review.review_id, label_id))

    return [review], new_labels, new_urls, review_labels

def scrape_authors_data(json_pl:Dict[str,Any]) -> Tuple[List[Author], List[Label], List[Review_Authors]]:
    """
    This function retrieves **author IDs and contributor details** from `json_pl`, 
    assigns unique IDs, and creates relationships between authors and reviews.

    Args:
        json_pl (Dict[str, Any]): 
            The **preloaded JSON data** extracted from the page.

    Returns:
        Tuple[List[Author], List[URL], List[Review_Authors]]: 
            - `List[Author]`: A list of **newly discovered authors**.
            - `List[URL]`: A list of **new author profile URLs**.
            - `List[Review_Authors]`: A list of **Review-Author relationships**.

    Process:
        1. **Retrieves `review_id` and `authorIds`** from `json_pl`.
        2. **Creates `Review_Authors` relationships** for each author in the review.
        3. **Assigns unique IDs to author profile URLs**.
        4. **Stores newly discovered authors** in `g.authors_set`.

    Example:
        ```python
        new_authors, new_urls, review_authors = scrape_authors_data(json_pl)
        print(new_authors)  # [Author(author_id=123, name="John Doe", url_id=45), ...]
        print(new_urls)     # [URL(url_id=45, "https://pitchfork.com/staff/john-doe", ...)]
        print(review_authors) # [Review_Authors(review_id=101, author_id=123), ...]
        ```

    Notes:
        - If **no authors are found**, it assigns `author_id=0` and returns early.
        - Uses **`g.lock`** to ensure **thread safety** when modifying `g.authors_set`.
        - The function **does not modify the database directly**; it returns structured data.
    """
    new_urls = []
    new_authors = []
    review_authors = []

    review_id = general.dict_lookup(json_pl, ['coreDataLayer', 'content', 'contentId'])
    authorids = general.dict_lookup(json_pl, ['coreDataLayer', 'content', 'authorIds'])
    if authorids is None:
        review_authors.append(Review_Authors(review_id, 0))
        return new_authors, new_urls, review_authors

    authors_info1 = authorids.split(',')
    authors_info2 = general.dict_lookup(json_pl, ['review', 'contributors', 'author', 'items'])

    for i, author_id in enumerate(authors_info1):
        
        author_url = f'https://pitchfork.com{authors_info2[i]["url"]}'
        is_new_url, url_id = general.get_url_id(author_url, return_isnew=True)
        if is_new_url:
            new_urls.append(URL(url_id, author_url,None, None, None, 0, 0, 1, 0))

        review_authors.append(Review_Authors(review_id, author_id))
        with g.lock:
            if author_id not in g.authors_set:
                g.authors_set.add(author_id)
                author_name = authors_info2[i]['name']
                author_name = author_name if not author_name else (author_name.strip().replace('  ',' '))
                new_authors.append(Author(author_id, author_name, url_id))

    return new_authors, new_urls, review_authors

def __create_album_object(item:Dict[str,Any]) -> Album:
    """
    Creates an `Album` namedtuple from a dictionary containing album details.

    This function extracts metadata such as **album ID, name, publisher, release year, 
    score, and "best new music/reissue" status**, ensuring safe conversions.

    Args:
        item (Dict[str, Any]): 
            A dictionary containing album metadata extracted from JSON.

    Returns:
        Album: 
            A namedtuple representing the album.

    Process:
        1. Extracts values safely using `.get()` or `general.dict_lookup()`.
        2. Converts **numeric fields** (`releaseYear`, `score`) where applicable.
        3. Defaults `is_best_new_music` and `is_best_new_reissue` to `0` if missing.
        4. Ensures `pitchfork_score` is **converted to an integer**.

    Example:
        ```python
        album_data = {
            "albumId": 101,
            "dangerousHed": "Great Album",
            "publisher": "Some Label",
            "releaseYear": "2024",
            "musicRating": {"score": "8.2", "isBestNewMusic": "1"}}
        album = __create_album_object(album_data)
        print(album)
        # Output: Album(album_id=101, album="Great Album", publisher="Some Label", release_year=2024, pitchfork_score=82, is_best_new_music=1, is_best_new_reissue=0)
        ```

    Notes:
        - **Handles missing values** gracefully (e.g., missing `score` returns `None`).
        - Converts `score` from **float (out of 10) to integer (out of 100)**.
        - Ensures `is_best_new_music` and `is_best_new_reissue` are **always integers**.
        - Uses **`general.dict_lookup()`** to navigate nested structures.

    Raises:
        ValueError: If numeric conversions fail.
    """
    publisher = item.get('publisher', None)
    year = item.get('releaseYear', None)
    score = general.dict_lookup(item, ['musicRating','score'])
    is_best_new_music = general.dict_lookup(item, ['musicRating','isBestNewMusic'])
    is_best_new_reissue = general.dict_lookup(item, ['musicRating','isBestNewReissue'])

    return Album(
        album_id = item.get('albumId', None),
        album = item.get('dangerousHed', None),
        publisher = publisher if publisher else None,
        release_year = year if year else None,
        pitchfork_score = None if (score is None) else int(float(score * 10)),
        is_best_new_music = 0 if (is_best_new_music is None) else int(is_best_new_music),
        is_best_new_reissue = 0 if (is_best_new_reissue is None) else int(is_best_new_reissue),)

def scrape_albums_data(json_pl:Dict[str,Any]) -> Tuple[List[Album], List[Review_Albums]]:
    """
    This function retrieves **album IDs and metadata** from `json_pl`, assigns unique IDs, 
    and creates relationships between albums and reviews.

    Args:
        json_pl (Dict[str, Any]): 
            The **preloaded JSON data** extracted from the page.

    Returns:
        Tuple[List[Album], List[Review_Albums]]: 
            - `List[Album]`: A list of **newly discovered albums**.
            - `List[Review_Albums]`: A list of **Review-Album relationships**.

    Process:
        1. **Retrieves `review_id` and `itemsReviewed`** from `json_pl`.
        2. **Creates `Review_Albums` relationships** for each album in the review.
        3. **Assigns unique IDs to albums** and ensures they are **stored uniquely** in `g.albums_set`.

    Example:
        ```python
        new_albums, review_albums = scrape_albums_data(json_pl)
        print(new_albums)  # [Album(album_id=101, album="Great Album", publisher="Label", ...)]
        print(review_albums) # [Review_Albums(review_id=55, album_id=101), ...]
        ```

    Notes:
        - If **no albums are found**, the function returns empty lists.
        - Uses **`g.lock`** to ensure **thread safety** when modifying `g.albums_set`.
        - The function delegates album object creation to `__create_album_object()`.
    """
    review_id = general.dict_lookup(json_pl, ['coreDataLayer', 'content', 'contentId'])
    items_reviewed = general.dict_lookup(json_pl, ['review', 'multiReviewHeaderProps', 'itemsReviewed'])
    if not items_reviewed:
        return [], []

    new_albums = []
    review_albums = []

    for item in items_reviewed:
        album_id = item['albumId']

        review_albums.append(Review_Albums(review_id, album_id))
        
        with g.lock:
            if album_id not in g.albums_set:

                g.albums_set.add(album_id)
                new_albums.append(__create_album_object(item))
    return new_albums, review_albums

def scrape_artists_data(json_pl:Dict[str,Any]
    ) -> Tuple[List[URL],List[Artist],List[Genre],List[Review_Artists],List[Review_Artist_Genres], ]:
    """
    This function retrieves **artist names, their profile URLs, and associated genres** 
    from `json_pl`, assigns unique IDs, and creates relationships between artists, genres, and reviews.

    Args:
        json_pl (Dict[str, Any]): 
            The **preloaded JSON data** extracted from the page.

    Returns:
        Tuple[
            List[URL],              # List of newly discovered artist profile URLs.
            List[Artist],           # List of newly discovered artists.
            List[Genre],            # List of newly discovered genres.
            List[Review_Artists],   # Relationships between reviews and artists.
            List[Review_Artist_Genres] # Relationships between artists and genres.]

    Process:
        1. **Retrieves `review_id` and `artists`** from `json_pl`.
        2. **Creates `Review_Artists` relationships** for each artist in the review.
        3. **Assigns unique IDs to artist profile URLs**.
        4. **Stores newly discovered artists and genres** in `g.artists_dict` and `g.genres_dict`.

    Example:
        ```python
        new_urls, new_artists, new_genres, review_artists, review_artist_genres = scrape_artists_data(json_pl)
        print(new_artists)  # [Artist(artist_id=101, artist="John Doe", url_id=45), ...]
        print(new_genres)   # [Genre(genre_id=1, genre="Indie Rock"), ...]
        print(review_artists) # [Review_Artists(review_id=55, artist_id=101), ...]
        print(review_artist_genres) # [Review_Artist_Genres(review_id=55, artist_id=101, genre_id=1), ...]
        ```

    Notes:
        - If **no artists are found**, the function returns empty lists.
        - Uses **`g.lock`** and **`g.lock`** to ensure **thread safety**.
        - The function **does not modify the database directly**; it returns structured data.

    Raises:
        KeyError: If expected fields are missing from `json_pl`.
    """
    new_urls = []
    new_artists = []
    new_genres = []
    review_artists = []
    review_artist_genres = []

    review_id = general.dict_lookup(json_pl, ['coreDataLayer', 'content', 'contentId'])
    artists = general.dict_lookup(json_pl, ['review', 'headerProps', 'artists'])

    if artists is None:
        return new_artists, new_genres, new_urls, review_artists, review_artist_genres

    for a in artists:
        artist_name = a['name']

        with g.lock:
            if artist_name not in g.artists_dict:
                artist_id = g.artists_id_counter
                g.artists_dict[artist_name] = artist_id
                g.artists_id_counter += 1
            else:
                artist_id = g.artists_dict[artist_name]

        artist_url = f'https://pitchfork.com/{a["uri"]}'
        is_new_url, url_id = general.get_url_id(artist_url, return_isnew=True)
        if is_new_url:
            new_urls.append(URL(url_id, artist_url, None, None, None, 0, 0, 0, 1))

        new_artists.append(Artist(artist_id, artist_name, url_id))
        review_artists.append(Review_Artists(review_id, artist_id))

        for gen in a['genres']:
            genre = gen['node']['name']

            with g.lock:
                if genre not in g.genres_dict:
                    genre_id = g.genres_id_counter
                    g.genres_dict[genre] = genre_id
                    g.genres_id_counter += 1
                    new_genres.append(Genre(genre_id, genre))
                else:
                    genre_id = g.genres_dict[genre]

            review_artist_genres.append(Review_Artist_Genres(review_id, artist_id, genre_id))

    return new_artists, new_genres, new_urls, review_artists, review_artist_genres

def scrape_entities_data(json_pl:Dict[str,Any]) -> Tuple[List[Entity], List[Review_Entities]]:
    """
    This function retrieves **named entities** (e.g., people, locations, topics) 
    from `json_pl`, assigns unique IDs, and creates relationships between reviews 
    and the entities they mention.

    Args:
        json_pl (Dict[str, Any]): 
            The **preloaded JSON data** extracted from the page.

    Returns:
        Tuple[
            List[Entity],          # List of newly discovered entities.
            List[Review_Entities]  # Relationships between reviews and entities.
        ]

    Process:
        1. **Retrieves `review_id` and `entities`** from `json_pl`.
        2. **Creates `Review_Entities` relationships** for each entity found.
        3. **Assigns unique IDs to newly discovered entities**.

    Example:
        ```python
        new_entities, review_entities = scrape_entities_data(json_pl)
        print(new_entities)  # [Entity(entity_id=1, entity="David Bowie"), ...]
        print(review_entities) # [Review_Entities(review_id=55, entity_id=1, score=0.9), ...]
        ```

    Notes:
        - If **no entities are found**, the function returns an empty list for `new_entities`
          and a default `Review_Entities(review_id, entity_id=0, score=None)`.
        - Uses **`g.lock`** to ensure **thread safety** when modifying `g.entities_dict`.
        - The function **does not modify the database directly**; it returns structured data.

    Raises:
        KeyError: If expected fields are missing from `json_pl`.
    """
    review_id = general.dict_lookup(json_pl, ['coreDataLayer', 'content', 'contentId'])
    entities = general.dict_lookup(json_pl, ['content4d', 'entities'])

    if entities is None:
        return [], [Review_Entities(review_id, 0, None)]

    new_entities = []
    review_entities = []

    for item in entities:
        entity = item['name']
        score = item['score']

        with g.lock:
            if entity not in g.entities_dict:
                entity_id = g.entities_id_counter
                g.entities_dict[entity] = entity_id
                g.entities_id_counter += 1
                new_entities.append(Entity(entity_id, entity))
            else:
                entity_id = g.entities_dict[entity]

        review_entities.append(Review_Entities(review_id, entity_id, score))

    return new_entities, review_entities

def scrape_keywords_data(json_pl:Dict[str,Any]) -> Tuple[List[Keyword], List[Review_Keywords]]:
    """
    This function retrieves **keywords** (e.g., themes, topics, tags) 
    from `json_pl`, assigns unique IDs, and creates relationships between 
    reviews and the keywords they are associated with.

    Args:
        json_pl (Dict[str, Any]): 
            The **preloaded JSON data** extracted from the page.

    Returns:
        Tuple[
            List[Keyword],        # List of newly discovered keywords.
            List[Review_Keywords] # Relationships between reviews and keywords.]

    Process:
        1. **Retrieves `review_id` and `keywords`** from `json_pl`.
        2. **Creates `Review_Keywords` relationships** for each keyword found.
        3. **Assigns unique IDs to newly discovered keywords**.

    Example:
        ```python
        new_keywords, review_keywords = scrape_keywords_data(json_pl)
        print(new_keywords)  # [Keyword(keyword_id=1, keyword="Experimental Rock"), ...]
        print(review_keywords) # [Review_Keywords(review_id=55, keyword_id=1, score=0.85), ...]
        ```

    Notes:
        - If **no keywords are found**, the function returns an empty list for `new_keywords`
          and a default `Review_Keywords(review_id, keyword_id=0, score=None)`.
        - Uses **`g.lock`** to ensure **thread safety** when modifying `g.keywords_dict`.
        - The function **does not modify the database directly**; it returns structured data.
    """
    review_id = general.dict_lookup(json_pl, ['coreDataLayer', 'content', 'contentId'])
    keywords = general.dict_lookup(json_pl, ['content4d', 'keywords', 'list'])

    if keywords is None:
        return [], [Review_Keywords(review_id, 0, None)]

    new_keywords = []
    review_keywords = []
    for item in keywords:
        keyword = item['keyword']
        score = item['score']

        with g.lock:
            if keyword not in g.keywords_dict:
                keyword_id = g.keywords_id_counter
                g.keywords_dict[keyword] = keyword_id
                g.keywords_id_counter += 1
                new_keywords.append(Keyword(keyword_id, keyword))
            else:
                keyword_id = g.keywords_dict[keyword]

        review_keywords.append(Review_Keywords(review_id, keyword_id, score))

    return new_keywords, review_keywords

## Orchestrate the scraping of an album review
def scrape_json_data(get_connection:SQLite3ConnectionGenerator, url_id:int, url:str, timeout:float=0.5) -> Tuple[Dict[str, Any], Dict[str, Any]]:
    """
    This function retrieves and parses two types of JSON metadata:
    - **Preloaded state JSON** (`window.__PRELOADED_STATE__`): Contains rich metadata about the page.
    - **Linked data JSON-LD** (`application/ld+json`): Provides structured semantic data.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        url_id (int): 
            A unique identifier for the URL.
        url (str): 
            The webpage URL to scrape.
        timeout (float, optional): 
            The timeout in seconds before retrying a failed request. Defaults to `0.5`.

    Returns:
        Tuple[
            Optional[Dict[str, Any]],  # Extracted preloaded JSON data (or `None` on failure)
            Optional[Dict[str, Any]]   # Extracted linked data JSON-LD (or `None` on failure)]

    Process:
        1. **Fetches the webpage using `general.parse_url()`**.
        2. **Parses the preloaded state JSON** using `extract_json_preload_data()`.
        3. **Parses the linked data JSON-LD** using `extract_json_linked_data_album()`.
        4. **Logs failures** if extraction fails at any stage.

    Example:
        ```python
        json_preload, json_linked_data = scrape_json_data(get_connection, url_id=101, url="https://pitchfork.com/review/album-name")
        print(json_preload.keys())   # Example output: ['coreDataLayer', 'review', ...]
        print(json_linked_data["@type"])  # Example output: "MusicAlbum"
        ```

    Notes:
        - If **no connection can be established**, both return values will be `None`.
        - Uses **`traceback.format_exc()`** to capture and log exceptions.
        - The function **does not modify the database**; it logs failures but only returns data.

    Raises:
        Exception: Any unexpected error is logged, and `None` is returned.
    """
    soup = general.parse_url(get_connection, url=url, format='html.parser', timeout=timeout)

    if soup is None:
        message = traceback.format_exc()
        db.log_event(get_connection, url_id=url_id, process="Couldn't stablish connection", success=0, message=message)
        return None, None
    
    try:
        json_preload = extract_json_preload_data(soup)
    except:
        message = traceback.format_exc()
        db.log_event(get_connection, url_id=url_id, process='Failed at parsing json preload data', success=0, message=message)
        return None, None

    try:
        json_linked_data = extract_json_linked_data_album(soup)
    except:
        message = traceback.format_exc()
        db.log_event(get_connection, url_id=url_id, process='Failed at parsing json linked data', success=0, message=message)
        return None, None
    
    return json_preload, json_linked_data

def scrape_section(
    get_connection: SQLite3ConnectionGenerator, 
    url_id: int, 
    section: str, 
    func: Callable[..., Tuple[Any, ...]], 
    *inputs: Tuple[Dict[str, Any]],
    verbose:bool = False
    ) -> None:
    """
    Executes a scraping function for a specific section and inserts the extracted data into the database.

    This function:
    - Calls a section-specific scraping function (e.g., `scrape_authors_data`, `scrape_entities_data`).
    - Handles any errors that occur during the scraping process.
    - Inserts the extracted data into the database.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        url_id (int): 
            A unique identifier for the URL.
        section (str): 
            The name of the section being scraped (e.g., "authors", "albums").
        func (Callable[..., Tuple[Any, ...]]): 
            The function responsible for scraping the section.
        *inputs (Tuple[Dict[str, Any]]): 
            The JSON input(s) to pass to the section scraping function.

    Returns:
        None: This function does not return a value but logs failures if they occur.

    Process:
        1. **Calls the scraping function (`func`)** with the provided inputs.
        2. **Catches and logs errors** if scraping fails.
        3. **Iterates through the returned data** and inserts each set of records into the database.
        4. **Handles errors that occur during database insertion**.

    Example:
        ```python
        scrape_section(get_connection, url_id=101, section="authors", func=scrape_authors_data, json_pl)
        ```

    Notes:
        - If the scraping function **fails**, an error message is logged, and the function exits.
        - If the database insertion **fails**, an error message is logged, but the function continues processing other data.
        - The function **does not modify the database directly**; it calls `db.insert_named_tuples()`.

    Raises:
        Exception: Any unexpected error is logged, and the function exits early.
    """
    try:
        list_of_lists = func(*inputs)
    except:
        message = traceback.format_exc()
        db.log_event(get_connection, url_id=url_id, process=f'Failed at parsing {section} data', success=0, message=message)
        return

    for list_of_tuples in list_of_lists:
        try:
            db.insert_named_tuples(get_connection, list_of_tuples, verbose=verbose)
        except:
            message = traceback.format_exc()
            db.log_event(get_connection, url_id=url_id, process=f'Failed at inserting {section} data', success=0, message=message)

def scrape_album_review(get_connection:SQLite3ConnectionGenerator, url_id:int, url:str, timeout:float=0.5, verbose:bool = False) -> None:
    """
    This function extracts JSON metadata from the provided `url`, then 
    processes various sections of the review, including **albums, authors, 
    artists, entities, keywords, and the main review itself**.

    Args:
        get_connection (SQLite3ConnectionGenerator): 
            A function that returns an SQLite connection.
        url_id (int): 
            A unique identifier for the URL.
        url (str): 
            The webpage URL of the album review.
        timeout (float, optional): 
            The timeout in seconds before retrying a failed request. Defaults to `0.5`.

    Returns:
        None: This function does not return a value but logs failures if they occur.

    Process:
        1. **Fetches and parses JSON metadata** from the review page using `scrape_json_data()`.
        2. **Checks if JSON extraction was successful**; if not, exits early.
        3. **Defines a mapping of section names to functions** responsible for extracting each section.
        4. **Calls `scrape_section()`** for each section, passing the appropriate JSON data.

    Example:
        ```python
        scrape_album_review(get_connection, url_id=101, url="https://pitchfork.com/reviews/albums/some-album")
        ```

    Notes:
        - If **JSON extraction fails**, the function exits early without processing sections.
        - Uses **a dictionary (`sections`)** to dynamically route processing to the correct function.
        - Calls `scrape_section()` to handle each section separately.

    Raises:
        Exception: Any unexpected error is logged, and the function exits early.
    """
    json_preload, json_linked_data = scrape_json_data(get_connection, url_id, url, timeout=timeout)

    if (json_preload is None) or (json_linked_data is None):
        return

    sections = {
        'review' : scrape_review_data,
        'albums' : scrape_albums_data,
        'authors' : scrape_authors_data,
        'artists' : scrape_artists_data,
        'entities' : scrape_entities_data,
        'keywords' : scrape_keywords_data,}

    for section, func in sections.items():
        inputs = (json_preload,) if section != 'review' else (json_preload, json_linked_data)
        scrape_section(get_connection, url_id, section, func, *inputs, verbose=verbose)
