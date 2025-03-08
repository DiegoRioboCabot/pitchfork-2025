from __future__ import annotations

import pytz
import sqlite3
import threading
import datetime as dt

from typing import Callable, Union, NamedTuple, Dict,Any
from collections import namedtuple


namedtuples = ['URL', 'Label', 'Genre', 'Keyword', 'Entity', 'Artist', 'Album', 'Review', 
'Review_Labels', 'Review_Authors', 'Review_Artists', 'Review_Entities', 'Review_Keywords', 'Review_Albums', 'Review_Artist_Genres', 
'Author', 'Author_Bio', 'Author_Type', 'Author_Type_Evolution', 'scraping_events']

__all__ = namedtuples + ['SQLite3ConnectionGenerator', 'DatabaseRow']

URL = namedtuple('urls', ['url_id', 'url', 'year', 'month', 'week', 'is_review', 'is_album', 'is_author', 'is_artist'])

Label = namedtuple('labels', ['label_id', 'label'])
Genre = namedtuple('genres', ['genre_id', 'genre'])
Keyword = namedtuple('keywords', ['keyword_id', 'keyword',])
Entity = namedtuple('entities', ['entity_id', 'entity',])

Artist = namedtuple('artists', ['artist_id', 'artist', 'url_id',])
Album = namedtuple('albums', ['album_id', 'album', 'publisher', 'release_year', 'pitchfork_score', 'is_best_new_music', 'is_best_new_reissue'])
Review = namedtuple('reviews', ['review_id', 'revisions', 'url_id', 'body', 'description', 'date_pub', 'date_mod'])

Review_Labels = namedtuple('review_labels', ['review_id', 'label_id'])
Review_Authors = namedtuple('review_authors', ['review_id', 'author_id'])
Review_Artists =  namedtuple('review_artists', ['review_id', 'artist_id'])
Review_Entities = namedtuple('review_entities', ['review_id', 'entity_id', 'score'])
Review_Keywords = namedtuple('review_keywords', ['review_id', 'keyword_id', 'score'])
Review_Albums = namedtuple('review_albums', ['review_id', 'album_id'])

Review_Artist_Genres =  namedtuple('review_artist_genres', ['review_id', 'artist_id', 'genre_id'])

Author = namedtuple('authors', ['author_id','author','url_id'])
Author_Bio = namedtuple('author_bios', ['author_id', 'date_pub', 'revisions', 'bio'])
Author_Type = namedtuple('author_types', ['author_type_id','author_type'])
Author_Type_Evolution = namedtuple('author_type_evolution', ['author_id', 'author_type1_id', 'author_type2_id', 'as_of_date', ])

class scraping_events(NamedTuple):
    timestamp: dt.datetime
    url_id: str = ""
    process: str = ""
    success: int = 1
    message: str = ""

    @staticmethod
    def default_timestamp():
        return dt.datetime.now(pytz.timezone("Europe/Vienna")).isoformat()

DatabaseRow = Union[
    URL, Label, Genre, Keyword, Entity, Artist, Album, Review,
    Review_Labels, Review_Authors, Review_Artists, Review_Entities, 
    Review_Keywords, Review_Albums, Review_Artist_Genres, 
    Author, Author_Bio, Author_Type, Author_Type_Evolution, None
]

SQLite3ConnectionGenerator = Callable[[], 'sqlite3.Connection']

class TSCDict():
    """
        This is a Thread-Safe Counter Dictionary.
        When using the bracket [ ] operator, you automatically
        either retrieve or add a new key to the dictionary.
        The values are an index starting from 1 and incrementing by 1.

        It always returns a tuple (is_new:bool, self[key]), to track if
        the looked up key was a new one or already existing one.

    """

    def __init__(self, initial_data: Dict[Any,Any] = {None:0}):
        self.__data = initial_data
        self.__counter = 1
        self.__lock = threading.Lock()


    @property
    def data(self):
        with self.__lock:
            return self.__data
    
    @data.setter
    def data(self):
        raise Exception('Use [ ] operator to set and retrieve data')
    

    def __getitem__(self, k:Any):
        with self.__lock:

            is_new = k not in self.__data

            if is_new:
                self.__data[k] = self.__counter
                self.__counter += 1

            return is_new, self.__data[k]
        
    def __setitem__(self, k:Any, v:Any):
        raise Exception('This is a counter-dict. The values are automatically set by internal clockwork.')
    
    def __contains__(self, k:Any):
        with self.__lock:
            return k in self.__data
    
    def __delitem__(self, k:Any):
        with self.__lock:
            del self.data[k]

    def get(self, k:Any, default=None):
        raise Exception('This is a counter-dict. Use the [ ] to retrieve or add data.')
    
    def keys(self):
        with self.__lock:
            return self.__data.keys()

    def values(self):
        with self.__lock:
            return self.__data.values()
        
    def items(self):
        with self.__lock:
            return self.__data.items()
        
    def __repr__(self):
        with self.__lock:
            return repr(self.__data)