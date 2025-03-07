import threading

# These quasi global variables are shared between scraper modules and CPU threads to ensure consistency

albums_set = set()
albums_lock = threading.Lock()

# For some articles, Pitchfork has given "no author" an ID.
# I don't know if that's relevant or not (yet). But I might as well track it
authors_set = set([0, '592604b17fd06e5349102f34'])
authors_lock = threading.Lock()

urls_dict = {None: 0}
urls_id_counter = 1
urls_lock = threading.Lock()

artists_dict = {None: 0}
artists_id_counter = 1
artists_lock = threading.Lock()

labels_dict = {None: 0}
labels_id_counter = 1
labels_lock = threading.Lock()

genres_dict = {None: 0}
genres_id_counter = 1
genres_lock = threading.Lock()

keywords_dict = {None: 0}
keywords_id_counter = 1
keywords_lock = threading.Lock()

entities_dict = {None: 0}
entities_id_counter = 1
entities_lock = threading.Lock()

author_types_dict = {None: 0}
author_types_id_counter = 1
author_types_lock = threading.Lock()
