import threading

# These quasi global variables are shared between scraper modules and CPU threads to ensure consistency

lock = threading.Lock() # To prevent racing conditions

albums_set = set()

# For some articles, Pitchfork has given "no author" an ID.
# I don't know if that's relevant or not (yet). But I might as well track it
authors_set = set([0, '592604b17fd06e5349102f34'])

urls_dict = {None: 0}
urls_id_counter = 1

artists_dict = {None: 0}
artists_id_counter = 1

labels_dict = {None: 0}
labels_id_counter = 1

genres_dict = {None: 0}
genres_id_counter = 1

keywords_dict = {None: 0}
keywords_id_counter = 1

entities_dict = {None: 0}
entities_id_counter = 1

author_types_dict = {None: 0}
author_types_id_counter = 1
