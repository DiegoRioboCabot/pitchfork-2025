-- Ensure metadata table exists
CREATE TABLE IF NOT EXISTS metadata (
    table_name TEXT,
    column_name TEXT,
    is_primary_key INTEGER,
    is_foreign_key INTEGER,
    description TEXT,
    PRIMARY KEY (table_name, column_name)
);

-- 🎵 Albums Table
INSERT INTO metadata VALUES ('albums', NULL, NULL, NULL, 'Album information retrieved from a single Pitchfork album review.');
INSERT INTO metadata VALUES ('albums', 'album_id', 1, NULL, 'Primary key. Unique identifier for each album.');
INSERT INTO metadata VALUES ('albums', 'publisher', NULL, NULL, 'Record label or company that published the album.');
INSERT INTO metadata VALUES ('albums', 'release_year', NULL, NULL, 'Year the album was released.');
INSERT INTO metadata VALUES ('albums', 'pitchfork_score', NULL, NULL, 'Pitchfork’s assigned score (scaled 0-100).');
INSERT INTO metadata VALUES ('albums', 'is_best_new_music', NULL, NULL, 'Indicates if the album received Pitchfork’s “Best New Music” recognition.');
INSERT INTO metadata VALUES ('albums', 'is_best_new_reissue', NULL, NULL, 'Indicates if the album received “Best New Reissue” recognition.');

-- 🎨 Artists Table
INSERT INTO metadata VALUES ('artists', NULL, NULL, NULL, 'Collection of found artists throughout album reviews.');
INSERT INTO metadata VALUES ('artists', 'artist_id', 1, NULL, 'Primary key. Unique identifier for each artist.');
INSERT INTO metadata VALUES ('artists', 'url_id', NULL, 1, 'Foreign key. Links artist to their Pitchfork profile URL.');

-- ✍ Authors Table
INSERT INTO metadata VALUES ('authors', NULL, NULL, NULL, 'Collection of found article authors throughout album reviews.');
INSERT INTO metadata VALUES ('authors', 'author_id', 1, NULL, 'Primary key. Unique identifier for each author.');
INSERT INTO metadata VALUES ('authors', 'url_id', NULL, 1, 'Foreign key. Links author to their Pitchfork profile URL.');

-- 📜 Author Biographies
INSERT INTO metadata VALUES ('author_bios', NULL, NULL, NULL, 'Extracted biography from the authors’ dedicated webpage.');
INSERT INTO metadata VALUES ('author_bios', 'author_id', 1, 1, 'Primary key & foreign key. Links to `authors.author_id`.');
INSERT INTO metadata VALUES ('author_bios', 'date_pub', NULL, NULL, 'Date of publication.');
INSERT INTO metadata VALUES ('author_bios', 'revisions', NULL, NULL, 'Number of revisions made to the biography.');
INSERT INTO metadata VALUES ('author_bios', 'bio', NULL, NULL, 'Biography text extracted from the webpage.');

-- 🏷️ Author Types
INSERT INTO metadata VALUES ('author_types', NULL, NULL, NULL, 'Different roles for authors (Editor, Contributor, Writer, etc.).');
INSERT INTO metadata VALUES ('author_types', 'author_type_id', 1, NULL, 'Primary key. Unique identifier for each author type.');
INSERT INTO metadata VALUES ('author_types', 'author_type', NULL, NULL, 'Descriptive label of the author type.');

-- 🔄 Author Role Evolution Over Time
INSERT INTO metadata VALUES ('author_type_evolution', NULL, NULL, NULL, 'Tracks changes in an author’s role over time.');
INSERT INTO metadata VALUES ('author_type_evolution', 'author_id', 1, 1, 'Primary & foreign key. Links to `authors.author_id`.');
INSERT INTO metadata VALUES ('author_type_evolution', 'author_type1_id', NULL, 1, 'Foreign key. First observed role in the evolution.');
INSERT INTO metadata VALUES ('author_type_evolution', 'author_type2_id', NULL, 1, 'Foreign key. Second observed role in the evolution.');
INSERT INTO metadata VALUES ('author_type_evolution', 'as_of_date', 1, NULL, 'Primary key. Timestamp of when the author had these roles.');

-- 📝 Reviews Table
INSERT INTO metadata VALUES ('reviews', NULL, NULL, NULL, 'Main reviews table. Each article has a unique ID.');
INSERT INTO metadata VALUES ('reviews', 'review_id', 1, NULL, 'Primary key. Unique identifier for each review.');
INSERT INTO metadata VALUES ('reviews', 'url_id', NULL, 1, 'Foreign key. Links to the `urls` table.');
INSERT INTO metadata VALUES ('reviews', 'body', NULL, NULL, 'Full review text.');
INSERT INTO metadata VALUES ('reviews', 'description', NULL, NULL, 'Short summary of the review.');
INSERT INTO metadata VALUES ('reviews', 'date_pub', NULL, NULL, 'Date when the review was published.');
INSERT INTO metadata VALUES ('reviews', 'date_mod', NULL, NULL, 'Date when the review was last modified.');

-- 📀 Review-Album Relationship
INSERT INTO metadata VALUES ('review_albums', NULL, NULL, NULL, 'Maps one or more albums to a single review.');
INSERT INTO metadata VALUES ('review_albums', 'review_id', 1, 1, 'Primary & foreign key. Links to `reviews.review_id`.');
INSERT INTO metadata VALUES ('review_albums', 'album_id', 1, 1, 'Primary & foreign key. Links to `albums.album_id`.');

-- 🎭 Review-Labels Relationship
INSERT INTO metadata VALUES ('review_labels', NULL, NULL, NULL, 'Maps one or more labels to a single review.');
INSERT INTO metadata VALUES ('review_labels', 'review_id', 1, 1, 'Primary & foreign key. Links to `reviews.review_id`.');
INSERT INTO metadata VALUES ('review_labels', 'label_id', 1, 1, 'Primary & foreign key. Links to `labels.label_id`.');

-- ✍ Review-Authors Relationship
INSERT INTO metadata VALUES ('review_authors', NULL, NULL, NULL, 'Maps one or more authors to a single review.');
INSERT INTO metadata VALUES ('review_authors', 'review_id', 1, 1, 'Primary & foreign key. Links to `reviews.review_id`.');
INSERT INTO metadata VALUES ('review_authors', 'author_id', 1, 1, 'Primary & foreign key. Links to `authors.author_id`.');

-- 🏛️ Review-Entities Relationship
INSERT INTO metadata VALUES ('review_entities', NULL, NULL, NULL, 'Entities recognized within a review.');
INSERT INTO metadata VALUES ('review_entities', 'review_id', 1, 1, 'Primary & foreign key. Links to `reviews.review_id`.');
INSERT INTO metadata VALUES ('review_entities', 'entity_id', 1, 1, 'Primary & foreign key. Links to `entities.entity_id`.');
INSERT INTO metadata VALUES ('review_entities', 'score', NULL, NULL, 'Relevance score of the entity.');

-- 🔑 Review-Keywords Relationship
INSERT INTO metadata VALUES ('review_keywords', NULL, NULL, NULL, 'Keywords identified within a review.');
INSERT INTO metadata VALUES ('review_keywords', 'review_id', 1, 1, 'Primary & foreign key. Links to `reviews.review_id`.');
INSERT INTO metadata VALUES ('review_keywords', 'keyword_id', 1, 1, 'Primary & foreign key. Links to `keywords.keyword_id`.');
INSERT INTO metadata VALUES ('review_keywords', 'score', NULL, NULL, 'Relevance score of the keyword.');

-- 🌐 URLs Table
INSERT INTO metadata VALUES ('urls', NULL, NULL, NULL, 'Tracks every page URL scraped or found during scraping.');
INSERT INTO metadata VALUES ('urls', 'url_id', 1, NULL, 'Primary key. Unique identifier for each URL.');
INSERT INTO metadata VALUES ('urls', 'url', NULL, NULL, 'The full URL link.');
INSERT INTO metadata VALUES ('urls', 'year', NULL, NULL, 'Year of the URL’s sitemap entry.');
INSERT INTO metadata VALUES ('urls', 'month', NULL, NULL, 'Month of the URL’s sitemap entry.');
INSERT INTO metadata VALUES ('urls', 'week', NULL, NULL, 'Week of the URL’s sitemap entry.');

-- 📜 Scraping Events Log
INSERT INTO metadata VALUES ('scraping_events', NULL, NULL, NULL, 'Tracks scraping activities and errors.');
INSERT INTO metadata VALUES ('scraping_events', 'timestamp', NULL, NULL, 'Timestamp of the event.');
INSERT INTO metadata VALUES ('scraping_events', 'url_id', NULL, 1, 'Foreign key. Links to `urls.url_id`.');
INSERT INTO metadata VALUES ('scraping_events', 'process', NULL, NULL, 'Describes the scraping process step.');
INSERT INTO metadata VALUES ('scraping_events', 'success', NULL, NULL, 'Indicates success (1) or failure (0).');
INSERT INTO metadata VALUES ('scraping_events', 'message', NULL, NULL, 'Error or success message.');



