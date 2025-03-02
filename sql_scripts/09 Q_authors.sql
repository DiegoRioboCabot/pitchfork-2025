DROP VIEW IF EXISTS Q_authors;
CREATE VIEW Q_authors AS

WITH author_urls AS (SELECT url_id, url FROM urls WHERE is_author = 1)

SELECT 
    a.author_id,
    a.author,
    mrat.author_type1,
    mrat.author_type2,
    ab.date_pub AS date_bio,
    ab.revisions,
    u.url,
    ab.bio
FROM authors a
JOIN author_bios ab ON a.author_id = ab.author_id
JOIN Q_most_recent_author_types mrat ON a.author_id = mrat.author_id
JOIN author_urls u ON a.url_id = u.url_id;