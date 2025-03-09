DROP VIEW IF EXISTS Q_artists;

CREATE VIEW Q_artists AS

WITH artist_urls AS (SELECT url_id, url FROM urls WHERE url LIKE '%/staff/%')

SELECT 
   a.artist_id,
   a.artist,
   u.url
FROM artists a 
JOIN artist_urls u ON a.url_id = u.url_id;