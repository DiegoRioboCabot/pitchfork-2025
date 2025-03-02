DROP VIEW IF EXISTS Q_review_artists;
CREATE VIEW Q_review_artists AS
SELECT 
    l.review_id, 
    '[' || GROUP_CONCAT(r.artist, '; ') || ']' AS artists,
    COUNT(r.artist_id) AS no_artists
FROM review_artists l
JOIN artists r ON l.artist_id = r.artist_id
GROUP BY l.review_id;