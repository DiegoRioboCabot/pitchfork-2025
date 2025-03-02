DROP VIEW IF EXISTS Q_review_artist_genres;
CREATE VIEW Q_review_artist_genres AS
SELECT 
    l.review_id, 
    l.artist_id, 
    '[' || GROUP_CONCAT(r.genre, '; ') || ']' AS genres,
    COUNT(r.genre_id) AS no_genres
FROM review_artist_genres l
JOIN genres r ON l.genre_id = r.genre_id
GROUP BY l.review_id, l.artist_id