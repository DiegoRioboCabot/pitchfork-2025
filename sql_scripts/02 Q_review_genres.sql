DROP VIEW IF EXISTS Q_review_genres;
CREATE VIEW Q_review_genres AS

WITH unique_genres AS (
    SELECT 
    DISTINCT l.review_id, r.genre_id, r.genre
    FROM review_artist_genres l
    JOIN genres r ON l.genre_id = r.genre_id)


SELECT 
    review_id, 
    '[' || GROUP_CONCAT(genre, '; ') || ']' AS genres,
    COUNT(genre_id) AS no_genres
FROM unique_genres
GROUP BY review_id;