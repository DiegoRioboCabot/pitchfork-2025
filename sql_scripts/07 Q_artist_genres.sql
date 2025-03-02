DROP VIEW IF EXISTS q_artist_genres;
CREATE VIEW Q_artist_genres AS

SELECT 
    a.artist_id,
    a.artist,
    unique_genres_per_artist.genres,
    unique_genres_per_artist.no_genres
FROM
        (SELECT 
            artist_id, 
            '[' || GROUP_CONCAT(genre, '; ') || ']' AS genres,
            COUNT(genre_id) AS no_genres
        FROM (SELECT 
            DISTINCT l.artist_id, r.genre_id, r.genre
            FROM review_artist_genres l
            JOIN genres r ON l.genre_id = r.genre_id
        ) AS u
GROUP BY artist_id) AS unique_genres_per_artist
JOIN artists a ON a.artist_id = unique_genres_per_artist.artist_id;