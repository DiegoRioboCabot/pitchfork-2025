DROP VIEW IF EXISTS Q_review_albums;
CREATE VIEW Q_review_albums AS
WITH q_albums AS (
   SELECT
      ra.review_id,
      a.*
   FROM review_albums ra
   JOIN albums a ON ra.album_id = a.album_ID)
   

SELECT
   review_id,
   COUNT(album) AS no_albums,
   AVG(pitchfork_score) AS score_avg,
   MAX(pitchfork_score) AS score_max,
   MIN(pitchfork_score) AS score_min,
   '[' || GROUP_CONCAT(pitchfork_score, '; ') || ']' AS scores,
   '[' || GROUP_CONCAT(release_year, '; ') || ']' AS years,
   '[' || GROUP_CONCAT(album, '; ') || ']' AS albums,
   '[' || GROUP_CONCAT(publisher, '; ') || ']' AS publishers
FROM q_albums

GROUP BY review_id