DROP VIEW IF EXISTS Q_review_authors;

CREATE VIEW Q_review_authors AS
SELECT 
    l.review_id, 
    '[' || GROUP_CONCAT(r.author, '; ') || ']' AS authors,
    COUNT(r.author_id) AS no_authors
FROM review_authors l
JOIN authors r ON l.author_id = r.author_id
GROUP BY l.review_id;