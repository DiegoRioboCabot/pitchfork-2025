DROP VIEW IF EXISTS Q_review_labels;
CREATE VIEW Q_review_labels AS
SELECT 
    rl.review_id, 
    '[' || GROUP_CONCAT(l.label, '; ') || ']' AS labels,
    COUNT(l.label_id) AS no_labels
FROM review_labels rl
JOIN labels l ON rl.label_id = l.label_id
GROUP BY rl.review_id;