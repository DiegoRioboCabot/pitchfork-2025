DROP VIEW IF EXISTS Q_most_recent_author_types;
CREATE VIEW Q_most_recent_author_types AS

WITH most_recent_author_types AS (
   SELECT *
   FROM author_type_evolution AS a
   WHERE as_of_date = (
       SELECT MAX(as_of_date)
       FROM author_type_evolution
       WHERE author_id = a.author_id)
),

most_recent_author_types_named AS (
SELECT 
   mrat.author_id,
   at1.author_type AS author_type1,
   at2.author_type AS author_type2
FROM most_recent_author_types mrat
JOIN author_types at1 ON at1.author_type_id = mrat.author_type1_id
JOIN author_types at2 ON at2.author_type_id = mrat.author_type2_id)


SELECT * FROM most_recent_author_types_named;