DROP VIEW IF EXISTS Q_reviews;

CREATE VIEW Q_reviews AS
SELECT
   rev.review_id,
   rev.revisions,
   rev.date_pub,
   rev.date_mod,
   u.url,
   alb.score_avg,
   alb.score_max,
   alb.score_min,
   alb.scores,
   alb.years,
   alb.albums,
   alb.publishers,
   aut.authors,
   lab.labels,
   gen.genres,
   alb.no_albums,
   aut.no_authors,
   lab.no_labels,
   gen.no_genres,
   LENGTH(rev.description) AS len_description,
   LENGTH(rev.body) AS len_body,
   rev.description,
   rev.body
FROM reviews rev
JOIN Q_review_albums alb ON rev.review_id = alb.review_id
JOIN Q_review_authors aut ON rev.review_id = aut.review_id
JOIN Q_review_labels lab ON rev.review_id = lab.review_id
JOIN Q_review_genres gen ON rev.review_id = gen.review_id
JOIN urls u ON rev.url_id = u.url_id