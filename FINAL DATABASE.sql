
SELECT * FROM RatingsCompare;
SELECT * FROM TopRatedMovies;
SELECT * FROM SentimentReviews;


SELECT 
    Movie_id,
    AVG(Sentiment_score) AS Sentiment_avg
FROM SentimentReviews
GROUP BY Movie_id

SELECT 
    t.Movie_id,
    t.Title,
    t.Genre,
    t.Avg_score,
    t.Vote_count,
    t.Release_year,
    s.Sentiment_avg
FROM TopRatedMovies t
JOIN (
    SELECT Movie_id, AVG(Sentiment_score) AS Sentiment_avg
    FROM SentimentReviews
    GROUP BY Movie_id
) s ON t.Movie_id = s.Movie_id