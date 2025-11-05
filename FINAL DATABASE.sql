-- Bảng 1: RatingsCompare
-- Lưu trữ điểm đánh giá từ giới phê bình và khán giả
CREATE TABLE RatingsCompare (
    movie_id INT PRIMARY KEY,
    title NVARCHAR(255),
    genre NVARCHAR(100),
    critics_score FLOAT,
    audience_score FLOAT,
    review_count INT,
    release_year INT
);

-- Bảng 2: TopRatedMovies
-- Lưu trữ thông tin các phim được đánh giá cao nhất từ nhiều nguồn
CREATE TABLE TopRatedMovies (
    movie_id INT PRIMARY KEY,
    title NVARCHAR(255),
    genre NVARCHAR(100),
    imdb_rating FLOAT,
    rt_rating FLOAT,
    metacritic_rating FLOAT,
    avg_score FLOAT,
    vote_count INT,
    release_year INT
);

-- Bảng 3: SentimentReviews
-- Lưu trữ kết quả phân tích cảm xúc của từng bài đánh giá
-- Ghi chú: movie_id ở đây không phải là khóa chính để cho phép nhiều review cho một phim.
-- Một cột review_id đã được thêm vào làm khóa chính.
CREATE TABLE SentimentReviews (
    review_id INT PRIMARY KEY, -- Cân nhắc sử dụng kiểu dữ liệu tự tăng (auto-increment)
    movie_id INT,
    title NVARCHAR(255),
    genre NVARCHAR(100),
    review_text NVARCHAR(MAX), -- Tùy thuộc vào CSDL, có thể là TEXT, CLOB, hoặc VARCHAR(MAX)
    sentiment_label NVARCHAR(20),
    sentiment_score FLOAT,
    language NVARCHAR(20)
);