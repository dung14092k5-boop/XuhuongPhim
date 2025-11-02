-- =========================================
-- Database: XUHUONGPHIM
-- =========================================
IF DB_ID('XUHUONGPHIM') IS NULL
BEGIN
    CREATE DATABASE XUHUONGPHIM;
END
GO

USE XUHUONGPHIM;
GO

-- =========================================
-- Table: People (Diễn viên, Đạo diễn)
-- =========================================
IF OBJECT_ID('People', 'U') IS NOT NULL DROP TABLE People;
GO
CREATE TABLE People (
    person_id INT IDENTITY(1,1) PRIMARY KEY,
    person_name NVARCHAR(255) NOT NULL,
    created_at DATETIME DEFAULT GETDATE()
);
GO

-- =========================================
-- Table: Movies
-- =========================================
IF OBJECT_ID('Movies', 'U') IS NOT NULL DROP TABLE Movies;
GO
CREATE TABLE Movies (
    movie_id NVARCHAR(50) PRIMARY KEY,
    title NVARCHAR(255) NOT NULL,
    description NVARCHAR(MAX) NULL,
    release_date DATE NULL,
    country NVARCHAR(100) NULL,
    language NVARCHAR(50) NULL,
    director_id INT NULL FOREIGN KEY REFERENCES People(person_id),
    poster_url NVARCHAR(500) NULL,
    created_at DATETIME DEFAULT GETDATE()
);
GO

-- =========================================
-- Table: Genres
-- =========================================
IF OBJECT_ID('Genres', 'U') IS NOT NULL DROP TABLE Genres;
GO
CREATE TABLE Genres (
    genre_id INT IDENTITY(1,1) PRIMARY KEY,
    genre_name NVARCHAR(50) UNIQUE
);
GO

-- =========================================
-- Table: Movie_Genres (Quan hệ N:N)
-- =========================================
IF OBJECT_ID('Movie_Genres', 'U') IS NOT NULL DROP TABLE Movie_Genres;
GO
CREATE TABLE Movie_Genres (
    movie_id NVARCHAR(50) FOREIGN KEY REFERENCES Movies(movie_id),
    genre_id INT FOREIGN KEY REFERENCES Genres(genre_id),
    PRIMARY KEY (movie_id, genre_id)
);
GO

-- =========================================
-- Table: Movie_Cast (Diễn viên)
-- =========================================
IF OBJECT_ID('Movie_Cast', 'U') IS NOT NULL DROP TABLE Movie_Cast;
GO
CREATE TABLE Movie_Cast (
    movie_id NVARCHAR(50) FOREIGN KEY REFERENCES Movies(movie_id),
    person_id INT FOREIGN KEY REFERENCES People(person_id),
    role_type NVARCHAR(50) DEFAULT 'Actor',
    PRIMARY KEY (movie_id, person_id, role_type)
);
GO

-- =========================================
-- Table: Financials
-- =========================================
IF OBJECT_ID('Financials', 'U') IS NOT NULL DROP TABLE Financials;
GO
CREATE TABLE Financials (
    movie_id NVARCHAR(50) FOREIGN KEY REFERENCES Movies(movie_id) PRIMARY KEY,
    budget BIGINT NULL,
    revenue_domestic BIGINT NULL,
    revenue_international BIGINT NULL
);
GO

-- =========================================
-- Table: Ratings (IMDb, TMDB, Rotten Tomatoes, v.v.)
-- =========================================
IF OBJECT_ID('Ratings', 'U') IS NOT NULL DROP TABLE Ratings;
GO
CREATE TABLE Ratings (
    movie_id NVARCHAR(50) FOREIGN KEY REFERENCES Movies(movie_id),
    source_name NVARCHAR(50),
    score FLOAT NULL,
    vote_count INT NULL,
    last_updated DATETIME DEFAULT GETDATE(),
    PRIMARY KEY (movie_id, source_name)
);
GO

-- =========================================
-- Table: Streaming_Popularity
-- =========================================
IF OBJECT_ID('Streaming_Popularity', 'U') IS NOT NULL DROP TABLE Streaming_Popularity;
GO
CREATE TABLE Streaming_Popularity (
    movie_id NVARCHAR(50) FOREIGN KEY REFERENCES Movies(movie_id),
    platform_name NVARCHAR(50),
    rank INT,
    hours_viewed BIGINT,
    measurement_week DATE DEFAULT GETDATE(),
    PRIMARY KEY (movie_id, platform_name)
);
GO

PRINT '✅ Database XUHUONGPHIM và tất cả bảng đã được tạo thành công!';
- =========================================
-- 🔍 KIỂM TRA DỮ LIỆU SAU KHI CRAWL
-- =========================================

-- 1️⃣ Xem danh sách phim cơ bản
SELECT TOP 20 
    M.movie_id,
    M.title,
    M.release_date,
    M.country,
    M.language,
    P.person_name AS director
FROM Movies M
LEFT JOIN People P ON M.director_id = P.person_id
ORDER BY M.release_date DESC;
GO

-- 2️⃣ Xem phim kèm thể loại
SELECT 
    M.title,
    STRING_AGG(G.genre_name, ', ') AS genres
FROM Movies M
JOIN Movie_Genres MG ON M.movie_id = MG.movie_id
JOIN Genres G ON MG.genre_id = G.genre_id
GROUP BY M.title
ORDER BY M.title;
GO

-- 3️⃣ Xem phim kèm diễn viên chính
SELECT 
    M.title,
    STRING_AGG(P.person_name, ', ') AS main_cast
FROM Movies M
JOIN Movie_Cast C ON M.movie_id = C.movie_id
JOIN People P ON C.person_id = P.person_id
WHERE C.role_type = 'Actor'
GROUP BY M.title
ORDER BY M.title;
GO

-- 4️⃣ Xem phim kèm điểm IMDb và số lượt bình chọn
SELECT 
    M.title,
    R.source_name,
    R.score,
    R.vote_count
FROM Movies M
JOIN Ratings R ON M.movie_id = R.movie_id
ORDER BY R.source_name, R.score DESC;
GO

-- 5️⃣ Tổng hợp doanh thu và ngân sách
SELECT 
    M.title,
    F.budget,
    F.revenue_domestic,
    F.revenue_international,
    (ISNULL(F.revenue_domestic,0) + ISNULL(F.revenue_international,0) - ISNULL(F.budget,0)) AS profit
FROM Movies M
JOIN Financials F ON M.movie_id = F.movie_id
ORDER BY profit DESC;
GO

-- 6️⃣ Xem độ phổ biến theo nền tảng streaming
SELECT 
    M.title,
    S.platform_name,
    S.rank,
    S.hours_viewed
FROM Movies M
JOIN Streaming_Popularity S ON M.movie_id = S.movie_id
ORDER BY S.platform_name, S.rank;
GO

-- 7️⃣ Tích hợp tất cả thông tin chính (dễ dùng cho phân tích hoặc xuất qua Python)
SELECT 
    M.title,
    M.language,
    M.country,
    P.person_name AS director,
    STRING_AGG(DISTINCT G.genre_name, ', ') AS genres,
    MAX(R.score) AS imdb_rating,
    MAX(R.vote_count) AS vote_count,
    MAX(F.revenue_domestic + F.revenue_international) AS total_revenue,
    MAX(S.hours_viewed) AS hours_viewed
FROM Movies M
LEFT JOIN People P ON M.director_id = P.person_id
LEFT JOIN Movie_Genres MG ON M.movie_id = MG.movie_id
LEFT JOIN Genres G ON MG.genre_id = G.genre_id
LEFT JOIN Ratings R ON M.movie_id = R.movie_id AND R.source_name = 'IMDb'
LEFT JOIN Financials F ON M.movie_id = F.movie_id
LEFT JOIN Streaming_Popularity S ON M.movie_id = S.movie_id
GROUP BY M.title, M.language, M.country, P.person_name
ORDER BY imdb_rating DESC;
GO