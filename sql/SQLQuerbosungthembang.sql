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
-- BỔ SUNG SAU KHỐI CREATE TABLE Movies
ALTER TABLE Movies ADD tmdb_id INT NULL, imdb_id NVARCHAR(12) NULL;

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
-- BỔ SUNG SAU KHỐI CREATE TABLE Ratings
ALTER TABLE Ratings ALTER COLUMN score DECIMAL(5,2) NULL;
IF NOT EXISTS (SELECT 1 FROM sys.check_constraints WHERE name='CK_Ratings_0_100')
  ALTER TABLE Ratings ADD CONSTRAINT CK_Ratings_0_100 CHECK (score BETWEEN 0 AND 100);
  -----------------------------------------------------------------------------------------
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
-- BỔ SUNG SAU KHỐI CREATE TABLE Streaming_Popularity
IF COL_LENGTH('Streaming_Popularity','week_start') IS NULL
  ALTER TABLE Streaming_Popularity ADD week_start DATE NULL;

-- Đổi PK (tên PK hiện tại không cố định, nên tra rồi drop)
DECLARE @pk NVARCHAR(128) =
  (SELECT name FROM sys.key_constraints
   WHERE [type]='PK' AND [parent_object_id]=OBJECT_ID('Streaming_Popularity'));
IF @pk IS NOT NULL EXEC('ALTER TABLE Streaming_Popularity DROP CONSTRAINT '+@pk);

ALTER TABLE Streaming_Popularity
  ADD CONSTRAINT PK_Streaming PRIMARY KEY (movie_id, platform_name, week_start);

-- ========================================================================================================================================
-- Bo sung Bang moi
-- ========================================================================================================================================
-- =========================================
-- Table:  Reviews for Sentiment
-- =========================================
IF OBJECT_ID('Reviews','U') IS NOT NULL DROP TABLE Reviews;
CREATE TABLE Reviews(
  review_id   INT IDENTITY(1,1) PRIMARY KEY,
  movie_id    NVARCHAR(50) NOT NULL FOREIGN KEY REFERENCES Movies(movie_id),
  review_text NVARCHAR(MAX) NOT NULL,
  lang        NVARCHAR(10) NULL,
  created_at  DATETIME DEFAULT GETDATE(),
  sent_label  NVARCHAR(8) NULL  -- 'Positive'/'Neutral'/'Negative'
);
GO
-----Movie_Crew và Franchise (yếu tố sáng tạo)----
IF OBJECT_ID('Movie_Crew','U') IS NOT NULL DROP TABLE Movie_Crew;
CREATE TABLE Movie_Crew(
  movie_id  NVARCHAR(50) FOREIGN KEY REFERENCES Movies(movie_id),
  person_id INT          FOREIGN KEY REFERENCES People(person_id),
  role_type NVARCHAR(50) NOT NULL, -- 'Director'/'Writer'/'Producer'
  PRIMARY KEY(movie_id, person_id, role_type)
);

IF OBJECT_ID('Franchises','U') IS NOT NULL DROP TABLE Franchises;
CREATE TABLE Franchises(
  franchise_id INT IDENTITY(1,1) PRIMARY KEY,
  franchise_name NVARCHAR(150) UNIQUE
);

IF OBJECT_ID('Movie_Franchise','U') IS NOT NULL DROP TABLE Movie_Franchise;
CREATE TABLE Movie_Franchise(
  movie_id NVARCHAR(50) FOREIGN KEY REFERENCES Movies(movie_id),
  franchise_id INT FOREIGN KEY REFERENCES Franchises(franchise_id),
  PRIMARY KEY(movie_id, franchise_id)
);

----Social: Trailers và Public_Interest
IF OBJECT_ID('Trailers','U') IS NOT NULL DROP TABLE Trailers;
CREATE TABLE Trailers(
  movie_id     NVARCHAR(50) FOREIGN KEY REFERENCES Movies(movie_id),
  platform     NVARCHAR(20) NOT NULL, -- 'YouTube'
  video_id     NVARCHAR(32) NOT NULL,
  view_count   BIGINT NULL,
  like_count   BIGINT NULL,
  snapshot_date DATE NOT NULL,
  PRIMARY KEY(movie_id, platform, snapshot_date)
);

IF OBJECT_ID('Public_Interest','U') IS NOT NULL DROP TABLE Public_Interest;
CREATE TABLE Public_Interest(
  keyword NVARCHAR(100) NOT NULL,
  region  NVARCHAR(10)  NOT NULL, -- 'US','VN',...
  [date]  DATE          NOT NULL,
  trend_index INT NULL,           -- 0-100
  PRIMARY KEY(keyword, region, [date])
);
-------- Market_Context (ngoại biên thị trường)
IF OBJECT_ID('Market_Context','U') IS NOT NULL DROP TABLE Market_Context;
CREATE TABLE Market_Context(
  region NVARCHAR(10) NOT NULL,
  [year] INT NOT NULL,
  gdp_per_capita NUMERIC(18,2) NULL,
  ott_penetration NUMERIC(5,2) NULL,
  cinema_open_rate NUMERIC(5,2) NULL,
  PRIMARY KEY(region, [year])
);
-----Views Cho tg Huy vs Ngoc 
-- Pivot điểm theo nguồn
CREATE OR ALTER VIEW vw_RatingPivot AS
SELECT m.movie_id, m.title,
  MAX(CASE WHEN r.source_name='IMDb'            THEN r.score END) AS imdb_norm,
  MAX(CASE WHEN r.source_name='TMDb'            THEN r.score END) AS tmdb_norm,
  MAX(CASE WHEN r.source_name='Rotten Tomatoes' THEN r.score END) AS rt,
  MAX(CASE WHEN r.source_name='Metacritic'      THEN r.score END) AS meta,
  MAX(CASE WHEN r.source_name='IMDb'            THEN r.vote_count END) AS imdb_votes,
  MAX(CASE WHEN r.source_name='TMDb'            THEN r.vote_count END) AS tmdb_votes
FROM Movies m
LEFT JOIN Ratings r ON r.movie_id=m.movie_id
GROUP BY m.movie_id, m.title;

-- Core + genres
CREATE OR ALTER VIEW vw_MovieCore AS
SELECT m.movie_id, m.title, m.release_date,
       STRING_AGG(g.genre_name, ', ') AS genres
FROM Movies m
LEFT JOIN Movie_Genres mg ON mg.movie_id=m.movie_id
LEFT JOIN Genres g ON g.genre_id=mg.genre_id
GROUP BY m.movie_id, m.title, m.release_date;

-- Scatter dataset (Huy)
CREATE OR ALTER VIEW vw_Scatter AS
SELECT p.movie_id, p.title,
       COALESCE(p.tmdb_norm, p.rt) AS audience_score,
       COALESCE(p.meta, p.rt)      AS critics_score,
       COALESCE(p.imdb_votes, p.tmdb_votes, 0) AS n_reviews
FROM vw_RatingPivot p;

-- Điểm tổng hợp (Ngọc)
CREATE OR ALTER VIEW vw_MovieScore AS
SELECT p.movie_id, p.title,
       CAST(0.40*ISNULL(p.imdb_norm,0)
          + 0.40*ISNULL(p.meta,0)
          + 0.20*ISNULL(p.rt,0) AS DECIMAL(5,2)) AS score_agg
FROM vw_RatingPivot p;
-----------------------------------------------------------------------------
CREATE INDEX IX_Movies_TitleDate      ON Movies(title, release_date);
CREATE INDEX IX_MovieGenres_Genre     ON Movie_Genres(genre_id);
CREATE INDEX IX_MovieCast_Person      ON Movie_Cast(person_id);
CREATE INDEX IX_Ratings_MovieSource   ON Ratings(movie_id, source_name);
CREATE INDEX IX_Stream_Week           ON Streaming_Popularity(movie_id, week_start);
CREATE INDEX IX_Reviews_Movie         ON Reviews(movie_id);

-- ========================================================================================================================================
-- Ket THuc Bo sung Bang moi
-- ========================================================================================================================================
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