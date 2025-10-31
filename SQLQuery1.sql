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
-- Table: People (diễn viên, đạo diễn)
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
    release_date DATE,
    country NVARCHAR(100),
    language NVARCHAR(50),
    studio NVARCHAR(100),
    director_id INT FOREIGN KEY REFERENCES People(person_id)
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
-- Table: Movie_Genres (N:N)
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
-- Table: Movie_Cast (Actors)
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
-- Table: Ratings
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

-- Hiển thị tất cả dữ liệu từ bảng Movies
SELECT * FROM Movies;

-- Hiển thị tất cả dữ liệu từ bảng People
SELECT * FROM People;

-- Hiển thị tất cả dữ liệu từ bảng Genres
SELECT * FROM Genres;

-- Hiển thị tất cả dữ liệu từ bảng Movie_Genres
SELECT * FROM Movie_Genres;

-- Hiển thị tất cả dữ liệu từ bảng Movie_Cast
SELECT * FROM Movie_Cast;

-- Hiển thị tất cả dữ liệu từ bảng Financials
SELECT * FROM Financials;

-- Hiển thị tất cả dữ liệu từ bảng Ratings
SELECT * FROM Ratings;

-- Hiển thị tất cả dữ liệu từ bảng Streaming_Popularity
SELECT * FROM Streaming_Popularity;

USE XUHUONGPHIM;
GO

SELECT TOP 10 
    m.title,
    sp.platform_name,
    sp.rank,
    sp.hours_viewed,
    sp.measurement_week
FROM Streaming_Popularity sp
JOIN Movies m ON sp.movie_id = m.movie_id
ORDER BY sp.hours_viewed DESC;

    SELECT 
        COUNT(*) AS SoLuongPhim_United_States
    FROM Movies
    WHERE country = N'United States';