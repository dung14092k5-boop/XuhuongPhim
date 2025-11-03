CREATE DATABASE XUHUONGPHIM;
GO
USE XUHUONGPHIM;
GO

-- 1️⃣ Bảng lưu người (đạo diễn, diễn viên)
CREATE TABLE People (
    person_id INT IDENTITY(1,1) PRIMARY KEY,
    person_name NVARCHAR(255) NOT NULL
);

-- 2️⃣ Bảng phim chính
CREATE TABLE Movies (
    movie_id NVARCHAR(50) PRIMARY KEY,
    title NVARCHAR(255),
    release_date DATE,
    country NVARCHAR(100),
    language NVARCHAR(100),
    studio NVARCHAR(255),
    director_id INT NULL,
    FOREIGN KEY (director_id) REFERENCES People(person_id)
);

-- 3️⃣ Bảng thể loại
CREATE TABLE Genres (
    genre_id INT IDENTITY(1,1) PRIMARY KEY,
    genre_name NVARCHAR(100)
);

-- 4️⃣ Liên kết phim - thể loại
CREATE TABLE Movie_Genres (
    movie_id NVARCHAR(50),
    genre_id INT,
    PRIMARY KEY (movie_id, genre_id),
    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
    FOREIGN KEY (genre_id) REFERENCES Genres(genre_id)
);

-- 5️⃣ Liên kết phim - diễn viên/đạo diễn
CREATE TABLE Movie_Cast (
    movie_id NVARCHAR(50),
    person_id INT,
    role_type NVARCHAR(50),
    PRIMARY KEY (movie_id, person_id, role_type),
    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id),
    FOREIGN KEY (person_id) REFERENCES People(person_id)
);

-- 6️⃣ Tài chính (doanh thu, ngân sách)
CREATE TABLE Financials (
    financial_id INT IDENTITY(1,1) PRIMARY KEY,
    movie_id NVARCHAR(50),
    budget BIGINT NULL,
    revenue_domestic BIGINT NULL,
    revenue_international BIGINT NULL,
    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id)
);

-- 7️⃣ Ratings từ IMDb, Rotten Tomatoes, Metacritic
CREATE TABLE Ratings (
    rating_id INT IDENTITY(1,1) PRIMARY KEY,
    movie_id NVARCHAR(50),
    source_name NVARCHAR(100),
    score FLOAT,
    vote_count INT NULL,
    last_updated DATETIME DEFAULT GETDATE(),
    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id)
);

-- 8️⃣ Dữ liệu xem trên nền tảng streaming
CREATE TABLE Streaming_Popularity (
    stream_id INT IDENTITY(1,1) PRIMARY KEY,
    movie_id NVARCHAR(50),
    platform_name NVARCHAR(100),
    rank INT,
    hours_viewed BIGINT,
    measurement_week DATETIME,
    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id)
);

-- 9️⃣ Bảng lưu sentiment/phân tích cảm xúc
CREATE TABLE Movie_Sentiment (
    sentiment_id INT IDENTITY(1,1) PRIMARY KEY,
    movie_id NVARCHAR(50),
    sentiment NVARCHAR(20),
    FOREIGN KEY (movie_id) REFERENCES Movies(movie_id)
);
