import requests
import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from textblob import TextBlob
import os
import warnings
import random

warnings.filterwarnings("ignore")

# ==============================
# 🔧 1. KẾT NỐI DATABASE
# ==============================
conn = pyodbc.connect(
    'DRIVER={ODBC Driver 17 for SQL Server};'
    'SERVER=localhost;'
    'DATABASE=FILM TRENDING;'
    'Trusted_Connection=yes;'
)
cursor = conn.cursor()

# ==============================
# 🧱 2. TẠO BẢNG (NẾU CHƯA CÓ)
# ==============================
cursor.execute("""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='RatingsCompare')
CREATE TABLE RatingsCompare (
    Movie_id INT PRIMARY KEY,
    Title NVARCHAR(255),
    Genre NVARCHAR(100),
    Critics_score FLOAT,
    Audience_score FLOAT,
    Review_count INT,
    Release_year INT
);
""")

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='TopRatedMovies')
CREATE TABLE TopRatedMovies (
    Movie_id INT PRIMARY KEY,
    Title NVARCHAR(255),
    Genre NVARCHAR(100),
    Imdb_rating FLOAT,
    Rt_rating FLOAT,
    Metacritic_rating FLOAT,
    Avg_score FLOAT,
    Vote_count INT,
    Release_year INT
);
""")

cursor.execute("""
IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='SentimentReviews')
CREATE TABLE SentimentReviews (
    Review_id INT IDENTITY(1,1) PRIMARY KEY,
    Movie_id INT,
    Title NVARCHAR(255),
    Genre NVARCHAR(100),
    Sentiment_label NVARCHAR(20),
    Sentiment_score FLOAT,
    Language NVARCHAR(20)
);
""")

conn.commit()

# ==============================
# 🎬 3. LẤY DỮ LIỆU TỪ OMDb + TMDb API
# ==============================
TMDB_KEY = os.getenv("TMDB_API_KEY", "4f013f2a8509b8f4b1ef3205f0ca9f00")
OMDB_KEY = os.getenv("OMDB_API_KEY", "a07802fd")

# 🎯 Lấy 100 phim trending (5 trang x 20 phim)
movies = []
for page in range(1, 6):
    tmdb_trending_url = f"https://api.themoviedb.org/3/trending/movie/week?api_key={TMDB_KEY}&language=en-US&page={page}"
    try:
        trending_data = requests.get(tmdb_trending_url).json()
        movies_page = [m["title"] for m in trending_data.get("results", []) if "title" in m]
        movies.extend(movies_page)
        print(f"✅ Lấy được {len(movies_page)} phim từ trang {page}")
    except Exception as e:
        print(f"⚠️ Không thể lấy trang {page}:", e)

print(f"🔥 Tổng cộng đã lấy được {len(movies)} phim trending từ TMDb!")

# ==============================
# 🧹 XÓA DỮ LIỆU CŨ
# ==============================
cursor.execute("DELETE FROM RatingsCompare")
cursor.execute("DELETE FROM TopRatedMovies")
cursor.execute("DELETE FROM SentimentReviews")
conn.commit()

# ==============================
# 🧩 4. LẤY DỮ LIỆU CHI TIẾT
# ==============================
for idx, title in enumerate(movies, start=1):
    print(f"📡 Đang xử lý ({idx}/{len(movies)}): {title}...")

    try:
        # --- TMDb details ---
        tmdb_search = f"https://api.themoviedb.org/3/search/movie?query={title}&api_key={TMDB_KEY}&language=en-US"
        tmdb_data = requests.get(tmdb_search).json()
        if not tmdb_data.get("results"):
            continue

        movie_id = tmdb_data["results"][0]["id"]
        tmdb_details = requests.get(
            f"https://api.themoviedb.org/3/movie/{movie_id}?api_key={TMDB_KEY}&language=en-US"
        ).json()

        genre = tmdb_details["genres"][0]["name"] if tmdb_details.get("genres") else "Unknown"
        year = tmdb_details.get("release_date", "")[:4]
        tmdb_rating = tmdb_details.get("vote_average", 0) * 10
        vote_count = tmdb_details.get("vote_count", 0)

        # --- OMDb Ratings ---
        omdb_url = f"https://www.omdbapi.com/?t={title}&apikey={OMDB_KEY}"
        omdb_data = requests.get(omdb_url).json()

        imdb_rating = rt_rating = meta_score = None
        if omdb_data.get("Response") == "True":
            if omdb_data.get("imdbRating") != "N/A":
                imdb_rating = float(omdb_data.get("imdbRating")) * 10
            if omdb_data.get("Metascore") != "N/A":
                meta_score = float(omdb_data.get("Metascore"))
            for r in omdb_data.get("Ratings", []):
                if r["Source"] == "Rotten Tomatoes":
                    rt_rating = float(r["Value"].replace("%", ""))
                    break

        scores = [x for x in [tmdb_rating, imdb_rating, rt_rating, meta_score] if x is not None]
        avg_score = round(sum(scores) / len(scores), 2) if scores else None

        # --- Ghi dữ liệu ---
        cursor.execute("""
            INSERT INTO RatingsCompare VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (idx, title, genre, rt_rating, imdb_rating, vote_count, year))

        cursor.execute("""
            INSERT INTO TopRatedMovies VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (idx, title, genre, imdb_rating, rt_rating, meta_score, avg_score, vote_count, year))

        # --- GIẢ LẬP REVIEW CẢM XÚC VỚI RANDOM ---
        review_templates = [
            # Positive reviews
            "Amazing cinematography and story! Absolutely loved it.",
            "Masterpiece, truly emotional and powerful.",
            "Outstanding performance by the entire cast.",
            "One of the best films I've seen this year.",
            "Brilliant direction and captivating plot.",
            "Visual masterpiece with stunning effects.",
            "Emotionally resonant and beautifully crafted.",
            
            # Neutral reviews  
            "Mediocre plot but great acting overall.",
            "Fun to watch but a bit too long in some parts.",
            "Enjoyable but nothing particularly special.",
            "Solid film with both strengths and weaknesses.",
            "The pacing felt uneven in several parts.",
            "Competently made but lacking originality.",
            "Worth watching once but probably not again.",
            
            # Negative reviews
            "Overrated, didn't enjoy it much at all.",
            "Disappointing compared to all the hype.",
            "The plot was confusing and hard to follow.",
            "Poor character development and weak storyline.",
            "Not my personal taste and poorly executed.",
            "A waste of time with uninteresting characters.",
            "Boring and predictable throughout most scenes."
        ]
        
        # Random số lượng review từ 8-15 cho mỗi phim
        num_reviews = random.randint(8, 15)
        
        for _ in range(num_reviews):
            # Chọn random một review template
            text = random.choice(review_templates)
            
            # Thêm một số biến thể ngẫu nhiên để đa dạng hóa
            random_variations = [
                " The soundtrack was also fantastic.",
                " However, the ending felt rushed.",
                " The cinematography was particularly impressive.",
                " Some scenes could have been edited better.",
                " The character development was exceptional.",
                " I would definitely recommend this film.",
                " Not what I expected but still enjoyable.",
                " The visual effects were groundbreaking.",
                " The dialogue felt unnatural at times.",
                " A truly unforgettable experience.",
                ""
            ]
            
            # 50% chance thêm variation
            if random.random() > 0.5:
                text += random.choice(random_variations)
            
            # Tính sentiment score
            score = TextBlob(text).sentiment.polarity
            
            # Điều chỉnh score ngẫu nhiên một chút để đa dạng
            score_variation = random.uniform(-0.1, 0.1)
            final_score = max(-1.0, min(1.0, score + score_variation))  # Giới hạn trong khoảng -1 đến 1
            
            # Phân loại sentiment
            if final_score > 0.1:
                label = "Positive"
            elif final_score < -0.1:
                label = "Negative"
            else:
                label = "Neutral"
            
            # Insert vào database
            cursor.execute("""
                INSERT INTO SentimentReviews (Movie_id, Title, Genre, Sentiment_label, Sentiment_score, Language)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (idx, title, genre, label, round(final_score, 3), "en"))

        print(f"✅ Đã thêm {num_reviews} review cho phim {title}")

    except Exception as e:
        print("⚠️ Lỗi khi xử lý phim:", title, "|", e)
# ==============================
# 🧹 5. XỬ LÝ DỮ LIỆU NULL TRONG PYTHON
# ==============================

# Đọc dữ liệu từ SQL Server vào pandas DataFrame
df = pd.read_sql("SELECT * FROM TopRatedMovies", conn)

# Kiểm tra số lượng giá trị null
print("🔍 Kiểm tra dữ liệu null trước khi xử lý:")
print(df.isnull().sum())

# Thay thế giá trị null trong các cột số bằng trung bình
num_cols = ['Imdb_rating', 'Rt_rating', 'Metacritic_rating', 'Avg_score', 'Vote_count']
for col in num_cols:
    if df[col].isnull().sum() > 0:
        mean_value = df[col].mean()
        df[col].fillna(mean_value, inplace=True)
        print(f"✅ Đã thay giá trị null trong cột {col} bằng trung bình: {round(mean_value,2)}")

# Nếu có cột chữ bị thiếu thể loại
if df['Genre'].isnull().sum() > 0:
    mode_genre = df['Genre'].mode()[0]
    df['Genre'].fillna(mode_genre, inplace=True)
    print(f"✅ Đã thay giá trị null trong cột Genre bằng thể loại phổ biến nhất: {mode_genre}")


# Cập nhật lại bảng trong SQL Server
cursor.execute("DELETE FROM TopRatedMovies")
conn.commit()

for _, row in df.iterrows():
    cursor.execute("""
        INSERT INTO TopRatedMovies (Movie_id, Title, Genre, Imdb_rating, Rt_rating, Metacritic_rating, Avg_score, Vote_count, Release_year)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, tuple(row))


conn.commit()
print("✅ Dữ liệu từ TMDb + OMDb đã được lưu vào SQL Server!")
