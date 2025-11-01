import requests
from textblob import TextBlob
import pyodbc
import time
import random
from datetime import datetime
from bs4 import BeautifulSoup
import matplotlib.pyplot as plt
import pandas as pd

# ======================
# 🔑 API KEYS
# ======================
TMDB_KEY = "4f013f2a8509b8f4b1ef3205f0ca9f00"
OMDB_KEY = "a07802fd"

# ======================
# 🧩 1️⃣ LẤY DANH SÁCH PHIM NHIỀU HƠN TỪ TMDb
# ======================
def get_top_movies_2025(pages=5):
    movies = []
    for page in range(1, pages + 1):
        url = "https://api.themoviedb.org/3/discover/movie"
        params = {
            "api_key": TMDB_KEY,
            "primary_release_year": 2025,
            "sort_by": "popularity.desc",
            "language": "en-US",
            "page": page
        }
        try:
            res = requests.get(url, params=params, timeout=10)
            res.raise_for_status()
            data = res.json()
            movies.extend(data.get("results", []))
        except Exception as e:
            print(f"⚠️ Lỗi khi lấy trang {page}: {e}")
    return movies

# ======================
# 🧩 2️⃣ LẤY DỮ LIỆU TỪ NETFLIX (BeautifulSoup)
# ======================
def get_netflix_top10():
    try:
        url = "https://www.netflix.com/tudum/top10"
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        titles = [t.get_text(strip=True) for t in soup.find_all("h3", class_="title")]
        return titles[:10]
    except:
        # fallback
        return [
            "Stranger Things", "The Crown", "Wednesday", "Bridgerton", 
            "Money Heist", "The Witcher", "Squid Game", "Lucifer", 
            "Dark", "The Queen's Gambit"
        ]

# ======================
# 🧩 3️⃣ LẤY DỮ LIỆU TỪ OMDb (rút gọn)
# ======================
def get_ratings_from_omdb(title=None, imdb_id=None):
    try:
        if imdb_id:
            query = f"?i={imdb_id}"
        elif title:
            query = f"?t={title.replace(' ', '+')}"
        else:
            return None

        url = f"https://www.omdbapi.com/{query}&apikey={OMDB_KEY}"
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        data = res.json()

        if data.get("Response") != "True":
            return None

        return {
            "imdb_id": data.get("imdbID"),
            "title": data.get("Title"),
            "year": data.get("Year"),
            "genre": data.get("Genre"),
            "country": data.get("Country"),
            "language": data.get("Language"),
            "director": data.get("Director"),
            "actors": data.get("Actors"),
            "plot": data.get("Plot", ""),
            "imdb_rating": float(data["imdbRating"]) if data.get("imdbRating") != "N/A" else None,
            "imdb_votes": int(data["imdbVotes"].replace(",", "")) if data.get("imdbVotes") != "N/A" else None,
            "box_office": data.get("BoxOffice")
        }
    except Exception as e:
        print(f"⚠️ Lỗi OMDb cho '{title}': {e}")
        return None

# ======================
# 🧩 4️⃣ PHÂN TÍCH SENTIMENT
# ======================
def analyze_sentiment(text):
    if not text:
        return "Neutral"
    score = TextBlob(text).sentiment.polarity
    if score > 0.1:
        return "Positive"
    elif score < -0.1:
        return "Negative"
    else:
        return "Neutral"

# ======================
# 🧩 5️⃣ LƯU DATABASE (đơn giản hóa)
# ======================
def get_db_connection():
    return pyodbc.connect(
        "DRIVER={SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=XUHUONGPHIM;"
        "Trusted_Connection=yes;"
    )

def save_to_database(movie_data_list):
    conn = get_db_connection()
    cursor = conn.cursor()

    try:
        for movie in movie_data_list:
            movie_id = movie.get('imdb_id') or f"custom_{abs(hash(movie['title']))}"
            year = movie.get('year')

            try:
                release_date = datetime.strptime(year, '%Y') if year and year.isdigit() else datetime(2025,1,1)
            except:
                release_date = datetime(2025,1,1)

            cursor.execute("SELECT 1 FROM Movies WHERE movie_id = ?", movie_id)
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO Movies (movie_id, title, release_date, country, language, director_id)
                    VALUES (?, ?, ?, ?, ?, NULL)
                """, (movie_id, movie['title'], release_date, movie.get('country',''), movie.get('language','')))

            if movie.get('imdb_rating') is not None:
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM Ratings WHERE movie_id=? AND source_name='IMDb')
                    INSERT INTO Ratings (movie_id, source_name, score, vote_count, last_updated)
                    VALUES (?, 'IMDb', ?, ?, GETDATE())
                """, movie_id, movie_id, movie['imdb_rating'], movie.get('imdb_votes',0))

        conn.commit()
        print(f"✅ Đã lưu {len(movie_data_list)} phim vào database.")
    except Exception as e:
        conn.rollback()
        print(f"❌ Lỗi khi lưu: {e}")
    finally:
        conn.close()

# ======================
# 🧩 6️⃣ MAIN CRAWLER
# ======================
if __name__ == "__main__":
    print("🚀 Đang thu thập dữ liệu phim...")
    tmdb_movies = get_top_movies_2025(pages=5)
    netflix_movies = get_netflix_top10()

    titles = [m["title"] for m in tmdb_movies] + netflix_movies
    movie_data_list = []

    for title in titles:
        print(f"🎬 Đang xử lý: {title}")
        movie_data = get_ratings_from_omdb(title=title)
        if movie_data:
            movie_data["sentiment"] = analyze_sentiment(movie_data.get("plot", ""))
            movie_data_list.append(movie_data)
            print(f"   ✅ {movie_data['title']} | IMDb: {movie_data.get('imdb_rating','N/A')} | Sentiment: {movie_data['sentiment']}")
        time.sleep(random.uniform(1.2, 2.5))

    if movie_data_list:
        save_to_database(movie_data_list)
        print("🎯 Hoàn tất lưu dữ liệu!")

# ======================
# 🧩 7️⃣ TRỰC QUAN HÓA
# ======================
    df = pd.DataFrame(movie_data_list)
    if 'sentiment' in df.columns and not df.empty:
        sentiment_counts = df['sentiment'].value_counts()
        plt.figure(figsize=(6,6))
        plt.pie(sentiment_counts, labels=sentiment_counts.index, autopct='%1.1f%%', startangle=90)
        plt.title("Tỷ lệ cảm xúc (Sentiment Analysis)", fontsize=14, fontweight='bold')
        plt.show()

        # Biểu đồ thể loại
        genre_sentiments = []
        for _, row in df.iterrows():
            if pd.notna(row.get('genre')) and pd.notna(row.get('sentiment')):
                for g in [x.strip() for x in row['genre'].split(',')]:
                    genre_sentiments.append({"Genre": g, "Sentiment": row['sentiment']})

        genre_df = pd.DataFrame(genre_sentiments)
        if not genre_df.empty:
            summary = genre_df.groupby(['Genre', 'Sentiment']).size().unstack(fill_value=0)
            top_genres = genre_df['Genre'].value_counts().head(8).index
            summary = summary.loc[top_genres]
            summary.plot(kind='bar', figsize=(10,6))
            plt.title("Cảm xúc theo thể loại phim", fontsize=14, fontweight='bold')
            plt.xlabel("Thể loại phim")
            plt.ylabel("Số lượng phim")
            plt.xticks(rotation=45)
            plt.tight_layout()
            plt.show()
