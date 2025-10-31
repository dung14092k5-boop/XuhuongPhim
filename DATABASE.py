import requests
from textblob import TextBlob
import pyodbc
import time
import random
from datetime import datetime
from bs4 import BeautifulSoup
# ======================
# 🔑 API KEYS
# ======================
TMDB_KEY = "4f013f2a8509b8f4b1ef3205f0ca9f00"
OMDB_KEY = "a07802fd"

# ======================
# 🧩 1️⃣ LẤY DANH SÁCH PHIM NĂM 2025 (TMDb)
# ======================
def get_top_movies_2025():
    url = "https://api.themoviedb.org/3/discover/movie"
    params = {
        "api_key": TMDB_KEY,
        "primary_release_year": 2025,
        "sort_by": "popularity.desc",
        "language": "en-US",
        "page": 1
    }
    try:
        res = requests.get(url, params=params, timeout=10)
        res.raise_for_status()
        data = res.json()
        return data.get("results", [])[:10]
    except Exception as e:
        print(f"⚠️ Lỗi khi lấy danh sách phim TMDb: {e}")
        return []

# ======================
# 🧩 2️⃣ LẤY DỮ LIỆU TỪ NETFLIX TOP10 (BeautifulSoup)
# ======================
def get_netflix_top10():
    try:
        url = "https://www.netflix.com/tudum/top10"  # Thay bằng URL thật
        res = requests.get(url, timeout=10)
        res.raise_for_status()
        soup = BeautifulSoup(res.text, "html.parser")
        
        # Giả sử các tiêu đề nằm trong thẻ <h3 class="title">
        titles = [t.get_text(strip=True) for t in soup.find_all("h3", class_="title")]
        return titles[:10]  # Lấy top 10
    except Exception as e:
        print(f"⚠️ Lỗi khi lấy dữ liệu Netflix: {e}")
        # fallback nếu không crawl được
        return [
            "Stranger Things", "The Crown", "Wednesday", "Bridgerton", 
            "Money Heist", "The Witcher", "Squid Game", "Lucifer", 
            "Dark", "The Queen's Gambit"
        ]

# ======================
# 🧩 3️⃣ LẤY RATING TỔNG HỢP (OMDb)
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
            print(f"⚠️ Không tìm thấy '{title}' trong OMDb.")
            return None

        ratings = {r["Source"]: r["Value"] for r in data.get("Ratings", [])}

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
            "rotten_tomatoes": ratings.get("Rotten Tomatoes"),
            "metacritic": ratings.get("Metacritic"),
            "box_office": data.get("BoxOffice"),
            "runtime": data.get("Runtime")
        }
    except Exception as e:
        print(f"⚠️ Lỗi khi lấy rating OMDb cho '{title}': {e}")
        return None

# ======================
# 🧩 4️⃣ PHÂN TÍCH SENTIMENT
# ======================
def analyze_sentiment(text):
    if not text:
        return "Neutral"
    try:
        score = TextBlob(text).sentiment.polarity
        if score > 0.1:
            return "Positive"
        elif score < -0.1:
            return "Negative"
        else:
            return "Neutral"
    except:
        return "Neutral"

# ======================
# 🧩 5️⃣ KẾT NỐI DATABASE
# ======================
def get_db_connection():
    return pyodbc.connect(
        "DRIVER={SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=XUHUONGPHIM;"
        "Trusted_Connection=yes;"
    )

# ======================
# 🧩 6️⃣ HÀM LƯU DỮ LIỆU AN TOÀN
# ======================
def save_to_database(movie_data_list):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        for movie_data in movie_data_list:
            # --- Director ---
            director_id = None
            if movie_data.get('director'):
                main_director = movie_data['director'].split(',')[0].strip()
                cursor.execute("SELECT person_id FROM People WHERE person_name = ?", main_director)
                res = cursor.fetchone()
                if res:
                    director_id = res[0]
                else:
                    cursor.execute(
                        "INSERT INTO People (person_name) OUTPUT INSERTED.person_id VALUES (?)",
                        main_director
                    )
                    director_id = cursor.fetchone()[0]

            # --- Movie ---
            movie_id = movie_data.get('imdb_id') or f"custom_{abs(hash(movie_data['title']))}"
            year = movie_data.get('year')
            try:
                release_date = datetime.strptime(year, '%Y') if year and year.isdigit() else datetime(2025,1,1)
            except:
                release_date = datetime(2025,1,1)

            cursor.execute("SELECT 1 FROM Movies WHERE movie_id = ?", movie_id)
            if not cursor.fetchone():
                cursor.execute("""
                    INSERT INTO Movies (movie_id, title, release_date, country, language, studio, director_id)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    movie_id,
                    movie_data['title'],
                    release_date,
                    movie_data.get('country',''),
                    movie_data.get('language',''),
                    '',
                    director_id
                ))

            # --- Genres ---
            if movie_data.get('genre'):
                for genre_name in [g.strip() for g in movie_data['genre'].split(',')]:
                    cursor.execute("SELECT genre_id FROM Genres WHERE genre_name = ?", genre_name)
                    res = cursor.fetchone()
                    if res:
                        genre_id = res[0]
                    else:
                        cursor.execute("INSERT INTO Genres (genre_name) OUTPUT INSERTED.genre_id VALUES (?)", genre_name)
                        genre_id = cursor.fetchone()[0]
                    cursor.execute("SELECT 1 FROM Movie_Genres WHERE movie_id=? AND genre_id=?", movie_id, genre_id)
                    if not cursor.fetchone():
                        cursor.execute("INSERT INTO Movie_Genres (movie_id, genre_id) VALUES (?, ?)", movie_id, genre_id)

            # --- Actors ---
            if movie_data.get('actors'):
                for actor_name in [a.strip() for a in movie_data['actors'].split(',')][:5]:
                    cursor.execute("SELECT person_id FROM People WHERE person_name = ?", actor_name)
                    res = cursor.fetchone()
                    if res:
                        person_id = res[0]
                    else:
                        cursor.execute("INSERT INTO People (person_name) OUTPUT INSERTED.person_id VALUES (?)", actor_name)
                        person_id = cursor.fetchone()[0]
                    cursor.execute("SELECT 1 FROM Movie_Cast WHERE movie_id=? AND person_id=? AND role_type='Actor'", movie_id, person_id)
                    if not cursor.fetchone():
                        cursor.execute("INSERT INTO Movie_Cast (movie_id, person_id, role_type) VALUES (?, ?, 'Actor')", movie_id, person_id)

            # --- Financials ---
            box_office = movie_data.get('box_office')
            if box_office and box_office != 'N/A':
                try:
                    revenue = int(box_office.replace('$','').replace(',',''))
                    cursor.execute("SELECT 1 FROM Financials WHERE movie_id=?", movie_id)
                    if not cursor.fetchone():
                        cursor.execute("INSERT INTO Financials (movie_id, budget, revenue_domestic, revenue_international) VALUES (?, NULL, ?, ?)", movie_id, revenue, 0)
                except:
                    pass

            # --- Ratings ---
            if movie_data.get('imdb_rating') is not None:
                cursor.execute("SELECT 1 FROM Ratings WHERE movie_id=? AND source_name='IMDb'", movie_id)
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO Ratings (movie_id, source_name, score, vote_count, last_updated) VALUES (?, 'IMDb', ?, ?, GETDATE())", movie_id, movie_data['imdb_rating'], movie_data.get('imdb_votes',0))

            if movie_data.get('rotten_tomatoes'):
                try:
                    rt_score = float(movie_data['rotten_tomatoes'].replace('%',''))
                    cursor.execute("SELECT 1 FROM Ratings WHERE movie_id=? AND source_name='Rotten Tomatoes'", movie_id)
                    if not cursor.fetchone():
                        cursor.execute("INSERT INTO Ratings (movie_id, source_name, score, vote_count, last_updated) VALUES (?, 'Rotten Tomatoes', ?, NULL, GETDATE())", movie_id, rt_score)
                except:
                    pass

            if movie_data.get('metacritic'):
                try:
                    meta_score = float(movie_data['metacritic'].split('/')[0])
                    cursor.execute("SELECT 1 FROM Ratings WHERE movie_id=? AND source_name='Metacritic'", movie_id)
                    if not cursor.fetchone():
                        cursor.execute("INSERT INTO Ratings (movie_id, source_name, score, vote_count, last_updated) VALUES (?, 'Metacritic', ?, NULL, GETDATE())", movie_id, meta_score)
                except:
                    pass

            # --- Streaming Popularity (mock) ---
            cursor.execute("SELECT 1 FROM Streaming_Popularity WHERE movie_id=? AND platform_name='Netflix'", movie_id)
            if not cursor.fetchone():
                cursor.execute("INSERT INTO Streaming_Popularity (movie_id, platform_name, rank, hours_viewed, measurement_week) VALUES (?, 'Netflix', ?, ?, GETDATE())", movie_id, random.randint(1,10), random.randint(100000,5000000))

        conn.commit()
        print(f"✅ Đã lưu {len(movie_data_list)} phim vào database an toàn.")
    
    except Exception as e:
        conn.rollback()
        print(f"❌ Lỗi khi lưu vào database: {e}")
    finally:
        conn.close()

# ======================
# 🧩 7️⃣ MAIN
# ======================
if __name__ == "__main__":
    print("🚀 Bắt đầu thu thập dữ liệu phim...")

    tmdb_movies = get_top_movies_2025()
    netflix_movies = get_netflix_top10()

    titles = [m["title"] for m in tmdb_movies] + netflix_movies
    movie_data_list = []

    for title in titles:
        print(f"🎬 Đang xử lý: {title}")
        movie_data = get_ratings_from_omdb(title=title)
        if movie_data:
            movie_data['sentiment'] = analyze_sentiment(movie_data.get('plot',''))
            movie_data_list.append(movie_data)
            # ---- In ra console ----
            print(f"   ✅ Title: {movie_data['title']}")
            print(f"   IMDb ID: {movie_data['imdb_id']}")
            print(f"   Year: {movie_data['year']}")
            print(f"   Genre: {movie_data['genre']}")
            print(f"   Rating IMDb: {movie_data.get('imdb_rating','N/A')}")
            print(f"   Sentiment (Plot): {movie_data['sentiment']}")
        else:
            print(f"   ❌ Không tìm thấy dữ liệu cho: {title}")
        time.sleep(random.uniform(1.2, 2.5))

    if movie_data_list:
        save_to_database(movie_data_list)
        print("🎯 Hoàn tất thu thập và lưu dữ liệu!")
        # Hiển thị danh sách phim đã lưu
        for movie in movie_data_list:
            print(f"- {movie['title']} | IMDb: {movie.get('imdb_rating','N/A')} | Sentiment: {movie['sentiment']}")
    else:
        print("⚠️ Không có dữ liệu nào để lưu!")
