import requests
import pyodbc
import time

# --- CONFIG ---
TMDB_KEY = "4f013f2a8509b8f4b1ef3205f0ca9f00"
OMDB_KEY = "a07802fd"
SERVER = "DESKTOP-BTV4GU9"
DATABASE = "XUHUONGPHIM"

# --- DB CONNECT ---
def get_db_connection():
    return pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    )

# --- CRAWL MOVIES ---
def crawl_movies():
    url = f"https://api.themoviedb.org/3/discover/movie?api_key={TMDB_KEY}&language=en-US&sort_by=popularity.desc&page=1"
    res = requests.get(url)
    if res.status_code != 200:
        print(f"⚠️ Lỗi API TMDB: {res.status_code} - {res.url}")
        return []

    movies = res.json().get("results", [])
    data = []
    for m in movies[:10]:  # lấy 10 phim cho nhanh
        movie_id = f"tt{m['id']}"
        title = m["title"]
        release_date = m.get("release_date", None)
        print(f"🎬 {title}")

        # --- Gọi OMDb để lấy thêm rating ---
        omdb_url = f"https://www.omdbapi.com/?apikey={OMDB_KEY}&t={title}"
        omdb_res = requests.get(omdb_url)
        omdb_data = omdb_res.json()

        imdb_rating = None
        if "imdbRating" in omdb_data and omdb_data["imdbRating"] != "N/A":
            imdb_rating = float(omdb_data["imdbRating"])

        data.append({
            "movie_id": movie_id,
            "title": title,
            "release_date": release_date,
            "imdb_rating": imdb_rating,
        })
        time.sleep(1)
    return data

# --- SAVE TO SQL ---
def save_to_database(data):
    conn = get_db_connection()
    cursor = conn.cursor()

    for m in data:
        try:
            # Insert Movies
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Movies WHERE movie_id = ?)
                INSERT INTO Movies (movie_id, title, release_date)
                VALUES (?, ?, ?)
            """, m["movie_id"], m["movie_id"], m["title"], m["release_date"])

            # Insert Ratings
            if m["imdb_rating"] is not None:
                cursor.execute("""
                    INSERT INTO Ratings (movie_id, source_name, score)
                    VALUES (?, ?, ?)
                """, m["movie_id"], "IMDb", m["imdb_rating"])

            conn.commit()
            print(f"✅ Lưu thành công: {m['title']}")
        except Exception as e:
            print(f"❌ Lỗi khi lưu {m['title']}: {e}")
            conn.rollback()

    conn.close()
    print("🎯 Crawl và lưu dữ liệu hoàn tất!")

if __name__ == "__main__":
    movies_data = crawl_movies()
    if movies_data:
        save_to_database(movies_data)
    else:
        print("⚠️ Không có dữ liệu phim nào được tải!")