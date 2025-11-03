import requests
import pyodbc
from textblob import TextBlob

# ==============================
# ⚙️ CẤU HÌNH KẾT NỐI DATABASE
# ==============================
SERVER = "DESKTOP-BTV4GU9"  # 👈 Đổi nếu bạn dùng server khác
DATABASE = "XUHUONGPHIM"
TMDB_KEY = "4f013f2a8509b8f4b1ef3205f0ca9f00"
OMDB_KEY = "a07802fd"

def get_db_connection():
    return pyodbc.connect(
        f"DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;"
    )

# =====================================
# ⚙️ HÀM LẤY DỮ LIỆU TỪ TMDb + OMDb
# =====================================
def get_movies_from_tmdb(pages=1):
    movies = []
    for page in range(1, pages + 1):
        url = f"https://api.themoviedb.org/3/discover/movie?api_key={TMDB_KEY}&language=en-US&sort_by=popularity.desc&page={page}"
        res = requests.get(url)
        if res.status_code != 200:
            print(f"⚠️ Lỗi API: {res.status_code} - {url}")
            continue
        data = res.json()
        for m in data.get("results", []):
            movies.append({
                "movie_id": f"tt{m['id']}",
                "title": m.get("title"),
                "release_date": m.get("release_date"),
            })
    print(f"✅ Lấy {len(movies)} phim từ TMDb")
    return movies

def enrich_movie_with_omdb(movie):
    imdb_id = movie["movie_id"]
    url = f"http://www.omdbapi.com/?i={imdb_id}&apikey={OMDB_KEY}"
    res = requests.get(url)
    if res.status_code != 200:
        return movie
    data = res.json()
    movie["country"] = data.get("Country")
    movie["language"] = data.get("Language")
    movie["studio"] = data.get("Production")
    movie["director"] = data.get("Director")
    movie["actors"] = data.get("Actors", "").split(", ")
    movie["genres"] = data.get("Genre", "").split(", ")
    movie["imdb_rating"] = data.get("imdbRating")
    movie["imdb_votes"] = data.get("imdbVotes", "0").replace(",", "")
    movie["plot"] = data.get("Plot", "")
    return movie

# =====================================
# 💾 LƯU DỮ LIỆU VÀO SQL SERVER
# =====================================
def save_to_database(movies):
    conn = get_db_connection()
    cursor = conn.cursor()

    for m in movies:
        # --- Thêm đạo diễn ---
        director_id = None
        if m.get("director"):
            cursor.execute("SELECT person_id FROM People WHERE person_name = ?", m["director"])
            result = cursor.fetchone()
            if result:
                director_id = result[0]
            else:
                cursor.execute("INSERT INTO People (person_name) VALUES (?)", m["director"])
                cursor.execute("SELECT @@IDENTITY")
                director_id = cursor.fetchone()[0]

        # --- Lưu phim ---
        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM Movies WHERE movie_id = ?)
            INSERT INTO Movies (movie_id, title, release_date, country, language, studio, director_id)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, m["movie_id"], m["movie_id"], m["title"], m["release_date"], m["country"], m["language"], m["studio"], director_id)

        # --- Thể loại ---
        for g in m.get("genres", []):
            g = g.strip()
            if not g:
                continue
            cursor.execute("IF NOT EXISTS (SELECT 1 FROM Genres WHERE genre_name = ?) INSERT INTO Genres (genre_name) VALUES (?)", g, g)
            cursor.execute("SELECT genre_id FROM Genres WHERE genre_name = ?", g)
            genre_id = cursor.fetchone()[0]
            cursor.execute("IF NOT EXISTS (SELECT 1 FROM Movie_Genres WHERE movie_id = ? AND genre_id = ?) INSERT INTO Movie_Genres (movie_id, genre_id) VALUES (?, ?)", m["movie_id"], genre_id, m["movie_id"], genre_id)

        # --- Diễn viên ---
        for actor in m.get("actors", []):
            actor = actor.strip()
            if not actor:
                continue
            cursor.execute("IF NOT EXISTS (SELECT 1 FROM People WHERE person_name = ?) INSERT INTO People (person_name) VALUES (?)", actor, actor)
            cursor.execute("SELECT person_id FROM People WHERE person_name = ?", actor)
            actor_id = cursor.fetchone()[0]
            cursor.execute("IF NOT EXISTS (SELECT 1 FROM Movie_Cast WHERE movie_id = ? AND person_id = ? AND role_type = 'Actor') INSERT INTO Movie_Cast (movie_id, person_id, role_type) VALUES (?, ?, 'Actor')", m["movie_id"], actor_id, m["movie_id"], actor_id)

        # --- Điểm đánh giá ---
        try:
            score = float(m["imdb_rating"]) if m["imdb_rating"] not in [None, "N/A", ""] else None
        except:
            score = None
        try:
            votes = int(m["imdb_votes"]) if m["imdb_votes"].isdigit() else None
        except:
            votes = None

        if score is not None:
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Ratings WHERE movie_id = ? AND source_name = 'IMDb')
                INSERT INTO Ratings (movie_id, source_name, score, vote_count)
                VALUES (?, 'IMDb', ?, ?)
            """, m["movie_id"], m["movie_id"], score, votes)

        # --- Phân tích cảm xúc ---
        plot = m.get("plot", "")
        if plot and plot != "N/A":
            blob = TextBlob(plot)
            polarity = blob.sentiment.polarity
            sentiment = "Positive" if polarity > 0 else "Negative" if polarity < 0 else "Neutral"

            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Movie_Sentiment WHERE movie_id = ?)
                INSERT INTO Movie_Sentiment (movie_id, sentiment)
                VALUES (?, ?)
            """, m["movie_id"], m["movie_id"], sentiment)

    conn.commit()
    conn.close()
    print("🎯 Crawl hoàn tất và đã lưu dữ liệu vào SQL Server!")

# =====================================
# 🚀 CHẠY CHÍNH
# =====================================
if __name__ == "__main__":
    movies = get_movies_from_tmdb(pages=1)
    full_data = [enrich_movie_with_omdb(m) for m in movies]
    save_to_database(full_data)
