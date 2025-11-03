import requests
import pyodbc
import socket
import subprocess

# ==============================
# ⚙️ Cấu hình cơ bản
# ==============================
API_KEY = "YOUR_TMDB_API_KEY"  # 👈 Thay bằng TMDb API key thật
DATABASE = "XUHUONGPHIM"
SERVER = r"DESKTOP-XXXX\SQLEXPRESS"  # 👈 Thay bằng tên server SQL thật của bạn

# ==============================
# 🔌 Kết nối SQL Server
# ==============================
def get_db_connection():
    try:
        conn = pyodbc.connect(
            f"DRIVER={{SQL Server}};SERVER={SERVER};DATABASE={DATABASE};Trusted_Connection=yes;",
            timeout=5
        )
        print(f"✅ Đã kết nối đến SQL Server: {SERVER}")
        return conn
    except pyodbc.Error as e:
        print("❌ Không thể kết nối SQL Server.")
        print("Chi tiết lỗi:", e)
        print("\n👉 Kiểm tra lại:")
        print("   1️⃣ SQL Server đã bật chưa (Configuration Manager → SQL Server Services).")
        print("   2️⃣ Tên server có đúng không. Dưới đây là danh sách có thể dùng:\n")
        try:
            result = subprocess.run(["sqlcmd", "-L"], capture_output=True, text=True, timeout=8)
            print(result.stdout)
        except Exception:
            print("⚠️ Không thể dò server. Bạn có thể mở SQL Server Management Studio để kiểm tra.")
        exit(1)

# ==============================
# 🎬 Crawl dữ liệu phim
# ==============================
def crawl_movies():
    url = f"https://api.themoviedb.org/3/discover/movie?api_key={API_KEY}&language=en-US&sort_by=popularity.desc&page=1"
    response = requests.get(url)

    if response.status_code != 200:
        print(f"⚠️ Lỗi API: {response.status_code} - {url}")
        return []

    data = response.json().get("results", [])
    print(f"✅ Lấy {len(data)} phim từ TMDb")
    movies = []
    for m in data[:10]:  # lấy 10 phim mẫu
        movie = {
            "movie_id": f"tt{m['id']}",
            "title": m.get("title"),
            "release_date": m.get("release_date"),
            "country": "US",
            "language": m.get("original_language"),
            "studio": "Unknown",
            "director": "N/A",
            "genres": [str(gid) for gid in m.get("genre_ids", [])],
            "budget": 0,
            "revenue_domestic": 0,
            "revenue_international": 0,
            "rating_source": "TMDb",
            "score": m.get("vote_average"),
            "vote_count": m.get("vote_count"),
            "platform_name": "Netflix",
            "rank": 0,
            "hours_viewed": 0,
            "sentiment": "Positive" if m.get("vote_average", 0) > 7 else "Neutral",
        }
        movies.append(movie)
    return movies

# ==============================
# 💾 Lưu dữ liệu vào SQL Server
# ==============================
def save_to_database(movies):
    conn = get_db_connection()
    cursor = conn.cursor()

    for m in movies:
        try:
            # Movies
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Movies WHERE movie_id = ?)
                INSERT INTO Movies (movie_id, title, release_date, country, language, studio)
                VALUES (?, ?, ?, ?, ?, ?)
            """, m["movie_id"], m["movie_id"], m["title"], m["release_date"], m["country"], m["language"], m["studio"])

            # Ratings
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Ratings WHERE movie_id = ? AND source_name = ?)
                INSERT INTO Ratings (movie_id, source_name, score, vote_count)
                VALUES (?, ?, ?, ?)
            """, m["movie_id"], m["rating_source"], m["movie_id"], m["rating_source"], m["score"], m["vote_count"])

            # Genres
            for gid in m["genres"]:
                cursor.execute("""
                    IF NOT EXISTS (SELECT 1 FROM Genres WHERE genre_name = ?)
                    INSERT INTO Genres (genre_name) VALUES (?)
                """, gid, gid)

            # Movie_Sentiment
            cursor.execute("""
                IF NOT EXISTS (SELECT 1 FROM Movie_Sentiment WHERE movie_id = ?)
                INSERT INTO Movie_Sentiment (movie_id, sentiment)
                VALUES (?, ?)
            """, m["movie_id"], m["movie_id"], m["sentiment"])

        except pyodbc.Error as e:
            print(f"❌ Lỗi khi lưu {m['title']}: {e}")
            conn.rollback()

    conn.commit()
    conn.close()
    print("🎯 Crawl & lưu dữ liệu hoàn tất!")

# ==============================
# 🚀 Chạy chính
# ==============================
if __name__ == "__main__":
    movies_data = crawl_movies()
    if movies_data:
        save_to_database(movies_data)
