# file: top_movies_chart.py
import pyodbc
import pandas as pd
import matplotlib.pyplot as plt

# ==========================
# 1) Kết nối SQL Server
# ==========================
def get_db_connection():
    return pyodbc.connect(
        "DRIVER={ODBC Driver 18 for SQL Server};"   # đổi thành {ODBC Driver 17 for SQL Server} nếu máy bạn dùng 17
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=XUHUONGPHIM;"
        "Trusted_Connection=yes;"
        "Encrypt=yes;TrustServerCertificate=yes;"
    )

# ==========================
# 2) Lấy TOP phim với điểm trung bình (0–100)
# ==========================
def load_top_movies(top_n=10):
    conn = get_db_connection()
    sql = """
    WITH R AS (
      SELECT
        movie_id,
        MAX(CASE WHEN source_name='IMDb'           THEN score*10 END) AS imdb_100,
        MAX(CASE WHEN source_name='RottenTomatoes' THEN score       END) AS rt_100,
        MAX(CASE WHEN source_name='Metacritic'     THEN score       END) AS mc_100,
        MAX(CASE WHEN source_name='TMDb'           THEN score       END) AS tmdb_100
      FROM Ratings
      GROUP BY movie_id
    ),
    GD AS (
      SELECT DISTINCT MG.movie_id, G.genre_name
      FROM Movie_Genres MG
      JOIN Genres G ON G.genre_id = MG.genre_id
    ),
    S AS (
      SELECT
        M.movie_id,
        M.title,
        ISNULL(STRING_AGG(GD.genre_name, ', '), 'Khác') AS genres,
        CAST((
              ISNULL(R.imdb_100,0)+ISNULL(R.rt_100,0)+ISNULL(R.mc_100,0)+ISNULL(R.tmdb_100,0)
            ) / NULLIF(
              (CASE WHEN R.imdb_100 IS NOT NULL THEN 1 ELSE 0 END)+
              (CASE WHEN R.rt_100   IS NOT NULL THEN 1 ELSE 0 END)+
              (CASE WHEN R.mc_100   IS NOT NULL THEN 1 ELSE 0 END)+
              (CASE WHEN R.tmdb_100 IS NOT NULL THEN 1 ELSE 0 END), 0
            ) AS FLOAT) AS avg_score
      FROM Movies M
      JOIN R  ON R.movie_id = M.movie_id
      LEFT JOIN GD ON GD.movie_id = M.movie_id
      WHERE R.imdb_100 IS NOT NULL OR R.rt_100 IS NOT NULL
         OR R.mc_100 IS NOT NULL OR R.tmdb_100 IS NOT NULL
      GROUP BY M.movie_id, M.title, R.imdb_100, R.rt_100, R.mc_100, R.tmdb_100
    )
    SELECT title, genres, avg_score
    FROM S
    ORDER BY avg_score DESC, title ASC
    OFFSET 0 ROWS FETCH NEXT ? ROWS ONLY;
    """
    df = pd.read_sql(sql, conn, params=(top_n,))
    conn.close()
    return df

# ==========================
# 3) Vẽ Horizontal Bar Chart (màu theo thể loại)
# ==========================
def plot_top_movies(df):
    if df.empty:
        print("⚠️ Không có dữ liệu điểm số trong Ratings. Hãy backfill hoặc crawl lại để có dữ liệu.")
        return

    # Lấy “thể loại chính” = mục đầu tiên trong chuỗi genres
    main_genre = df["genres"].fillna("Khác").apply(lambda s: s.split(",")[0].strip() if s else "Khác")
    df = df.assign(main_genre=main_genre)

    # Sắp xếp GIẢM DẦN để phim điểm cao nhất nằm TRÊN CÙNG
    df = df.sort_values("avg_score", ascending=False)

    titles = df["title"].tolist()
    scores = df["avg_score"].tolist()

    # Màu theo thể loại (tab20)
    unique_genres = df["main_genre"].unique().tolist()
    genre_to_idx = {g: i for i, g in enumerate(unique_genres)}
    cmap = plt.cm.get_cmap("tab20", len(unique_genres))
    colors = [cmap(genre_to_idx[g]) for g in df["main_genre"]]

    plt.figure(figsize=(10, 6))
    bars = plt.barh(titles, scores, edgecolor="black", linewidth=0.5, color=colors)

    # Ghi nhãn điểm ở cuối thanh
    for b, s in zip(bars, scores):
        plt.text(b.get_width() + 0.8, b.get_y() + b.get_height()/2, f"{s:.1f}",
                 va="center", ha="left", fontsize=9)

    plt.title("Top phim được đánh giá cao nhất (Ngọc)")
    plt.xlabel("Điểm trung bình (0–100)")
    plt.ylabel("Tên phim")
    plt.xlim(0, max(100, (max(scores) if scores else 100) + 5))
    # KHÔNG đảo trục — giữ nguyên thứ tự từ trên xuống
    # plt.gca().invert_yaxis()

    # Legend theo thể loại
    handles = [
        plt.Line2D([0], [0], marker="s", linestyle="", markerfacecolor=cmap(genre_to_idx[g]),
                   markeredgecolor="black", label=g)
        for g in unique_genres
    ]
    plt.legend(handles=handles, title="Thể loại", loc="best")
    plt.tight_layout()
    plt.show()

# ==========================
# 4) MAIN
# ==========================
if __name__ == "__main__":
    top_n = 10
    df = load_top_movies(top_n)
    print(f"Đã lấy {len(df)} phim có điểm trung bình.")
    plot_top_movies(df)
