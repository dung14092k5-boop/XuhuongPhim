import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# ==============================
# Kết nối đến SQL Server
# ==============================
def get_connection():
    conn = pyodbc.connect(
        "Driver={ODBC Driver 17 for SQL Server};"
        "Server=localhost;"
        "Database=XUHUONGPHIM;"
        "Trusted_Connection=yes;"
    )
    return conn


# ==============================
# Truy vấn dữ liệu phim và điểm đánh giá
# ==============================
def load_ratings_data():
    conn = get_connection()
    query = """
        SELECT 
            m.title,
            g.genre_name,
            r.critics_score,
            r.audience_score,
            ISNULL(r.vote_count, 1000) AS vote_count
        FROM Movies m
        JOIN Movie_Genres mg ON m.movie_id = mg.movie_id
        JOIN Genres g ON mg.genre_id = g.genre_id
        LEFT JOIN Ratings r ON m.movie_id = r.movie_id
        WHERE r.critics_score IS NOT NULL
          AND r.audience_score IS NOT NULL
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df


# ==============================
# Biểu đồ Scatter: Critics vs Audience (mỗi thể loại 1 màu)
# ==============================
def plot_critics_vs_audience(df):
    plt.figure(figsize=(12, 8))
    genres = sorted(df["genre_name"].unique())

    # ✅ Dải màu mịn và khác biệt
    cmap = plt.cm.get_cmap("gist_ncar", len(genres))
    colors = [cmap(i / len(genres)) for i in range(len(genres))]

    # Vẽ scatter plot cho từng thể loại
    for i, genre in enumerate(genres):
        subset = df[df["genre_name"] == genre]
        plt.scatter(
            subset["audience_score"],
            subset["critics_score"],
            s=subset["vote_count"] / subset["vote_count"].max() * 250 + 40,
            color=[colors[i]],
            alpha=0.8,
            edgecolors="black",
            linewidths=0.6,
            label=genre
        )

    # Đường chéo biểu thị đường cân bằng Critics = Audience
    plt.plot([40, 100], [40, 100], "r--", label="Critics = Audience")

    # Giới hạn zoom từ 40–100
    plt.xlim(40, 100)
    plt.ylim(40, 100)

    plt.title("Critics vs. Audience Scores theo từng thể loại phim", fontsize=15, fontweight="bold")
    plt.xlabel("Audience Score", fontsize=12)
    plt.ylabel("Critics Score", fontsize=12)
    plt.grid(True, linestyle="--", alpha=0.5)

    # Legend gọn gàng
    plt.legend(
        loc="center left",
        bbox_to_anchor=(1, 0.5),
        fontsize=9,
        title="Thể loại phim",
        title_fontsize=10,
        ncol=1
    )

    plt.tight_layout()
    plt.show()


# ==============================
# Biểu đồ Bar Chart trung bình theo thể loại
# ==============================
def plot_genre_bar_chart(df):
    genre_avg = (
        df.groupby("genre_name")[["critics_score", "audience_score"]]
        .mean()
        .sort_values("critics_score", ascending=False)
    )

    genre_avg.plot(kind="bar", figsize=(12, 6), color=["#d62728", "#1f77b4"])
    plt.title("Điểm trung bình theo thể loại phim", fontsize=15, fontweight="bold")
    plt.xlabel("Thể loại phim (Genre)")
    plt.ylabel("Điểm trung bình")
    plt.legend(["Critics Score", "Audience Score"])
    plt.xticks(rotation=45, ha="right")
    plt.grid(axis="y", linestyle="--", alpha=0.6)
    plt.tight_layout()
    plt.show()


# ==============================
# Main
# ==============================
def main():
    df = load_ratings_data()
    print(f"📊 Đã tải {len(df)} phim có dữ liệu đánh giá.\n")
    print(df.head())

    plot_critics_vs_audience(df)
    plot_genre_bar_chart(df)


if __name__ == "__main__":
    main()
