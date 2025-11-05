import pyodbc
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import warnings
import os

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

# ==============================
# 🎬 2. ĐỌC DỮ LIỆU TỪ SQL
# ==============================
df_ratings = pd.read_sql("SELECT * FROM RatingsCompare", conn)
df_sentiment = pd.read_sql("SELECT * FROM SentimentReviews", conn)
df_top = pd.read_sql("SELECT * FROM TopRatedMovies", conn)

print("📊 RatingsCompare:", df_ratings.shape)
print("💬 SentimentReviews:", df_sentiment.shape)
print("🌟 TopRatedMovies:", df_top.shape)

# Tạo thư mục lưu biểu đồ nếu chưa có
os.makedirs("charts", exist_ok=True)

# ==============================
# 📈 3. BIỂU ĐỒ 1 – Critics vs Audience (HUY)
# ==============================
if not df_ratings.empty:
    plt.figure(figsize=(15, 10))
    sns.set_style("whitegrid")

    df_ratings = df_ratings.dropna(subset=["Critics_score", "Audience_score", "Genre"])

    scatter = plt.scatter(
        df_ratings["Critics_score"],
        df_ratings["Audience_score"],
        s=(df_ratings["Review_count"] / 30).astype(float),
        alpha=0.7,
        c=pd.factorize(df_ratings["Genre"])[0],
        cmap="tab10",
        edgecolors="white",
        linewidths=0.7
    )

    plt.title("🎯 Biểu đồ 1 So sánh Critics vs Audience theo thể loại ", fontsize=14, weight="bold")
    plt.xlabel("Critics Score (%)")
    plt.ylabel("Audience Score (%)")
    plt.grid(True, linestyle="--", alpha=0.4)

    # Thêm chú thích thể loại
    handles, labels = scatter.legend_elements(prop="colors")
    plt.legend(handles, df_ratings["Genre"].unique(), title="Thể loại", bbox_to_anchor=(1.05, 1), loc="upper left")

    plt.tight_layout()
    plt.savefig("charts/critics_vs_audience_by_genre.png", dpi=300)
    plt.show()
else:
    print("⚠️ Không có dữ liệu trong RatingsCompare.")
# =========================
# 💬 2️⃣ Phân tích cảm xúc theo thể loại phim (DŨNG)
# =========================

query_sentiment = """
SELECT Genre, Sentiment_label, COUNT(*) AS ReviewCount
FROM SentimentReviews
GROUP BY Genre, Sentiment_label
"""
df_sentiment = pd.read_sql(query_sentiment, conn)

if not df_sentiment.empty:
    # Tổng số review mỗi thể loại
    total_by_genre = df_sentiment.groupby("Genre")["ReviewCount"].sum().reset_index()
    df_sentiment = df_sentiment.merge(total_by_genre, on="Genre", how="left", suffixes=("", "_Total"))
    df_sentiment["Percentage"] = (df_sentiment["ReviewCount"] / df_sentiment["ReviewCount_Total"]) * 100

    # Pivot để biểu đồ stacked bar 100%
    pivot_df = df_sentiment.pivot(index="Genre", columns="Sentiment_label", values="Percentage").fillna(0)

    # Đảm bảo đủ 3 cột cảm xúc
    for label in ["Positive", "Neutral", "Negative"]:
        if label not in pivot_df.columns:
            pivot_df[label] = 0
    pivot_df = pivot_df[["Positive", "Neutral", "Negative"]]

    # Thiết lập màu
    sentiment_colors = {
        "Positive": "#4CAF50",
        "Neutral": "#FFC107",
        "Negative": "#F44336"
    }

    # Vẽ stacked bar chart
    ax = pivot_df.plot(
        kind="bar",
        stacked=True,
        color=[sentiment_colors[col] for col in pivot_df.columns],
        figsize=(11, 6),
        edgecolor='black'
    )

    # Thêm tiêu đề & nhãn
    plt.title(" Tỷ lệ cảm xúc review theo thể loại phim", fontsize=15, weight="bold", pad=20)
    plt.xlabel("Thể loại phim", fontsize=12)
    plt.ylabel("Tỷ lệ (%)", fontsize=12)
    plt.xticks(rotation=30, ha="right")
    plt.legend(title="Cảm xúc", loc="upper right")

    # Thêm phần trăm lên cột
    for idx, genre in enumerate(pivot_df.index):
        y_offset = 0
        for sentiment in ["Positive", "Neutral", "Negative"]:
            value = pivot_df.loc[genre, sentiment]
            if value > 2:  # chỉ hiển thị nếu > 2%
                ax.text(
                    idx, 
                    y_offset + value / 2, 
                    f"{value:.1f}%", 
                    ha='center', va='center',
                    color='black', fontsize=10, weight='bold'
                )
            y_offset += value

    plt.tight_layout()
    os.makedirs("charts", exist_ok=True)
    plt.savefig("charts/sentiment_by_genre_percent.png", dpi=300)
    plt.show()

else:
    print("⚠️ Không có dữ liệu cảm xúc để hiển thị.")

# ==============================
# 🌟 5. BIỂU ĐỒ 3 – Top phim được đánh giá cao nhất (NGỌC)
# ==============================
if not df_top.empty:
    plt.figure(figsize=(8, 6))
    sns.set_style("whitegrid")

    # Lấy top 10 phim có Avg_score cao nhất
    df_top_sorted = df_top.nlargest(10, "Avg_score")

    sns.barplot(
        data=df_top_sorted,
        y="Title",
        x="Avg_score",
        hue="Genre",
        dodge=False
    )

    plt.title("🌟 Biểu đồ 3️ Top 10 phim được đánh giá cao nhất (NGỌC)", fontsize=14, weight="bold")
    plt.xlabel("Điểm trung bình (IMDb + RT + Metacritic)")
    plt.ylabel("Tên phim")
    plt.legend(title="Thể loại", bbox_to_anchor=(1.05, 1))
    plt.tight_layout()
    plt.savefig("charts/top_rated_movies.png", dpi=300)
    plt.show()
else:
    print("⚠️ Không có dữ liệu trong TopRatedMovies.")

# ==============================
# ✅ KẾT THÚC
# ==============================
conn.close()
print("🎉 Tất cả biểu đồ đã được tạo và lưu trong thư mục /charts/")
