import pyodbc
import pandas as pd
from textblob import TextBlob
import matplotlib.pyplot as plt

# ==========================
# 1️⃣ Kết nối database
# ==========================
def get_db_connection():
    return pyodbc.connect(
        "DRIVER={SQL Server};"
        "SERVER=localhost\\SQLEXPRESS;"
        "DATABASE=XUHUONGPHIM;"
        "Trusted_Connection=yes;"
    )

# ==========================
# 2️⃣ Đọc dữ liệu từ database
# ==========================
def load_movie_data():
    conn = get_db_connection()
    query = """
        SELECT m.title, m.language, m.country, r.score AS imdb_rating, r.vote_count
        FROM Movies m
        LEFT JOIN Ratings r ON m.movie_id = r.movie_id AND r.source_name = 'IMDb'
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# ==========================
# 3️⃣ Phân tích cảm xúc từ mô tả phim
# ==========================
def analyze_sentiment(text):
    if not text or pd.isna(text):
        return "Neutral"
    polarity = TextBlob(text).sentiment.polarity
    if polarity > 0.1:
        return "Positive"
    elif polarity < -0.1:
        return "Negative"
    else:
        return "Neutral"

# ==========================
# 4️⃣ Vẽ biểu đồ
# ==========================
def visualize_sentiments(df):
    sentiments = df['Sentiment'].value_counts()

    # Pie chart
    plt.figure(figsize=(6,6))
    plt.pie(sentiments, labels=sentiments.index, autopct='%1.1f%%', startangle=90)
    plt.title("📊 Phân tích cảm xúc các bộ phim")
    plt.show()

    # Bar chart by language
    plt.figure(figsize=(8,5))
    df.groupby(['language', 'Sentiment']).size().unstack(fill_value=0).plot(kind='bar')
    plt.title("Cảm xúc theo ngôn ngữ phim")
    plt.xlabel("Ngôn ngữ")
    plt.ylabel("Số lượng phim")
    plt.tight_layout()
    plt.show()

# ==========================
# 5️⃣ MAIN
# ==========================
if __name__ == "__main__":
    df = load_movie_data()
    print(f"📥 Đã tải {len(df)} phim từ database.")
    df['Sentiment'] = df['title'].apply(analyze_sentiment)
    visualize_sentiments(df)
