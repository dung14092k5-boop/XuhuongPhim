import pandas as pd
import pyodbc
import matplotlib.pyplot as plt
import seaborn as sns

# 1️⃣ Kết nối SQL Server
conn = pyodbc.connect(
    "DRIVER={SQL Server};"
    "SERVER=DESKTOP-BTV4GU9;"
    "DATABASE=XUHUONGPHIM;"
    "Trusted_Connection=yes;"
)

# 2️⃣ Lấy dữ liệu IMDb
query = """
SELECT m.title, g.genre_name, r.score
FROM Ratings r
JOIN Movies m ON r.movie_id = m.movie_id
JOIN Movie_Genres mg ON m.movie_id = mg.movie_id
JOIN Genres g ON mg.genre_id = g.genre_id
WHERE r.source_name = 'IMDb';
"""
df = pd.read_sql(query, conn)
conn.close()

print("✅ Số lượng dòng dữ liệu:", len(df))
print(df.head())

# 3️⃣ Biểu đồ 1: Phân bố điểm IMDb
plt.figure(figsize=(8,5))
sns.histplot(df["score"], bins=10, kde=True)
plt.title("Phân bố điểm IMDb")
plt.xlabel("Điểm IMDb")
plt.ylabel("Số lượng phim")
plt.show()

# 4️⃣ Biểu đồ 2: Trung bình điểm IMDb theo thể loại
plt.figure(figsize=(10,5))
avg_score = df.groupby("genre_name")["score"].mean().reset_index().sort_values(by="score", ascending=False)
sns.barplot(data=avg_score, x="score", y="genre_name", palette="viridis")
plt.title("Điểm IMDb trung bình theo thể loại")
plt.xlabel("Điểm trung bình")
plt.ylabel("Thể loại")
plt.show()

# 5️⃣ Biểu đồ 3: Top 10 phim có điểm IMDb cao nhất
plt.figure(figsize=(10,5))
top_movies = df.sort_values(by="score", ascending=False).head(10)
sns.barplot(data=top_movies, x="score", y="title", hue="genre_name", dodge=False)
plt.title("Top 10 phim có điểm IMDb cao nhất")
plt.xlabel("Điểm IMDb")
plt.ylabel("Tên phim")
plt.legend(title="Thể loại", bbox_to_anchor=(1.05, 1), loc='upper left')
plt.show()
