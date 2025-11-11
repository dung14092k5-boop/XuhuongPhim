import pyodbc
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import warnings, os
from sklearn.model_selection import train_test_split
from sklearn.linear_model import LinearRegression
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import r2_score, mean_absolute_error

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
df_top = pd.read_sql("""
    SELECT Movie_id, Title, Genre, Avg_score, Vote_count, Release_year
    FROM TopRatedMovies
""", conn)

df_sentiment = pd.read_sql("""
    SELECT Movie_id, AVG(Sentiment_score) AS Sentiment_avg
    FROM SentimentReviews
    GROUP BY Movie_id
""", conn)

# ==============================
# 🧩 3. GỘP DỮ LIỆU
# ==============================
df = pd.merge(df_top, df_sentiment, on="Movie_id", how="inner")

# ==============================
# 🧠 4. TÍNH TREND_SCORE (%)
# ==============================
df['log_vote'] = np.log1p(df['Vote_count'])
max_log = df['log_vote'].max()

df['trend_score'] = 100 * (
    0.5 * (df['Avg_score'] / 100) +
    0.3 * ((df['Sentiment_avg'] + 1) / 2) +
    0.2 * (df['log_vote'] / max_log)
)
df['trend_score'] = df['trend_score'].round(2)

print("\n📊 Kiểm tra dữ liệu mô tả:")
print(df[['Avg_score', 'Sentiment_avg', 'Vote_count', 'trend_score']].describe())

# ==============================
# 📈 5. PHÂN TÍCH TƯƠNG QUAN
# ==============================
plt.figure(figsize=(6,4))
sns.heatmap(df[['Avg_score','Sentiment_avg','Vote_count','trend_score']].corr(), annot=True, cmap='coolwarm', fmt=".2f")
plt.title("🔥 Ma trận tương quan giữa các biến")
plt.tight_layout()
os.makedirs("charts", exist_ok=True)
plt.savefig("charts/correlation_matrix.png", dpi=300)
plt.show()

# ==============================
# 🤖 6. XÂY DỰNG MÔ HÌNH HỒI QUY
# ==============================
print("\n🤖 Huấn luyện mô hình Linear Regression...")

# --- CHUẨN HÓA DỮ LIỆU ---
X = df[['Avg_score', 'Sentiment_avg', 'Vote_count', 'Release_year']].copy()
X['Vote_count'] = np.log1p(X['Vote_count'])  # giảm chênh lệch
y = df['trend_score']

scaler = StandardScaler()
X_scaled = scaler.fit_transform(X)

# --- CHIA TRAIN/TEST ---
X_train, X_test, y_train, y_test = train_test_split(X_scaled, y, test_size=0.2, random_state=42)

# --- HUẤN LUYỆN ---
model = LinearRegression()
model.fit(X_train, y_train)
y_pred = model.predict(X_test)

# --- ĐÁNH GIÁ ---
r2 = r2_score(y_test, y_pred)
mae = mean_absolute_error(y_test, y_pred)

print(f"✅ R² Score: {r2:.4f}")
print(f"📉 MAE: {mae:.4f}\n")

# --- HỆ SỐ ẢNH HƯỞNG ---
coef_df = pd.DataFrame({
    'Yếu tố': X.columns,
    'Hệ số ảnh hưởng': model.coef_.round(4)
}).sort_values(by='Hệ số ảnh hưởng', ascending=False)

print("💡 Ảnh hưởng của từng yếu tố đến trend_score:")
print(coef_df)

plt.figure(figsize=(8,5))
sns.barplot(data=coef_df, x='Hệ số ảnh hưởng', y='Yếu tố', palette='coolwarm')
plt.title('📊 Ảnh hưởng của các yếu tố đến Trend Score')
plt.tight_layout()
plt.savefig("charts/factor_influence.png", dpi=300)
plt.show()

# ==============================
# 📊 8. TRỰC QUAN HÓA KẾT QUẢ DỰ ĐOÁN
# ==============================

print("\n📊 Đang tạo biểu đồ Scatter Plot và Bar Chart...")

# --- SCATTER PLOT: trend thực tế vs trend dự đoán ---
plt.figure(figsize=(6,6))
plt.scatter(y_test, y_pred, alpha=0.6, color='teal', edgecolors='k')
plt.plot([y_test.min(), y_test.max()], [y_test.min(), y_test.max()], 'r--', lw=2)
plt.xlabel("Trend Score thực tế (%)")
plt.ylabel("Trend Score dự đoán (%)")
plt.title("📈 So sánh Trend Score thực tế và dự đoán")
plt.tight_layout()
plt.savefig("charts/scatter_actual_vs_predicted.png", dpi=300)
plt.show()

# --- PHÂN TÍCH BỔ SUNG: trend trung bình theo thể loại ---
genre_mean = df.groupby("Genre")['trend_score'].mean().reset_index()
genre_mean['trend_score'] = genre_mean['trend_score'].round(2)
genre_mean = genre_mean.sort_values(by='trend_score', ascending=False)

plt.figure(figsize=(10,6))
sns.barplot(data=genre_mean, x='trend_score', y='Genre', palette='coolwarm')
plt.xlabel("Trend Score trung bình (%)")
plt.ylabel("Thể loại phim")
plt.title("🔥 Xu hướng phổ biến theo thể loại")
plt.tight_layout()
plt.savefig("charts/bar_genre_trend_mean.png", dpi=300)
plt.show()


print("✅ Biểu đồ Scatter Plot và Bar Chart đã lưu trong thư mục /charts/")

# ==============================
# ✅ KẾT THÚC
# ==============================
conn.close()
print("🎉 Hoàn tất! Biểu đồ đã lưu tại thư mục /charts/")
