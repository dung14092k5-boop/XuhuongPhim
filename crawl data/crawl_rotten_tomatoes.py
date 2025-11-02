"""
Cập nhật dữ liệu Rotten Tomatoes vào SQL Server
Tìm kiếm phim dựa trên tên và cập nhật ratings
Cập nhật trực tiếp critics_score và audience_score vào hàng IMDb/Metacritic
Tự động tính mean và điền vào các giá trị NULL ngay trong CSDL

VERSION: 3.0 (Logic mới: Cập nhật trực tiếp & Tự động điền Mean)
"""

from crawlbase import CrawlingAPI
from bs4 import BeautifulSoup
import json
import pyodbc
from datetime import datetime
import re
import numpy as np 

# ======================
# 🔑 CONFIG
# ======================
CRAWLBASE_TOKEN = 'pWKGcx1K2GOVwYP75IVCvg'
SQL_DRIVER = "ODBC Driver 17 for SQL Server"
SQL_SERVER = "localhost"
SQL_DATABASE = "XUHUONGPHIM"

# ======================
# 🔌 DATABASE CONNECTION
# ======================
def get_db_connection():
    conn_str = (
        f"DRIVER={{{SQL_DRIVER}}};"
        f"SERVER={SQL_SERVER};"
        f"DATABASE={SQL_DATABASE};"
        "Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str, autocommit=False)

# ======================
# 🍅 CRAWL ROTTEN TOMATOES
# ======================
def fetch_rotten_tomatoes_data():
    """Crawl dữ liệu từ Rotten Tomatoes"""
    print("\n" + "="*80)
    print("🍅 CRAWLING ROTTEN TOMATOES")
    print("="*80)
    
    crawling_api = CrawlingAPI({'token': CRAWLBASE_TOKEN})
    url = 'https://www.rottentomatoes.com/browse/movies_in_theaters/sort:top_box_office'
    options = {
        'ajax_wait': 'true',
        'page_wait': '5000',
        'css_click_selector': 'button[data-qa="dlp-load-more-button"]'
    }
    
    try:
        response = crawling_api.get(url, options)
        
        if response.get('status_code') == 200:
            print("✅ Crawl thành công!")
            html_content = response['body'].decode('utf-8')
            
            soup = BeautifulSoup(html_content, 'html.parser')
            movies = soup.select('div[data-qa="discovery-media-list"] > div.flex-container')
            
            movie_data = []
            for movie in movies:
                title_elem = movie.select_one('span[data-qa="discovery-media-list-item-title"]')
                critics_elem = movie.select_one('rt-text[slot="criticsScore"]')
                audience_elem = movie.select_one('rt-text[slot="audienceScore"]')
                link_elem = movie.select_one('a[data-qa^="discovery-media-list-item"]')
                
                if title_elem:
                    movie_data.append({
                        'title': title_elem.text.strip(),
                        'critics_score': critics_elem.text.strip() if critics_elem else None,
                        'audience_score': audience_elem.text.strip() if audience_elem else None,
                        'link': 'https://www.rottentomatoes.com' + link_elem['href'] if link_elem else None
                    })
            
            print(f"✅ Đã lấy {len(movie_data)} phim từ Rotten Tomatoes")
            return movie_data
        else:
            print(f"❌ Failed. Status code: {response.get('status_code')}")
            return []
            
    except Exception as e:
        print(f"❌ Error: {e}")
        return []

# ======================
# 🔍 TÌM PHIM TRONG DATABASE
# ======================
def normalize_title(title):
    """Chuẩn hóa tên phim để so sánh"""
    normalized = re.sub(r'[^\w\s]', '', title.lower())
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized

def find_movie_in_db(db_movies, rt_movie):
    """
    Tìm phim trong danh sách db_movies (đã tải trước) dựa trên tên.
    Không query CSDL trong hàm này.
    """
    rt_title = rt_movie['title']
    normalized_rt = normalize_title(rt_title)
    
    matches = []
    for movie_id, db_title in db_movies:
        normalized_db = normalize_title(db_title)
        if normalized_rt == normalized_db:
            return movie_id, db_title, "exact" # Ưu tiên exact match
        if normalized_rt in normalized_db or normalized_db in normalized_rt:
            matches.append((movie_id, db_title, "partial"))
    
    if matches:
        return matches[0] # Lấy partial match đầu tiên
    
    return None, None, None

# ======================
# 💾 CẬP NHẬT RATINGS (LOGIC MỚI VÀ TỰ ĐỘNG FILL MEAN)
# ======================
def update_ratings(rt_data):
    """
    Cập nhật ratings vào database.
    LOGIC MỚI: 
    1. Ghi đè critics/audience score TRỰC TIẾP vào hàng 'IMDb' & 'Metacritic'.
    2. Tính toán Mean và UPDATE tất cả các hàng CÒN LẠI (NULL) bằng Mean.
    """
    print("\n" + "="*80)
    print("💾 CẬP NHẬT DỮ LIỆU VÀO SQL SERVER (LOGIC MỚI + FILL MEAN)")
    print("="*80)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    stats = {
        'exact_match': 0, 'partial_match': 0, 'no_match': 0,
        'updated_critics': 0, 'updated_audience': 0
    }
    
    # === TỐI ƯU HÓA: LẤY DỮ LIỆU 1 LẦN ===
    print("Đang tải danh sách phim từ CSDL...")
    cursor.execute("SELECT movie_id, title FROM Movies")
    db_movies = cursor.fetchall()
    print(f"✅ Đã tải {len(db_movies)} phim.")
    
    results = []
    
    try:
        # ================================================
        # VÒNG LẶP 1: CẬP NHẬT PHIM CRAWL TỪ ROTTEN TOMATOES
        # ================================================
        for rt_movie in rt_data:
            movie_id, db_title, match_type = find_movie_in_db(db_movies, rt_movie)
            
            result = {
                'rt_title': rt_movie['title'], 'db_title': db_title,
                'movie_id': movie_id, 'match_type': match_type,
                'critics_updated': False, 'audience_updated': False
            }
            
            if movie_id:
                if match_type == "exact":
                    stats['exact_match'] += 1
                    print(f"\n✅ EXACT MATCH: '{rt_movie['title']}'")
                else:
                    stats['partial_match'] += 1
                    print(f"\n⚠️ PARTIAL MATCH:")
                    print(f"    RT: '{rt_movie['title']}'")
                    print(f"    DB: '{db_title}'")
                
                print(f"    Movie ID: {movie_id}")
                
                # Critics Score
                if rt_movie['critics_score']:
                    try:
                        score = float(rt_movie['critics_score'].replace('%', ''))
                        cursor.execute("""
                            UPDATE Ratings 
                            SET critics_score = ?, last_updated = GETDATE()
                            WHERE movie_id = ? 
                              AND source_name IN ('IMDb', 'Metacritic')
                        """, score, movie_id)
                        
                        if cursor.rowcount > 0:
                            stats['updated_critics'] += 1
                            result['critics_updated'] = True
                            print(f"    -> Đã cập nhật critics_score = {score} cho 'IMDb'/'Metacritic'")
                        
                    except Exception as e:
                        print(f"    ❌ Error updating critics score: {e}")
                
                # Audience Score
                if rt_movie['audience_score']:
                    try:
                        score = float(rt_movie['audience_score'].replace('%', ''))
                        cursor.execute("""
                            UPDATE Ratings 
                            SET audience_score = ?, last_updated = GETDATE()
                            WHERE movie_id = ? 
                              AND source_name IN ('IMDb', 'Metacritic')
                        """, score, movie_id)
                        
                        if cursor.rowcount > 0:
                            stats['updated_audience'] += 1
                            result['audience_updated'] = True
                            print(f"    -> Đã cập nhật audience_score = {score} cho 'IMDb'/'Metacritic'")

                    except Exception as e:
                        print(f"    ❌ Error updating audience score: {e}")
            else:
                stats['no_match'] += 1
                print(f"\n❌ NO MATCH: '{rt_movie['title']}'")
            
            results.append(result)
        
        # === KẾT THÚC VÒNG LẶP 1 ===

        # ================================================
        # VÒNG LẶP 2: TÍNH MEAN VÀ CẬP NHẬT CÁC HÀNG NULL
        # ================================================
        print("\n" + "-"*80)
        print("📊 ĐANG TÍNH TOÁN MEAN VÀ CẬP NHẬT CÁC HÀNG CÒN LẠI...")
        
        try:
            # 1. Lấy TẤT CẢ điểm đã có để tính mean
            cursor.execute("""
                SELECT critics_score, audience_score 
                FROM Ratings 
                WHERE source_name IN ('IMDb', 'Metacritic')
            """)
            all_scores = cursor.fetchall()

            rt_c_scores = np.array([r[0] for r in all_scores if r[0] is not None])
            rt_a_scores = np.array([r[1] for r in all_scores if r[1] is not None])

            mean_rt_c = np.mean(rt_c_scores) if rt_c_scores.size > 0 else 0
            mean_rt_a = np.mean(rt_a_scores) if rt_a_scores.size > 0 else 0

            print(f"📈 Mean tính được: Critics = {mean_rt_c:.1f}, Audience = {mean_rt_a:.1f}")

            # 2. Cập nhật critics_score cho các hàng BỊ NULL
            if mean_rt_c > 0:
                cursor.execute("""
                    UPDATE Ratings
                    SET critics_score = ?
                    WHERE critics_score IS NULL
                      AND source_name IN ('IMDb', 'Metacritic')
                """, mean_rt_c)
                print(f"✅ Đã cập nhật {cursor.rowcount} hàng (critics_score) bị NULL bằng mean.")

            # 3. Cập nhật audience_score cho các hàng BỊ NULL
            if mean_rt_a > 0:
                cursor.execute("""
                    UPDATE Ratings
                    SET audience_score = ?
                    WHERE audience_score IS NULL
                      AND source_name IN ('IMDb', 'Metacritic')
                """, mean_rt_a)
                print(f"✅ Đã cập nhật {cursor.rowcount} hàng (audience_score) bị NULL bằng mean.")
            
            print(f"✅ Hoàn tất điền NULL bằng mean.")

        except Exception as e:
            print(f"❌ Lỗi khi cập nhật NULL bằng mean: {e}")
            
        print("-" * 80)

        # ================================================
        # COMMIT VÀ IN THỐNG KÊ
        # ================================================
        conn.commit()
        
        print("\n📊 THỐNG KÊ CẬP NHẬT (TỪ CRAWL)")
        print(f"✅ Exact Match: {stats['exact_match']}")
        print(f"⚠️ Partial Match: {stats['partial_match']}")
        print(f"❌ No Match: {stats['no_match']}")
        print(f"🍅 Critics Score Updated: {stats['updated_critics']}")
        print(f"🍅 Audience Score Updated: {stats['updated_audience']}")
        
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error: {e}")
    finally:
        conn.close()
    
    return results, stats

# ======================
# 📊 TRỰC QUAN HÓA DỮ LIỆU (ĐÃ ĐƠN GIẢN HÓA)
# ======================
def visualize_data():
    """Lấy và hiển thị dữ liệu sau khi cập nhật.
    Không cần tính mean ở đây nữa vì update_ratings đã làm."""
    print("\n" + "="*80)
    print("📊 TRỰC QUAN HÓA DỮ LIỆU (ĐÃ ĐIỀN MEAN TỪ TRƯỚC)")
    print("="*80)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    try:
        # === CÂU QUERY ĐÃ ĐƯỢC ĐƠN GIẢN HÓA ===
        query = """
        SELECT 
            m.title,
            r1.score as imdb_rating,
            r1.critics_score as rt_critics,    -- Lấy từ hàng IMDb
            r1.audience_score as rt_audience,  -- Lấy từ hàng IMDb
            r4.score as metacritic
        FROM Movies m
        LEFT JOIN Ratings r1 ON m.movie_id = r1.movie_id AND r1.source_name = 'IMDb'
        -- Không cần JOIN r2 (Rotten Tomatoes) và r3 (Rotten Tomatoes Audience) nữa
        LEFT JOIN Ratings r4 ON m.movie_id = r4.movie_id AND r4.source_name = 'Metacritic'
        ORDER BY m.title
        """
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print("⚠️ Không có dữ liệu để hiển thị.")
            return
        
        # *** KHÔNG CẦN TÍNH MEAN Ở ĐÂY NỮA ***
        
        print(f"\n{'TITLE':<40} {'IMDb':<8} {'RT-C':<8} {'RT-A':<8} {'Meta':<8}")
        print("-" * 80)
        
        for row in rows:
            title = row[0][:37] + "..." if len(row[0]) > 40 else row[0]
            # Dùng giá trị 0.0 làm dự phòng nếu có lỗi (dù không nên có NULL)
            imdb = row[1] if row[1] is not None else 0.0
            rt_c = row[2] if row[2] is not None else 0.0
            rt_a = row[3] if row[3] is not None else 0.0
            meta = row[4] if row[4] is not None else 0.0
            
            print(f"{title:<40} {imdb:<8.1f} {rt_c:<8.1f} {rt_a:<8.1f} {meta:<8.1f}")
        
        print(f"\n✅ Total: {len(rows)} phim có dữ liệu")
        
    except Exception as e:
        print(f"❌ Error: {e}")
    finally:
        conn.close()

# ======================
# 🚀 MAIN
# ======================
def main():
    rt_data = fetch_rotten_tomatoes_data()
    if not rt_data:
        print("❌ Không có dữ liệu để cập nhật")
        return
    
    with open('rt_raw_data.json', 'w', encoding='utf-8') as f:
        json.dump(rt_data, f, indent=4, ensure_ascii=False)
    print(f"\n💾 Đã lưu raw data vào 'rt_raw_data.json'")
    
    results, stats = update_ratings(rt_data)
    
    with open('rt_update_results.json', 'w', encoding='utf-8') as f:
        json.dump({
            'stats': stats,
            'results': results,
            'timestamp': datetime.now().isoformat()
        }, f, indent=4, ensure_ascii=False)
    print(f"\n💾 Đã lưu kết quả vào 'rt_update_results.json'")
    
    visualize_data()
    
    print("\n" + "="*80)
    print("✅ HOÀN TẤT!")
    print("="*80)

if __name__ == "__main__":
    main()
