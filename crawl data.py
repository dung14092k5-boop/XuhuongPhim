"""
crawl_data_safe.py

- Thu thập phim từ TMDb (API), lấy chi tiết + credits
- Lấy thông tin rating từ OMDb (API)
- (Tùy chọn) Lấy thêm một số trường từ IMDb bằng scraping nhẹ (ít requests, delays)
- Lưu vào SQL Server (XUHUONGPHIM) theo schema bạn cung cấp
- Cơ chế an toàn: headers rotator, retry + exponential backoff, rate-limit, tùy chọn proxy
"""

import requests
from bs4 import BeautifulSoup
import time
import random
import pyodbc
from datetime import datetime
from typing import Optional, Dict, Any, List
import logging
import os

# -------------------------
# CONFIG - sửa theo môi trường của bạn
# -------------------------
TMDB_API_KEY = os.getenv("TMDB_API_KEY", "4f013f2a8509b8f4b1ef3205f0ca9f00")
OMDB_API_KEY = os.getenv("OMDB_API_KEY", "a07802fd")

SQL_SERVER = os.getenv("SQL_SERVER", "localhost\\SQLEXPRESS")
SQL_DATABASE = os.getenv("SQL_DATABASE", "XUHUONGPHIM")
SQL_TRUSTED = True  # True dùng Trusted_Connection, nếu dùng user/pass thì chỉnh lại connection string
# Optional proxies dict like {"http": "http://user:pass@host:port", "https": "..."}
PROXIES = None

# CRAWL SETTINGS
TMDB_YEAR = 2025
TMDB_PAGES = 5          # số trang TMDb discover lấy (mỗi trang ~20 phim)
REQUESTS_TIMEOUT = 12
MIN_DELAY = 0.8         # giãn cách giữa request (s)
MAX_DELAY = 2.0
MAX_RETRIES = 3

# User-Agent rotation (giúp giảm khả năng bị chặn)
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.3 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
]

# Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s: %(message)s")

# -------------------------
# HELPERS: HTTP with retry + backoff
# -------------------------
def random_headers():
    return {"User-Agent": random.choice(USER_AGENTS), "Accept-Language": "en-US,en;q=0.9"}

def safe_get(url: str, params: dict = None, headers: dict = None, timeout: int = REQUESTS_TIMEOUT) -> Optional[requests.Response]:
    """GET with retries and exponential backoff."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            h = headers or random_headers()
            resp = requests.get(url, params=params, headers=h, timeout=timeout, proxies=PROXIES)
            if resp.status_code == 200:
                return resp
            # 429 or 5xx => backoff and retry
            if resp.status_code in (429, 500, 502, 503, 504):
                logging.warning("Status %s for %s — retry %d/%d", resp.status_code, url, attempt, MAX_RETRIES)
                time.sleep((2 ** attempt) + random.random())
                continue
            logging.warning("Unexpected status %s for %s", resp.status_code, url)
            return resp
        except requests.RequestException as e:
            logging.warning("Request error %s for %s — attempt %d/%d", e, url, attempt, MAX_RETRIES)
            time.sleep((2 ** attempt) + random.random())
    logging.error("Failed to GET %s after %d attempts", url, MAX_RETRIES)
    return None

# -------------------------
# DB helpers
# -------------------------
def get_db_connection():
    if SQL_TRUSTED:
        conn_str = f"DRIVER={{SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};Trusted_Connection=yes;"
    else:
        # nếu cần username/password, thay vào đây
        conn_str = f"DRIVER={{SQL Server}};SERVER={SQL_SERVER};DATABASE={SQL_DATABASE};UID=sa;PWD=YourPassword;"
    return pyodbc.connect(conn_str, autocommit=False)

def upsert_person(cursor, name: str) -> int:
    """Insert person if not exists, return person_id"""
    if not name:
        return None
    cursor.execute("SELECT person_id FROM People WHERE person_name = ?", name)
    row = cursor.fetchone()
    if row:
        return row[0]
    cursor.execute("INSERT INTO People (person_name) VALUES (?)", name)
    # get id by select
    cursor.execute("SELECT person_id FROM People WHERE person_name = ?", name)
    return cursor.fetchone()[0]

def upsert_genre(cursor, genre_name: str) -> int:
    cursor.execute("SELECT genre_id FROM Genres WHERE genre_name = ?", genre_name)
    r = cursor.fetchone()
    if r:
        return r[0]
    cursor.execute("INSERT INTO Genres (genre_name) VALUES (?)", genre_name)
    cursor.execute("SELECT genre_id FROM Genres WHERE genre_name = ?", genre_name)
    return cursor.fetchone()[0]

# -------------------------
# TMDb helpers
# -------------------------
def tmdb_discover(year: int = TMDB_YEAR, pages: int = TMDB_PAGES) -> List[Dict[str, Any]]:
    results = []
    base = "https://api.themoviedb.org/3/discover/movie"
    for page in range(1, pages + 1):
        params = {"api_key": TMDB_API_KEY, "primary_release_year": year, "sort_by": "popularity.desc", "page": page, "language": "en-US"}
        resp = safe_get(base, params=params)
        if resp is None:
            continue
        data = resp.json()
        items = data.get("results", [])
        results.extend(items)
        logging.info("TMDb discover: got %d items from page %d", len(items), page)
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    return results

def tmdb_get_details(tmdb_id: int) -> Optional[Dict[str, Any]]:
    base = f"https://api.themoviedb.org/3/movie/{tmdb_id}"
    params = {"api_key": TMDB_API_KEY, "language": "en-US"}
    resp = safe_get(base, params=params)
    if not resp:
        return None
    return resp.json()

def tmdb_get_credits(tmdb_id: int) -> Optional[Dict[str, Any]]:
    base = f"https://api.themoviedb.org/3/movie/{tmdb_id}/credits"
    params = {"api_key": TMDB_API_KEY}
    resp = safe_get(base, params=params)
    if not resp:
        return None
    return resp.json()

# -------------------------
# OMDb helper
# -------------------------
def omdb_get(title: str = None, imdb_id: str = None) -> Optional[dict]:
    base = "https://www.omdbapi.com/"
    if imdb_id:
        params = {"i": imdb_id, "apikey": OMDB_API_KEY}
    elif title:
        params = {"t": title, "apikey": OMDB_API_KEY}
    else:
        return None
    resp = safe_get(base, params=params)
    if not resp:
        return None
    data = resp.json()
    if data.get("Response") != "True":
        return None
    return data

# -------------------------
# Light IMDb scraping (tùy chọn) - rất nhẹ, hạn chế trường, tôn trọng delays
# -------------------------
def imdb_scrape_basic(imdb_url: str) -> dict:
    """
    Lấy thêm thể loại / country / language nếu TMDb/OMDb thiếu.
    Chỉ dùng khi thực sự cần và có imdb_url (ví dụ '/title/tt1234567/')
    """
    result = {"genres": [], "country": None, "language": None}
    full = imdb_url if imdb_url.startswith("http") else f"https://www.imdb.com{imdb_url}"
    resp = safe_get(full, headers=random_headers())
    if not resp:
        return result
    soup = BeautifulSoup(resp.text, "html.parser")
    try:
        # genres
        g_elems = soup.select("div[data-testid='genres'] a") or soup.select(".subtext a[href^='/search/']")
        result["genres"] = [g.get_text(strip=True) for g in g_elems][:5]
    except Exception:
        pass
    try:
        # country & language (try to find by data-testid)
        country_elem = soup.select_one("li[data-testid='title-details-origin'] a")
        if country_elem:
            result["country"] = country_elem.get_text(strip=True)
        language_elem = soup.select_one("li[data-testid='title-details-languages'] a")
        if language_elem:
            result["language"] = language_elem.get_text(strip=True)
    except Exception:
        pass
    time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))
    return result

# -------------------------
# Save record to DB (main function)
# -------------------------
def save_movie_record(details: dict, credits: dict, omdb: dict):
    conn = get_db_connection()
    cursor = conn.cursor()
    try:
        # Director
        director_id = None
        for c in credits.get("crew", []) if credits else []:
            if c.get("job") == "Director":
                director_id = upsert_person(cursor, c.get("name"))
                break

        # Movie ID: prefer imdb, fallback tmdb_{id}
        imdb_id = details.get("imdb_id")
        movie_id = imdb_id if imdb_id else f"tmdb_{details.get('id')}"
        title = details.get("title") or details.get("original_title")
        # release date
        rd = details.get("release_date")
        release_date = None
        if rd:
            try:
                release_date = datetime.strptime(rd, "%Y-%m-%d").date()
            except:
                release_date = None

        # country, language from details if available
        country = None
        if details.get("production_countries"):
            arr = details.get("production_countries", [])
            if len(arr) > 0:
                country = arr[0].get("iso_3166_1")

        language = details.get("original_language")

        # Insert Movies if not exists
        cursor.execute("SELECT 1 FROM Movies WHERE movie_id = ?", movie_id)
        if not cursor.fetchone():
            cursor.execute("INSERT INTO Movies (movie_id, title, release_date, country, language, director_id) VALUES (?, ?, ?, ?, ?, ?)",
                           movie_id, title, release_date, country, language, director_id)

        # Genres (from details)
        for g in details.get("genres", []):
            gname = g.get("name")
            if not gname:
                continue
            genre_id = upsert_genre(cursor, gname)
            cursor.execute("SELECT 1 FROM Movie_Genres WHERE movie_id = ? AND genre_id = ?", movie_id, genre_id)
            if not cursor.fetchone():
                cursor.execute("INSERT INTO Movie_Genres (movie_id, genre_id) VALUES (?, ?)", movie_id, genre_id)

        # Cast - top 5
        for cast in (credits.get("cast", [])[:5] if credits else []):
            name = cast.get("name")
            if not name:
                continue
            person_id = upsert_person(cursor, name)
            cursor.execute("SELECT 1 FROM Movie_Cast WHERE movie_id = ? AND person_id = ? AND role_type = 'Actor'", movie_id, person_id)
            if not cursor.fetchone():
                cursor.execute("INSERT INTO Movie_Cast (movie_id, person_id, role_type) VALUES (?, ?, 'Actor')", movie_id, person_id)

        # Financials
        budget = details.get("budget")
        revenue = details.get("revenue")
        cursor.execute("SELECT 1 FROM Financials WHERE movie_id = ?", movie_id)
        if not cursor.fetchone():
            cursor.execute("INSERT INTO Financials (movie_id, budget, revenue_domestic, revenue_international) VALUES (?, ?, ?, ?)",
                           movie_id, budget if budget else None, revenue if revenue else None, None)

        # Ratings - from OMDb if available
        if omdb:
            try:
                imdb_rating = float(omdb.get("imdbRating")) if omdb.get("imdbRating") and omdb.get("imdbRating") != "N/A" else None
            except:
                imdb_rating = None
            try:
                imdb_votes = int(omdb.get("imdbVotes").replace(",", "")) if omdb.get("imdbVotes") and omdb.get("imdbVotes") != "N/A" else None
            except:
                imdb_votes = None

            if imdb_rating is not None:
                cursor.execute("SELECT 1 FROM Ratings WHERE movie_id = ? AND source_name = 'IMDb'", movie_id)
                if not cursor.fetchone():
                    cursor.execute("INSERT INTO Ratings (movie_id, source_name, score, vote_count, last_updated) VALUES (?, 'IMDb', ?, ?, GETDATE())",
                                   movie_id, imdb_rating, imdb_votes)

        # Streaming popularity - mock data if you don't have real source
        cursor.execute("SELECT 1 FROM Streaming_Popularity WHERE movie_id = ? AND platform_name = 'Netflix'", movie_id)
        if not cursor.fetchone():
            cursor.execute("INSERT INTO Streaming_Popularity (movie_id, platform_name, rank, hours_viewed, measurement_week) VALUES (?, 'Netflix', ?, ?, GETDATE())",
                           movie_id, random.randint(1, 50), random.randint(10000, 500000))

        conn.commit()
        logging.info("Saved movie: %s (%s)", title, movie_id)
    except Exception as e:
        conn.rollback()
        logging.exception("Error saving movie %s: %s", details.get("title"), e)
    finally:
        conn.close()

# -------------------------
# ENTRY POINT
# -------------------------
def main():
    logging.info("Start crawling TMDb discover pages...")
    discover_list = tmdb_discover(year=TMDB_YEAR, pages=TMDB_PAGES)
    logging.info("Total discover items: %d", len(discover_list))

    for item in discover_list:
        tmdb_id = item.get("id")
        try:
            details = tmdb_get_details(tmdb_id)
            credits = tmdb_get_credits(tmdb_id)
            imdb_id = details.get("imdb_id")
            # OMDb preferred by imdb_id
            omdb_info = None
            if imdb_id:
                omdb_info = omdb_get(imdb_id=imdb_id)
            else:
                omdb_info = omdb_get(title=details.get("title"))

            # If critical metadata missing (e.g., genres/country), optionally scrape IMDb lightly
            # imdb_url = details.get("homepage") or (f"https://www.imdb.com/title/{imdb_id}/" if imdb_id else None)
            # if imdb_id and (not details.get("genres") or not details.get("production_countries")):
            #     imdb_extras = imdb_scrape_basic(f"/title/{imdb_id}/")
            #     # merge imdb_extras into details if needed (left as optional)

            save_movie_record(details, credits, omdb_info)

        except Exception as e:
            logging.exception("Skipping TMDb id %s due to error: %s", tmdb_id, e)

        # politeness: brief randomized sleep
        time.sleep(random.uniform(MIN_DELAY, MAX_DELAY))

    logging.info("Crawling finished.")

if __name__ == "__main__":
    main()

