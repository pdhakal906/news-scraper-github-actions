import os
import re
import requests
import psycopg2
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from psycopg2.extras import execute_values

DATABASE_URL = os.getenv("DATABASE_URL")
REVALIDATE_SECRET = os.getenv("REVALIDATE_SECRET")

# ---------- GLOBAL REUSABLE OBJECTS (IMPORTANT FOR LAMBDA WARM START) ----------

session = requests.Session()
session.headers.update(
    {"User-Agent": "PostmanRuntime/7.36.0", "X-Forwarded-For": "127.0.0.1"}
)
session.timeout = 30  # default timeout

connection = None

print("Session headers:")
print(session.headers)


# ---------- REGEX (COMPILED ONCE) ----------

SETOPATI_HREF = re.compile(r'<a href="(https://www\.setopati\.com/\w+/\d+)"')
SETOPATI_TITLE = re.compile(r'<h1 class="news-big-title">(.*?)</h1>')

EKANTIPUR_HREF = re.compile(
    r'<h2><a href="(https://ekantipur\.com/[a-zA-Z0-9_-]+/\d{4}/\d{2}/\d{2}/[a-zA-Z0-9-]+\.html)"'
)
EKANTIPUR_TITLE = re.compile(
    r'<section\s+class="news-section-wrap"[^>]*>\s*<h2[^>]*>(.*?)</h2>', re.DOTALL
)

# ---------- FETCH ARTICLE ----------


def fetch_article(url, title_regex, source):
    try:

        resp = session.get(url, timeout=10)
        print(f"Fetching {url} - Status Code: {resp.status_code}")
        resp.raise_for_status()

        match = title_regex.search(resp.text)

        if not match:
            return None
        print(f"Fetching {url} - Title: {match.group(1)}")
        return (
            match.group(1),  # title
            url,  # url
            None,  # image_url (not scraping for now)
            datetime.now(),
            source,
        )

    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None


# ---------- SCRAPE SITE ----------


def scrape_site(base_url, href_regex, title_regex, source):
    try:
        resp = session.get(base_url, timeout=10)
        print(f"Scraping {base_url} - Status Code: {resp.status_code}")
        if resp.status_code != 200:
            return []

        links = list(set(href_regex.findall(resp.text)))

        articles = []

        # Parallel fetch article pages
        with ThreadPoolExecutor(max_workers=5) as executor:
            futures = [
                executor.submit(fetch_article, link, title_regex, source)
                for link in links
            ]

            for future in as_completed(futures):
                result = future.result()
                if result:
                    articles.append(result)

        return articles

    except Exception as e:
        print(f"Error scraping {base_url}: {e}")
        return []


# ---------- MAIN PIPELINE ----------


def scrape_and_store():
    print("Starting scraping process...")
    # Parallelize both sites
    with ThreadPoolExecutor(max_workers=2) as executor:
        future_ekantipur = executor.submit(
            scrape_site,
            "https://www.ekantipur.com/",
            EKANTIPUR_HREF,
            EKANTIPUR_TITLE,
            "ekantipur",
        )

        future_setopati = executor.submit(
            scrape_site,
            "https://www.setopati.com/",
            SETOPATI_HREF,
            SETOPATI_TITLE,
            "setopati",
        )

        ekantipur_articles = future_ekantipur.result()
        setopati_articles = future_setopati.result()

    all_articles = ekantipur_articles + setopati_articles
    # print(all_articles)

    if not all_articles:
        return

    conn = psycopg2.connect(DATABASE_URL)
    cursor = conn.cursor()

    try:
        execute_values(
            cursor,
            """
            INSERT INTO api_news (title, link, image_url, created_at, source)
            VALUES %s
            ON CONFLICT (link) DO NOTHING
            """,
            all_articles,
        )
        conn.commit()
    except Exception as e:
        print("DB ERROR:", e)
        conn.rollback()
    finally:
        cursor.close()
        conn.close()

    requests.get("https://www.pratikdhakal906.com.np/news")

    # 2. Clear cache
    requests.get(
        "https://www.pratikdhakal906.com.np/api/revalidate",
        headers={"x-secret": REVALIDATE_SECRET},
    )

    # 3. Prewarm cache
    requests.get("https://www.pratikdhakal906.com.np/news")


scrape_and_store()
