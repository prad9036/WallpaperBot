import requests
from bs4 import BeautifulSoup
import psycopg2
import re
import os
from urllib.parse import urlparse

# Use DATABASE_URL style connection string
DATABASE_URL = os.getenv(
    "DATABASE_URL",
    "postgres://koyeb-ka-URI"
)

BASE_URL = "https://4kwallpapers.com"


def sanitize_tags(raw_tags):
    """Convert tags into Telegram-safe (no #, no spaces, only underscores)."""
    tags = []
    for t in raw_tags.split(","):
        t = t.strip()
        t = re.sub(r"\s+", "_", t)
        t = re.sub(r"[^A-Za-z0-9_]", "_", t)
        if t:
            tags.append(t)
    return tags


def get_highest_jpg(url):
    """Fetch wallpaper page and return highest resolution JPG URL."""
    html = requests.get(url).text
    matches = re.findall(r'/images/wallpapers/[^"]+\.jpe?g', html)
    if not matches:
        return None
    full_urls = [BASE_URL + m for m in matches]
    # pick highest by resolution
    best = None
    best_pixels = 0
    for u in full_urls:
        m = re.search(r'-(\d+)x(\d+)-\d+\.', u)
        if m:
            w, h = map(int, m.groups())
            pixels = w * h
            if pixels > best_pixels:
                best_pixels = pixels
                best = u
    return best


def already_in_db(cur, wallpaper_url, jpg_url):
    """Check if either wallpaper_url or jpg_url already exists."""
    cur.execute(
        """
        SELECT 1 FROM wallpapers
        WHERE wallpaper_url = %s OR jpg_url = %s
        LIMIT 1
        """,
        (wallpaper_url, jpg_url),
    )
    return cur.fetchone() is not None


def scrape_page(cur, conn, page_url, consecutive_skips):
    """Scrape one page and insert new rows into DB."""
    html = requests.get(page_url).text
    soup = BeautifulSoup(html, "html.parser")
    links = [a["href"] for a in soup.select("a.wallpapers__canvas_image")]

    for href in links:
        wallpaper_url = href if href.startswith("http") else BASE_URL + href
        category = wallpaper_url.split("/")[3]

        # fetch tags
        html2 = requests.get(wallpaper_url).text
        soup2 = BeautifulSoup(html2, "html.parser")
        meta = soup2.find("meta", {"name": "keywords"})
        tags = sanitize_tags(meta["content"]) if meta else []

        # highest res JPG
        jpg_url = get_highest_jpg(wallpaper_url)
        if not jpg_url:
            continue

        if already_in_db(cur, wallpaper_url, jpg_url):
            consecutive_skips += 1
            if consecutive_skips >= 50:
                print("50 consecutive matches found. Terminating.")
                return consecutive_skips, False
            continue

        # reset skip counter if new insert
        consecutive_skips = 0

        # Insert into DB
        cur.execute(
            """
            INSERT INTO wallpapers (category, wallpaper_url, jpg_url, tags)
            VALUES (%s, %s, %s, %s)
            """,
            (category, wallpaper_url, jpg_url, tags),
        )
        conn.commit()

        print("------ INSERTED ROW ------")
        print(f"category:      {category}")
        print(f"wallpaper_url: {wallpaper_url}")
        print(f"jpg_url:       {jpg_url}")
        print(f"tags:          {tags}")
        print("--------------------------\n")

    return consecutive_skips, True


def main():
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()

    page = 1
    consecutive_skips = 0

    while True:
        url = BASE_URL if page == 1 else f"{BASE_URL}/?page={page}"
        print(f"\n=== Scraping {url} ===")
        consecutive_skips, keep_going = scrape_page(cur, conn, url, consecutive_skips)
        if not keep_going:
            break
        page += 1

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
