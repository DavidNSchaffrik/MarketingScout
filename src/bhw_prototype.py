import asyncio
from bs4 import BeautifulSoup
from pydoll.browser.chromium import Chrome
import re
import psycopg
import hashlib
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
import random
from pathlib import Path

load_dotenv()

def load_thread_urls():
    path = os.getenv("THREADS_FILE", "data/seeds/threads.txt")

    project_root = Path(__file__).resolve().parent.parent
    file_path = project_root / path

    if not file_path.exists():
        raise FileNotFoundError(f"Threads file not found: {file_path}")

    urls = []
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            u = line.strip()
            if not u or u.startswith("#"):
                continue
            urls.append(u)

    return urls

def get_db_connection():
    return psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )

def fetched_recently_conn(conn, url: str, days: int) -> bool:
    if days <= 0:
        return False
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1
            FROM page_fetch
            WHERE url = %s
              AND fetched_at >= now() - make_interval(days => %s)
            LIMIT 1;
        """, (url, days))
        return cur.fetchone() is not None

def log_fetch_conn(conn, url, status="ok", error=None):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO page_fetch (url, status, error)
            VALUES (%s, %s, %s)
        """, (url, status, error))

def hash_text(text):
    return hashlib.sha256(text.encode("utf-8")).digest()

def parse_html_for_class(html, selector):
    soup = BeautifulSoup(html, "html.parser")
    return soup.select(f".{selector}")

def extract_thread_id(url):
    m = re.search(r"\.(\d+)/?$", url)
    return m.group(1) if m else None

def extract_post_permalink(section):
    a = section.select_one('a[href*="post-"]')
    if not a:
        return None

    href = a.get("href", "").strip()
    if not href:
        return None

    # make absolute
    if href.startswith("/"):
        return "https://www.blackhatworld.com" + href
    if href.startswith("http"):
        return href

    return None

def extract_like_count(section) -> int:
    el = section.select_one(".reactionsBar-link")
    if not el:
        return 0

    text = el.get_text(" ", strip=True)
    if not text:
        return 0

    t = text.lower()

    # "No Likes"
    if "no likes" in t:
        return 0

    # Common formats:
    # 1) "Topiano"  -> 1
    # 2) "A, B, C" -> 3
    # 3) "A, B, C and 4 others" -> 3 + 4 = 7
    # 4) "A and 1 other" -> 2

    # If there's an "and N other(s)" piece, count that plus the names before it.
    m = re.search(r"\band\s+(\d+)\s+other[s]?\b", t)
    if m:
        others = int(m.group(1))

        # everything before " and N other(s)"
        before = re.split(r"\band\s+\d+\s+other[s]?\b", text, maxsplit=1)[0].strip()

        # count comma-separated names in the "before" part
        names = [p.strip() for p in before.split(",") if p.strip()]
        return len(names) + others

    # Otherwise, if there are commas, it's a list of names: "A, B, C"
    if "," in text:
        names = [p.strip() for p in text.split(",") if p.strip()]
        return len(names)

    # Otherwise it's a single username: "Topiano"
    return 1

async def crawl_thread(tab, conn, thread_url: str):
    skip_days = get_skip_days()
    if fetched_recently_conn(conn, thread_url, skip_days):
        print(f"Thread fetched in last {skip_days} day(s), skipping: {thread_url}")
        return

    current_page = thread_url

    while True:
        await go_to_page(tab, current_page)
        html = await get_page_html(tab)

        # Log fetch using the SAME conn
        log_fetch_conn(conn, current_page, status="ok", error=None)

        posts = process_posts(html)
        save_posts(conn, posts, thread_url)

        # commit once per page (safe + simple)
        conn.commit()

        print(f"Fetched {current_page} -> {len(posts)} posts")

        next_page = get_next_page(html)
        if not next_page:
            break
        current_page = next_page

def extract_external_post_id(section):
    # Try to find a permalink containing "post-123456"
    a = section.select_one('a[href*="post-"]')
    if not a:
        return None

    href = a.get("href", "")
    m = re.search(r'post-(\d+)', href)
    return m.group(1) if m else None

def get_skip_days(default: int = 7) -> int:
    raw = os.getenv("CRAWL_SKIP_DAYS", str(default)).strip()
    try:
        days = int(raw)
        return max(days, 0)
    except ValueError:
        return default
    
def extract_username(section):
    user = section.select_one(".username.username--wide")
    if user:
        return user.get_text(strip=True)
    return None

def extract_time_posted(section):
    time_posted = section.select_one(".message-attribution-main.listInline")
    if time_posted:
        return time_posted.get_text(strip=True)
    return None

def parse_bhw_date(date_str):
    if not date_str:
        return None

    try:
        dt = datetime.strptime(date_str, "%b %d, %Y")
        return dt.replace(tzinfo=timezone.utc)
    except ValueError:
        return None

def extract_raw_post_text(section):
    post_contents = section.select_one(".message-body.js-selectToQuote")

    if post_contents:
        for quote in post_contents.select("blockquote"):
            quote.decompose()

        return post_contents.get_text(strip=True)

    return None

def extract_post_data(section):
    return {
        "external_item_id": extract_external_post_id(section),
        "canonical_url": extract_post_permalink(section),
        "username": extract_username(section),
        "time_posted": extract_time_posted(section),
        "post_content": extract_raw_post_text(section),
        "like_count": extract_like_count(section)
    }

def print_post_data(post):
    print("user:", post.get("username"))
    print("time:", post.get("time_posted"))
    print("likes:", post.get("like_count", 0))
    print("post_id:", post.get("external_item_id"))
    print("text:", (post.get("post_content") or "")[:120], "...")

async def start_browser():
    browser = Chrome()
    tab = await browser.start()
    return browser, tab

async def go_to_page(tab, url):
    await tab.go_to(url)
    await asyncio.sleep(random.uniform(2.0, 5.0))

async def get_page_html(tab):
    return await tab.page_source

async def stop_browser(browser):
    await browser.stop()

def process_posts(html):
    sections = parse_html_for_class(html, "message-inner")
    posts = []
    for section in sections:
        post = extract_post_data(section)
        if not post["external_item_id"]:
            continue
        posts.append(post)
    return posts

def get_next_page(html):
    buttons = parse_html_for_class(html, "pageNav-jump--next")

    if buttons:
        href = buttons[0].get("href")
        return "https://www.blackhatworld.com" + href

    return None
    
def print_posts(posts):
    for post in posts:
        print_post_data(post)
        print("-" * 40)

def save_posts(conn, posts, thread_url: str):
    with conn.cursor() as cur:

        # ensure source exists
        cur.execute("""
            INSERT INTO source (source_type, name, base_url)
            VALUES (%s,%s,%s)
            ON CONFLICT (source_type,name)
            DO UPDATE SET base_url=EXCLUDED.base_url
            RETURNING source_id
        """, ("forum", "BlackHatWorld", "https://www.blackhatworld.com"))
        source_id = cur.fetchone()[0]

        # dynamic thread id from the passed-in URL
        thread_id = extract_thread_id(thread_url)
        if not thread_id:
            raise ValueError(f"Could not extract thread id from URL: {thread_url}")

        # ensure container exists (thread)
        cur.execute("""
            INSERT INTO container (source_id,container_type,external_container_id,canonical_url)
            VALUES (%s,%s,%s,%s)
            ON CONFLICT (source_id,external_container_id)
            DO UPDATE SET canonical_url=EXCLUDED.canonical_url
            RETURNING container_id
        """, (source_id, "thread", thread_id, thread_url))
        container_id = cur.fetchone()[0]

        for post in posts:
            if not post.get("external_item_id"):
                continue

            username = (post.get("username") or "unknown").strip()
            published_at = parse_bhw_date(post.get("time_posted"))
            like_count = int(post.get("like_count", 0) or 0)
            content_text = (post.get("post_content") or "").strip()
            canonical_url = post.get("canonical_url")

            # actor
            cur.execute("""
                INSERT INTO actor (source_id,handle)
                VALUES (%s,%s)
                ON CONFLICT (source_id,handle)
                DO UPDATE SET handle=EXCLUDED.handle
                RETURNING actor_id
            """, (source_id, username))
            actor_id = cur.fetchone()[0]

            # item (post)
            cur.execute("""
                INSERT INTO item (
                    source_id,
                    container_id,
                    item_type,
                    external_item_id,
                    canonical_url,
                    actor_id,
                    score,
                    published_at
                )
                VALUES (%s,%s,'forum_post',%s,%s,%s,%s,%s)
                ON CONFLICT (source_id,external_item_id)
                DO UPDATE SET
                    canonical_url = EXCLUDED.canonical_url,
                    score = EXCLUDED.score,
                    published_at = EXCLUDED.published_at,
                    scraped_last_at = now()
                RETURNING item_id
            """, (
                source_id,
                container_id,
                str(post["external_item_id"]),
                canonical_url,
                actor_id,
                like_count,
                published_at
            ))
            item_id = cur.fetchone()[0]

            # content version
            if content_text:
                text_hash = hash_text(content_text)

                cur.execute("""
                    INSERT INTO item_content (item_id,content_text,content_hash,is_current)
                    VALUES (%s,%s,%s,true)
                    ON CONFLICT (item_id,content_hash) DO NOTHING
                    RETURNING item_content_id;
                """, (item_id, content_text, text_hash))

                new_row = cur.fetchone()
                if new_row:
                    new_content_id = new_row[0]
                    cur.execute("""
                        UPDATE item_content
                        SET is_current = false
                        WHERE item_id = %s AND item_content_id <> %s;
                    """, (item_id, new_content_id))

    conn.commit()
    print(f"Saved {len(posts)} posts to DB")

async def main():
    thread_urls = load_thread_urls()
    if not thread_urls:
        raise ValueError("No thread URLs found. Check THREADS_FILE / threads.txt")

    # ONE DB connection for the whole run
    conn = get_db_connection()

    browser, tab = await start_browser()
    try:
        for i, thread_url in enumerate(thread_urls, start=1):
            print(f"\n[{i}/{len(thread_urls)}] Crawling: {thread_url}")
            try:
                await crawl_thread(tab, conn, thread_url)
            except Exception as e:
                # log thread-level error using SAME conn
                log_fetch_conn(conn, thread_url, status="error", error=str(e))
                conn.commit()
                print("Error crawling thread:", thread_url, "|", e)

    finally:
        try:
            conn.close()
        except Exception:
            pass
        await stop_browser(browser)

asyncio.run(main())


