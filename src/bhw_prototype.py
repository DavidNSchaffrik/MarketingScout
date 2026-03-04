import asyncio
from bs4 import BeautifulSoup
from pydoll.browser.chromium import Chrome
import re
import psycopg
import hashlib
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()


URL = "https://www.blackhatworld.com/seo/guide-future-proof-backlink-strategies-build-once-benefit-forever-2025-edition.1478334/"




def get_db_connection():
    return psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )

def hash_text(text):
    return hashlib.sha256(text.encode("utf-8")).digest()

def parse_html_for_class(html, selector):
    soup = BeautifulSoup(html, "html.parser")
    return soup.select(f".{selector}")


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


def extract_external_post_id(section):
    # Try to find a permalink containing "post-123456"
    a = section.select_one('a[href*="post-"]')
    if not a:
        return None

    href = a.get("href", "")
    m = re.search(r'post-(\d+)', href)
    return m.group(1) if m else None


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
        "username": extract_username(section),
        "time_posted": extract_time_posted(section),
        "post_content": extract_raw_post_text(section),
        "like_count" : extract_like_count(section)
    }


def print_post_data(post):
    if post["username"]:
        print(post["username"])

    if post["time_posted"]:
        print(post["time_posted"])

    if post["post_content"]:
        print(post["post_content"])

    if post["like_count"]:
        print(post["like_count"])

    print("likes:", post.get("like_count", 0))


async def start_browser():
    browser = Chrome()
    tab = await browser.start()
    return browser, tab


async def go_to_page(tab, url):
    await tab.go_to(url)
    await asyncio.sleep(3)


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


def save_posts(posts):

    with get_db_connection() as conn:
        with conn.cursor() as cur:

            # ensure source exists
            cur.execute("""
                INSERT INTO source (source_type, name, base_url)
                VALUES (%s,%s,%s)
                ON CONFLICT (source_type,name)
                DO UPDATE SET base_url=EXCLUDED.base_url
                RETURNING source_id
            """, ("forum","BlackHatWorld","https://www.blackhatworld.com"))

            source_id = cur.fetchone()[0]

            # ensure container exists (thread)
            cur.execute("""
                INSERT INTO container (source_id,container_type,external_container_id,canonical_url)
                VALUES (%s,%s,%s,%s)
                ON CONFLICT (source_id,external_container_id)
                DO UPDATE SET canonical_url=EXCLUDED.canonical_url
                RETURNING container_id
            """, (source_id,"thread","1478334",URL))

            container_id = cur.fetchone()[0]

            for post in posts:

                username = post["username"] or "unknown"

                # actor
                cur.execute("""
                    INSERT INTO actor (source_id,handle)
                    VALUES (%s,%s)
                    ON CONFLICT (source_id,handle)
                    DO UPDATE SET handle=EXCLUDED.handle
                    RETURNING actor_id
                """,(source_id,username))

                actor_id = cur.fetchone()[0]

                # post item
                cur.execute("""
                    INSERT INTO item (
                        source_id,
                        container_id,
                        item_type,
                        external_item_id,
                        actor_id,
                        score
                    )
                    VALUES (%s,%s,'forum_post',%s,%s,%s)
                    ON CONFLICT (source_id,external_item_id)
                    DO UPDATE SET
                        score=EXCLUDED.score,
                        scraped_last_at=now()
                    RETURNING item_id
                """,(
                    source_id,
                    container_id,
                    post["external_item_id"],
                    actor_id,
                    post["like_count"]
                ))

                item_id = cur.fetchone()[0]

                # content version
                text = post["post_content"] or ""
                text_hash = hash_text(text)

                cur.execute("""
                    INSERT INTO item_content (item_id,content_text,content_hash)
                    VALUES (%s,%s,%s)
                    ON CONFLICT (item_id,content_hash)
                    DO NOTHING
                """,(item_id,text,text_hash))

        conn.commit()

    print(f"Saved {len(posts)} posts to DB")

async def main():
    browser, tab = await start_browser()
    current_page = URL

    try:
        while True:
            await go_to_page(tab, current_page)
            html = await get_page_html(tab)

            posts = process_posts(html)
            save_posts(posts)
            print_posts(posts)

            next_page = get_next_page(html)
            if not next_page:
                break
            current_page = next_page
    finally:
        await stop_browser(browser)

asyncio.run(main())