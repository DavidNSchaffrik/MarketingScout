import asyncio
import threading
from bs4 import BeautifulSoup
import cloudscraper
import re
import psycopg
import hashlib
from datetime import datetime, timezone
import os
from dotenv import load_dotenv
import random
from pathlib import Path
from urllib.parse import urljoin

load_dotenv(override=False)


# ---------------------------------------------------------------------------
# Proxy loading (cached)
# ---------------------------------------------------------------------------

_proxies_cache: list[dict] | None = None

def load_proxies() -> list[dict]:
    global _proxies_cache
    if _proxies_cache is not None:
        return _proxies_cache

    path = os.getenv("PROXIES_FILE", "data/proxies.txt")
    project_root = Path(__file__).resolve().parent.parent
    file_path = project_root / path

    if not file_path.exists():
        return []

    proxies = []
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            parts = line.split(":")
            if len(parts) != 4:
                continue
            proxies.append({
                "host": parts[0],
                "port": int(parts[1]),
                "username": parts[2],
                "password": parts[3],
            })

    _proxies_cache = proxies
    return _proxies_cache


# ---------------------------------------------------------------------------
# Scraper / HTTP
# ---------------------------------------------------------------------------

def make_scraper(proxy: dict | None = None) -> cloudscraper.CloudScraper:
    scraper = cloudscraper.create_scraper()
    if proxy:
        proxy_url = f"http://{proxy['username']}:{proxy['password']}@{proxy['host']}:{proxy['port']}"
        scraper.proxies = {"http": proxy_url, "https": proxy_url}
    return scraper


def _fetch_page_sync(scraper: cloudscraper.CloudScraper, url: str) -> str | None:
    """Synchronous fetch — always called via fetch_page_async in crawl loops."""
    try:
        response = scraper.get(url, timeout=30)
        if response.status_code == 200:
            return response.text
        print(f"HTTP {response.status_code} for {url}")
        return None
    except Exception as e:
        print(f"Request failed: {e}")
        return None


async def fetch_page_async(scraper: cloudscraper.CloudScraper, url: str) -> str | None:
    """
    Non-blocking fetch. Runs the synchronous cloudscraper call in a thread
    executor so the asyncio event loop stays alive during the HTTP request.
    This means stop-signal checks and sleeps are never frozen by a slow fetch.
    """
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(None, _fetch_page_sync, scraper, url)


# Sync alias kept for any call sites that aren't in async context.
fetch_page = _fetch_page_sync

# ---------------------------------------------------------------------------
# Worker session
# ---------------------------------------------------------------------------

class WorkerSession:
    """Tracks per-worker state for rate limiting and proxy rotation."""

    def __init__(self, worker_id: int):
        self.worker_id = worker_id
        self.proxy_index = worker_id
        self.consecutive_errors = 0
        self.requests_on_current_proxy = 0
        self.rotate_after = int(os.getenv("PROXY_ROTATE_AFTER", "50"))
        self.scraper = make_scraper(self.get_proxy())

    def get_proxy(self) -> dict | None:
        proxies = load_proxies()
        if not proxies:
            return None
        return proxies[self.proxy_index % len(proxies)]

    def get_scraper(self) -> cloudscraper.CloudScraper:
        return self.scraper

    def on_timeout(self):
        print(f"[Worker {self.worker_id}] timeout, rotating proxy")
        self._rotate_proxy(reason="timeout")

    def on_success(self):
        self.consecutive_errors = 0
        self.requests_on_current_proxy += 1
        if self.requests_on_current_proxy >= self.rotate_after:
            self._rotate_proxy(reason="scheduled")

    def on_error(self):
        self.consecutive_errors += 1
        if self.consecutive_errors >= 3:
            self._rotate_proxy(reason="errors")

    def _rotate_proxy(self, reason: str = ""):
        proxies = load_proxies()
        if not proxies:
            return
        self.proxy_index = (self.proxy_index + len(proxies) // 2 + 1) % len(proxies)
        self.requests_on_current_proxy = 0
        self.consecutive_errors = 0
        proxy = self.get_proxy()
        self.scraper = make_scraper(proxy)
        print(f"[Worker {self.worker_id}] Proxy rotated ({reason}) -> {proxy['host']}:{proxy['port']}")

    def get_delay(self) -> float:
        base_min = float(os.getenv("CRAWL_DELAY_MIN", "2.0"))
        base_max = float(os.getenv("CRAWL_DELAY_MAX", "5.0"))
        backoff = min(2 ** self.consecutive_errors, 60)
        return random.uniform(base_min + backoff, base_max + backoff)


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def update_worker_stats(conn, worker_id: int, worker_type: str, pages_fetched: int = 0, posts_saved: int = 0, errors: int = 0, last_url: str = None, status: str = "running"):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO worker_stats (worker_id, worker_type, pages_fetched, posts_saved, errors, last_url, status, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, now())
            ON CONFLICT (worker_id, worker_type) DO UPDATE SET
                pages_fetched = worker_stats.pages_fetched + EXCLUDED.pages_fetched,
                posts_saved = worker_stats.posts_saved + EXCLUDED.posts_saved,
                errors = worker_stats.errors + EXCLUDED.errors,
                last_url = COALESCE(EXCLUDED.last_url, worker_stats.last_url),
                status = EXCLUDED.status,
                updated_at = now()
        """, (worker_id, worker_type, pages_fetched, posts_saved, errors, last_url, status))
    conn.commit()


def cleanup_stale_workers(conn, worker_id: int, worker_type: str):
    """
    Mark this worker as 'running' on startup and clear any leftover stop
    signals from a previous session so the dashboard starts clean.
    """
    # Clear any stale stop signal left from last run
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM stop_signals
            WHERE worker_id = %s AND worker_type = %s
        """, (worker_id, worker_type))

    # Reset status to 'running' (upsert so the row exists immediately)
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO worker_stats (worker_id, worker_type, pages_fetched, posts_saved, errors, status, updated_at)
            VALUES (%s, %s, 0, 0, 0, 'running', now())
            ON CONFLICT (worker_id, worker_type) DO UPDATE SET
                status = 'running',
                updated_at = now()
        """, (worker_id, worker_type))

    conn.commit()
    print(f"[{worker_type.capitalize()} {worker_id}] Cleaned up stale state, marked as running.")


def check_stop_signal(conn, worker_id: int, worker_type: str) -> bool:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM stop_signals
            WHERE worker_id = %s AND worker_type = %s
        """, (worker_id, worker_type))
        return cur.fetchone() is not None

def clear_stop_signal(conn, worker_id: int, worker_type: str):
    with conn.cursor() as cur:
        cur.execute("""
            DELETE FROM stop_signals
            WHERE worker_id = %s AND worker_type = %s
        """, (worker_id, worker_type))
    conn.commit()


async def interruptible_sleep(signal_conn, worker_id: int, worker_type: str, delay: float, interval: float = 0.5) -> bool:
    """
    Sleep for `delay` seconds, waking every `interval` seconds to check for a
    stop signal on the dedicated signal_conn. Returns True if sleep completed
    normally, False if interrupted.
    """
    elapsed = 0.0
    while elapsed < delay:
        await asyncio.sleep(min(interval, delay - elapsed))
        elapsed += interval
        if check_stop_signal(signal_conn, worker_id, worker_type):
            return False
    return True


def get_db_connection():
    return psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD")
    )


def get_signal_connection():
    """
    Dedicated connection used only for stop-signal reads and clears.
    Kept completely separate from the main crawl connection so heavy
    write transactions on the main conn never block signal polling.
    Autocommit is on so there is never an open transaction to contend with.
    """
    conn = psycopg.connect(
        host=os.getenv("PG_HOST"),
        port=os.getenv("PG_PORT"),
        dbname=os.getenv("PG_DATABASE"),
        user=os.getenv("PG_USER"),
        password=os.getenv("PG_PASSWORD"),
        autocommit=True,
    )
    return conn


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


def get_skip_days(default: int = 7) -> int:
    raw = os.getenv("CRAWL_SKIP_DAYS", str(default)).strip()
    try:
        return max(int(raw), 0)
    except ValueError:
        return default


def get_discovery_worker_count(default: int = 1) -> int:
    raw = os.getenv("DISCOVERY_COUNT", str(default)).strip()
    try:
        return max(int(raw), 1)
    except ValueError:
        return default


# ---------------------------------------------------------------------------
# Hub tracking
# ---------------------------------------------------------------------------

def is_hub_fully_discovered(conn, start_url: str) -> bool:
    rediscover_days = int(os.getenv("HUB_REDISCOVER_DAYS", "10"))
    with conn.cursor() as cur:
        cur.execute("""
            SELECT 1 FROM discovered_hub
            WHERE url = %s
              AND completed_at IS NOT NULL
              AND completed_at >= now() - make_interval(days => %s)
        """, (start_url, rediscover_days))
        return cur.fetchone() is not None


def get_hub_resume_url(conn, start_url: str) -> str | None:
    with conn.cursor() as cur:
        cur.execute("""
            SELECT last_page_url FROM discovered_hub
            WHERE url = %s AND completed_at IS NULL
        """, (start_url,))
        row = cur.fetchone()
        return row[0] if row else None


def mark_hub_page_progress(conn, start_url: str, current_page_url: str, worker_id: int):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO discovered_hub (url, worker_id, last_page_url)
            VALUES (%s, %s, %s)
            ON CONFLICT (url) DO UPDATE SET
                last_page_url = EXCLUDED.last_page_url,
                worker_id = EXCLUDED.worker_id,
                completed_at = NULL
        """, (start_url, worker_id, current_page_url))


def mark_hub_completed(conn, start_url: str, worker_id: int):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO discovered_hub (url, worker_id, completed_at, last_page_url)
            VALUES (%s, %s, now(), NULL)
            ON CONFLICT (url) DO UPDATE SET
                completed_at = now(),
                last_page_url = NULL,
                worker_id = EXCLUDED.worker_id
        """, (start_url, worker_id))


# ---------------------------------------------------------------------------
# Queue helpers
# ---------------------------------------------------------------------------

def enqueue_thread_job(conn, container_id: int, url: str):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO crawl_queue (container_id, url, job_type)
            VALUES (%s, %s, 'forum_thread')
            ON CONFLICT DO NOTHING
        """, (container_id, url))


def claim_next_thread_job(conn):
    with conn.cursor() as cur:
        cur.execute("""
            WITH job AS (
                SELECT queue_id
                FROM crawl_queue
                WHERE job_type='forum_thread'
                  AND status IN ('pending','retry')
                ORDER BY priority ASC, queued_at ASC
                FOR UPDATE SKIP LOCKED
                LIMIT 1
            )
            UPDATE crawl_queue q
            SET status='claimed',
                claimed_at=now(),
                attempts = attempts + 1
            FROM job
            WHERE q.queue_id = job.queue_id
            RETURNING q.queue_id, q.url
        """)
        row = cur.fetchone()
    conn.commit()
    if not row:
        return None
    return {"queue_id": row[0], "url": row[1]}


def mark_queue_done(conn, queue_id):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE crawl_queue
            SET status='done', finished_at=now()
            WHERE queue_id=%s
        """, (queue_id,))
    conn.commit()


def mark_queue_failed(conn, queue_id, error):
    with conn.cursor() as cur:
        cur.execute("""
            UPDATE crawl_queue
            SET status='retry', last_error=%s
            WHERE queue_id=%s
        """, (error, queue_id))
    conn.commit()


# ---------------------------------------------------------------------------
# URL / parsing helpers
# ---------------------------------------------------------------------------

def parse_forum_count(text: str) -> int:
    if not text:
        return 0
    t = text.strip().lower().replace(",", "")
    m = re.match(r"(\d+(?:\.\d+)?)([km]?)", t)
    if not m:
        return 0
    number = float(m.group(1))
    suffix = m.group(2)
    if suffix == "k":
        number *= 1_000
    elif suffix == "m":
        number *= 1_000_000
    return int(number)


def make_absolute_url(href: str, page_url: str) -> str | None:
    if not href:
        return None
    return urljoin(page_url, href.strip())


def strip_page_suffix(url: str) -> str:
    url = re.sub(r"/page-\d+/?$", "/", url)
    if not url.endswith("/"):
        url += "/"
    return url


def is_regular_thread_url(url: str) -> bool:
    return bool(re.search(r"\.\d+/?$", url))


def is_sticky_thread_url(url: str) -> bool:
    return bool(re.search(r"/sticky-threads/\d+/?$", url))


def normalise_thread_url(href: str, page_url: str) -> str | None:
    url = make_absolute_url(href, page_url)
    if not url:
        return None
    url = strip_page_suffix(url)
    if is_regular_thread_url(url) or is_sticky_thread_url(url):
        return url
    return None


def extract_thread_id(url: str) -> str | None:
    if not url:
        return None
    m = re.search(r"\.(\d+)/?$", url)
    if m:
        return m.group(1)
    m = re.search(r"/sticky-threads/(\d+)/?$", url)
    if m:
        return m.group(1)
    return None


def parse_bhw_date(date_str: str | None) -> datetime | None:
    if not date_str:
        return None
    try:
        return datetime.fromisoformat(date_str)
    except ValueError:
        pass
    try:
        return datetime.strptime(date_str, "%b %d, %Y").replace(tzinfo=timezone.utc)
    except ValueError:
        return None


# ---------------------------------------------------------------------------
# Forum page extraction
# ---------------------------------------------------------------------------

def get_thread_href(row) -> str | None:
    a = row.select_one("div.structItem-title a[href]")
    return a.get("href").strip() if a else None


def get_reply_count(row) -> int:
    for pair in row.select("dl.pairs"):
        dt = pair.select_one("dt")
        dd = pair.select_one("dd")
        if not dt or not dd:
            continue
        if "repl" in dt.get_text(" ", strip=True).lower():
            return parse_forum_count(dd.get_text(" ", strip=True))
    return 0


def get_view_count(row) -> int:
    for pair in row.select("dl.pairs"):
        dt = pair.select_one("dt")
        dd = pair.select_one("dd")
        if not dt or not dd:
            continue
        if "view" in dt.get_text(" ", strip=True).lower():
            return parse_forum_count(dd.get_text(" ", strip=True))
    return 0


def build_thread_summary(row, page_url: str, is_pinned: bool) -> dict | None:
    href = get_thread_href(row)
    thread_url = normalise_thread_url(href, page_url)
    if not thread_url:
        return None
    return {
        "thread_url": thread_url,
        "reply_count": get_reply_count(row),
        "view_count": get_view_count(row),
        "is_pinned": is_pinned,
    }


def extract_threads_from_forum_page(html: str, page_url: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    results = []
    seen = set()

    sticky_rows = soup.select("div.structItemContainer-group.stickyThreadContainer")
    regular_rows = soup.select("div.structItem.structItem--thread")

    for row in sticky_rows:
        thread = build_thread_summary(row, page_url, is_pinned=True)
        if thread and thread["thread_url"] not in seen:
            seen.add(thread["thread_url"])
            results.append(thread)

    for row in regular_rows:
        thread = build_thread_summary(row, page_url, is_pinned=False)
        if thread and thread["thread_url"] not in seen:
            seen.add(thread["thread_url"])
            results.append(thread)

    return results


def get_next_forum_page_url(html: str, page_url: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    a = soup.select_one("a.pageNav-jump.pageNav-jump--next[href]")
    if not a:
        return None
    href = a.get("href", "").strip()
    if not href:
        return None
    return urljoin(page_url, href)


def get_next_page(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    a = soup.select_one(".pageNav-jump--next[href]")
    if a:
        href = a.get("href", "")
        return "https://www.blackhatworld.com" + href
    return None


# ---------------------------------------------------------------------------
# Thread / post extraction
# ---------------------------------------------------------------------------

def extract_thread_title(html: str) -> str | None:
    soup = BeautifulSoup(html, "html.parser")
    el = soup.select_one("h1.p-title-value")
    return el.get_text(strip=True) if el else None


def extract_thread_created_at(html: str) -> datetime | None:
    soup = BeautifulSoup(html, "html.parser")
    time_el = soup.select_one("time.u-dt[datetime]")
    if not time_el:
        return None
    raw = time_el.get("datetime", "").strip()
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        return None


def extract_thread_tags(html: str) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    tag_list = soup.select_one("span.js-tagList")
    if not tag_list:
        return []
    return [a.get_text(strip=True) for a in tag_list.select("a.tagItem") if a.get_text(strip=True)]


def extract_thread_page_count(html: str) -> int:
    soup = BeautifulSoup(html, "html.parser")
    inp = soup.select_one("input.js-pageJumpPage")
    if not inp:
        return 1
    try:
        return int(inp.get("max", 1))
    except (ValueError, TypeError):
        return 1


def extract_breadcrumbs(html: str) -> dict:
    soup = BeautifulSoup(html, "html.parser")
    items = soup.select("ul.p-breadcrumbs li[itemprop='itemListElement']")
    crumbs = []
    for item in items:
        name = item.select_one("span[itemprop='name']")
        if not name:
            continue
        text = name.get_text(strip=True)
        if text.lower() in ("home", "forums"):
            continue
        crumbs.append(text)
    return {
        "category": crumbs[0] if crumbs else None,
        "subforum": crumbs[-1] if crumbs else None,
    }


def extract_quotes_from_post(section) -> list[dict]:
    quotes = []
    for block in section.select("blockquote[data-quote]"):
        quoted_username = block.get("data-quote", "").strip() or None
        source = block.get("data-source", "").strip()
        m = re.search(r"post:\s*(\d+)", source)
        quoted_post_id = m.group(1) if m else None
        content_el = block.select_one(".bbCodeBlock-expandContent") or block.select_one(".bbCodeBlock-content")
        quoted_text = content_el.get_text(strip=True) if content_el else None
        quotes.append({
            "quoted_username": quoted_username,
            "quoted_post_id": quoted_post_id,
            "quoted_text": quoted_text,
        })
    return quotes


def extract_raw_post_text(section) -> str | None:
    post_contents = section.select_one(".message-body.js-selectToQuote")
    if post_contents:
        for quote in post_contents.select("blockquote"):
            quote.decompose()
        return post_contents.get_text(strip=True)
    return None


def extract_external_post_id(section) -> str | None:
    a = section.select_one('a[href*="post-"]')
    if not a:
        return None
    href = a.get("href", "")
    m = re.search(r'post-(\d+)', href)
    return m.group(1) if m else None


def extract_post_permalink(section) -> str | None:
    a = section.select_one('a[href*="post-"]')
    if not a:
        return None
    href = a.get("href", "").strip()
    if not href:
        return None
    if href.startswith("/"):
        return "https://www.blackhatworld.com" + href
    if href.startswith("http"):
        return href
    return None


def extract_username(section) -> str | None:
    a = section.select_one("h4.message-name a.username--wide")
    if not a:
        return None
    span = a.select_one("span")
    return span.get_text(strip=True) if span else a.get_text(strip=True)


def extract_user_profile_url(section) -> str | None:
    a = section.select_one("h4.message-name a.username--wide")
    if not a:
        return None
    href = a.get("href", "").strip()
    if not href:
        return None
    if href.startswith("/"):
        return "https://www.blackhatworld.com" + href
    return href


def extract_user_id(section) -> str | None:
    a = section.select_one("h4.message-name a[data-user-id]")
    if not a:
        return None
    return a.get("data-user-id", "").strip() or None


def extract_user_title(section) -> str | None:
    el = section.select_one("h5.userTitle.message-userTitle")
    return el.get_text(strip=True) if el else None


def extract_user_extras(section) -> dict:
    result = {"post_count": None, "reaction_score": None, "joined_at": None}
    for dl in section.select("div.message-userExtras dl.pairs"):
        svg = dl.select_one("dt i svg")
        dd = dl.select_one("dd")
        if not svg or not dd:
            continue
        title = (svg.get("data-original-title") or "").lower()
        value = dd.get_text(strip=True)
        if "messages" in title:
            result["post_count"] = parse_forum_count(value)
        elif "reaction" in title:
            result["reaction_score"] = parse_forum_count(value)
        elif "joined" in title:
            try:
                result["joined_at"] = datetime.strptime(value, "%b %d, %Y").date()
            except ValueError:
                pass
    return result


def extract_time_posted(section) -> str | None:
    time_el = section.select_one(".message-attribution-main time.u-dt[datetime]")
    if not time_el:
        return None
    return time_el.get("datetime", "").strip() or None


def extract_like_count(section) -> int:
    el = section.select_one(".reactionsBar-link")
    if not el:
        return 0
    text = el.get_text(" ", strip=True)
    if not text:
        return 0
    t = text.lower()
    if "no likes" in t:
        return 0
    m = re.search(r"\band\s+(\d+)\s+other[s]?\b", t)
    if m:
        others = int(m.group(1))
        before = re.split(r"\band\s+\d+\s+other[s]?\b", text, maxsplit=1)[0].strip()
        names = [p.strip() for p in before.split(",") if p.strip()]
        return len(names) + others
    if "," in text:
        return len([p.strip() for p in text.split(",") if p.strip()])
    return 1


def extract_external_links(section) -> dict:
    post_body = section.select_one(".message-body.js-selectToQuote")
    if not post_body:
        return {"domains": [], "urls": []}
    domains = set()
    urls = set()
    for a in post_body.select("a[href]"):
        href = a.get("href", "").strip()
        if not href or href.startswith("#"):
            continue
        m = re.match(r"https?://(?:www\.)?([^/]+)", href)
        if not m:
            continue
        domain = m.group(1).lower()
        if "blackhatworld.com" in domain:
            continue
        domains.add(domain)
        urls.add(href)
    return {"domains": sorted(domains), "urls": sorted(urls)}


def extract_post_data(section) -> dict:
    user_extras = extract_user_extras(section)
    external_links = extract_external_links(section)
    quotes = extract_quotes_from_post(section)       # BEFORE decompose
    post_content = extract_raw_post_text(section)    # decompose happens here
    return {
        "external_item_id": extract_external_post_id(section),
        "canonical_url": extract_post_permalink(section),
        "username": extract_username(section),
        "user_title": extract_user_title(section),
        "profile_url": extract_user_profile_url(section),
        "external_actor_id": extract_user_id(section),
        "post_count": user_extras["post_count"],
        "reaction_score": user_extras["reaction_score"],
        "joined_at": user_extras["joined_at"],
        "time_posted": extract_time_posted(section),
        "post_content": post_content,
        "like_count": extract_like_count(section),
        "quotes": quotes,
        "external_domains": external_links["domains"],
        "external_urls": external_links["urls"],
    }


def process_posts(html: str) -> list[dict]:
    soup = BeautifulSoup(html, "html.parser")
    sections = soup.select(".message-inner")
    posts = []
    for section in sections:
        post = extract_post_data(section)
        if not post["external_item_id"]:
            continue
        posts.append(post)
    return posts


# ---------------------------------------------------------------------------
# Marketplace filter
# ---------------------------------------------------------------------------

def is_marketplace_thread(subforum: str | None, category: str | None) -> bool:
    terms = ["marketplace", "buy sell trade", "services for sale", "bst"]
    for val in [subforum, category]:
        if val and any(t in val.lower() for t in terms):
            return True
    return False


def should_exclude_marketplace() -> bool:
    return os.getenv("EXCLUDE_MARKETPLACE", "false").strip().lower() == "true"


# ---------------------------------------------------------------------------
# DB writes
# ---------------------------------------------------------------------------

def upsert_thread_metadata(conn, thread: dict, forum_hub_url: str, thread_title: str | None = None, thread_created_at=None, thread_tags: list | None = None) -> int:
    thread_url = thread["thread_url"]
    thread_id = extract_thread_id(thread_url)
    if not thread_id:
        raise ValueError(f"Could not extract thread id from URL: {thread_url}")

    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO source (source_type, name, base_url)
            VALUES (%s, %s, %s)
            ON CONFLICT (source_type, name)
            DO UPDATE SET base_url = EXCLUDED.base_url
            RETURNING source_id
        """, ("forum", "BlackHatWorld", "https://www.blackhatworld.com"))
        source_id = cur.fetchone()[0]

        cur.execute("""
            INSERT INTO container (
                source_id, container_type, external_container_id, canonical_url,
                is_pinned, reply_count, view_count, discovered_from_forum_url,
                thread_title, thread_created_at, tags
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_id, external_container_id)
            DO UPDATE SET
                canonical_url = EXCLUDED.canonical_url,
                is_pinned = EXCLUDED.is_pinned,
                reply_count = COALESCE(EXCLUDED.reply_count, container.reply_count),
                view_count = COALESCE(EXCLUDED.view_count, container.view_count),
                discovered_from_forum_url = EXCLUDED.discovered_from_forum_url,
                thread_title = COALESCE(EXCLUDED.thread_title, container.thread_title),
                thread_created_at = COALESCE(EXCLUDED.thread_created_at, container.thread_created_at),
                tags = COALESCE(EXCLUDED.tags, container.tags)
            RETURNING container_id
        """, (
            source_id, "thread", thread_id, thread_url,
            thread["is_pinned"], thread["reply_count"], thread["view_count"],
            forum_hub_url, thread_title, thread_created_at, thread_tags or [],
        ))
        return cur.fetchone()[0]


def save_posts(conn, posts, thread_url: str, is_pinned: bool = False, reply_count: int | None = None, view_count: int | None = None, thread_title: str | None = None, thread_created_at=None, thread_tags: list | None = None, subforum: str | None = None, category: str | None = None, page_count: int | None = None, post_position_offset: int = 0):
    with conn.cursor() as cur:
        cur.execute("""
            INSERT INTO source (source_type, name, base_url)
            VALUES (%s,%s,%s)
            ON CONFLICT (source_type,name)
            DO UPDATE SET base_url=EXCLUDED.base_url
            RETURNING source_id
        """, ("forum", "BlackHatWorld", "https://www.blackhatworld.com"))
        source_id = cur.fetchone()[0]

        thread_id = extract_thread_id(thread_url)
        if not thread_id:
            raise ValueError(f"Could not extract thread id from URL: {thread_url}")

        cur.execute("""
            INSERT INTO container (
                source_id, container_type, external_container_id, canonical_url,
                is_pinned, reply_count, view_count, thread_title, thread_created_at,
                tags, subforum, category, page_count
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source_id, external_container_id)
            DO UPDATE SET
                canonical_url = EXCLUDED.canonical_url,
                is_pinned = EXCLUDED.is_pinned,
                reply_count = EXCLUDED.reply_count,
                view_count = EXCLUDED.view_count,
                thread_title = COALESCE(EXCLUDED.thread_title, container.thread_title),
                thread_created_at = COALESCE(EXCLUDED.thread_created_at, container.thread_created_at),
                tags = COALESCE(EXCLUDED.tags, container.tags),
                subforum = COALESCE(EXCLUDED.subforum, container.subforum),
                category = COALESCE(EXCLUDED.category, container.category),
                page_count = COALESCE(EXCLUDED.page_count, container.page_count)
            RETURNING container_id
        """, (source_id, "thread", thread_id, thread_url, is_pinned, reply_count, view_count, thread_title, thread_created_at, thread_tags or [], subforum, category, page_count))
        container_id = cur.fetchone()[0]

        published_dates = []

        for i, post in enumerate(posts):
            if not post.get("external_item_id"):
                continue

            username = (post.get("username") or "unknown").strip()
            published_at = parse_bhw_date(post.get("time_posted"))
            like_count = int(post.get("like_count", 0) or 0)
            content_text = (post.get("post_content") or "").strip()
            canonical_url = post.get("canonical_url")
            position_in_thread = post_position_offset + i + 1
            is_op = position_in_thread == 1
            external_domains = post.get("external_domains") or []
            external_urls = post.get("external_urls") or []

            if published_at:
                published_dates.append(published_at)

            cur.execute("""
                INSERT INTO actor (source_id, handle, user_title, post_count, reaction_score, joined_at, profile_url, external_actor_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source_id, handle)
                DO UPDATE SET
                    user_title = COALESCE(EXCLUDED.user_title, actor.user_title),
                    post_count = COALESCE(EXCLUDED.post_count, actor.post_count),
                    reaction_score = COALESCE(EXCLUDED.reaction_score, actor.reaction_score),
                    joined_at = COALESCE(EXCLUDED.joined_at, actor.joined_at),
                    profile_url = COALESCE(EXCLUDED.profile_url, actor.profile_url),
                    external_actor_id = COALESCE(EXCLUDED.external_actor_id, actor.external_actor_id)
                RETURNING actor_id
            """, (
                source_id, username, post.get("user_title"), post.get("post_count"),
                post.get("reaction_score"), post.get("joined_at"),
                post.get("profile_url"), post.get("external_actor_id"),
            ))
            actor_id = cur.fetchone()[0]

            cur.execute("""
                INSERT INTO item (
                    source_id, container_id, item_type, external_item_id, canonical_url,
                    actor_id, score, published_at, position_in_thread, is_op,
                    external_domains, external_urls
                )
                VALUES (%s,%s,'forum_post',%s,%s,%s,%s,%s,%s,%s,%s,%s)
                ON CONFLICT (source_id,external_item_id)
                DO UPDATE SET
                    canonical_url = EXCLUDED.canonical_url,
                    score = EXCLUDED.score,
                    published_at = EXCLUDED.published_at,
                    position_in_thread = EXCLUDED.position_in_thread,
                    is_op = EXCLUDED.is_op,
                    external_domains = EXCLUDED.external_domains,
                    external_urls = EXCLUDED.external_urls,
                    scraped_last_at = now()
                RETURNING item_id
            """, (
                source_id, container_id, str(post["external_item_id"]), canonical_url,
                actor_id, like_count, published_at, position_in_thread, is_op,
                external_domains, external_urls,
            ))
            item_id = cur.fetchone()[0]

            if content_text:
                text_hash = hash_text(content_text)
                cur.execute("""
                    INSERT INTO item_content (item_id, content_text, content_hash, is_current)
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

            for quote in post.get("quotes") or []:
                if not quote.get("quoted_post_id"):
                    continue
                cur.execute("""
                    INSERT INTO post_quote (source_item_id, quoted_post_id, quoted_username)
                    VALUES (%s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (item_id, quote["quoted_post_id"], quote.get("quoted_username")))

        if published_dates:
            last_reply_at = max(published_dates)
            cur.execute("""
                UPDATE container
                SET last_reply_at = GREATEST(last_reply_at, %s)
                WHERE container_id = %s
            """, (last_reply_at, container_id))

    print(f"Saved {len(posts)} posts to DB")


# ---------------------------------------------------------------------------
# Seeds loader
# ---------------------------------------------------------------------------

def load_forum_hubs() -> list[str]:
    path = os.getenv("FORUMS_FILE", "data/seeds/forums.txt")
    project_root = Path(__file__).resolve().parent.parent
    file_path = project_root / path
    if not file_path.exists():
        raise FileNotFoundError(f"Forums file not found: {file_path}")
    hubs = []
    with open(file_path, encoding="utf-8") as f:
        for line in f:
            u = line.strip()
            if not u or u.startswith("#"):
                continue
            hubs.append(u)
    return hubs


# ---------------------------------------------------------------------------
# Crawl logic
# ---------------------------------------------------------------------------

async def crawl_forum_pages(conn, signal_conn, start_url: str, session: WorkerSession) -> bool:
    """
    Returns True if work completed normally, False if a stop signal interrupted.
    Uses dedicated signal_conn for stop checks so main conn locks never interfere.
    """
    worker_id = session.worker_id

    if is_hub_fully_discovered(conn, start_url):
        print(f"Hub fully discovered recently, skipping: {start_url}")
        return True

    resume_url = get_hub_resume_url(conn, start_url)
    current_url = resume_url if resume_url else start_url
    if resume_url:
        print(f"Resuming hub from: {resume_url}")

    page_num = 1
    seen_pages = set()

    while current_url:
        # ── Stop signal check (inside the pagination loop) ──────────────────
        if check_stop_signal(signal_conn, worker_id, "discovery"):
            print(f"[Discovery {worker_id}] stop signal received mid-hub, stopping cleanly...")
            clear_stop_signal(signal_conn, worker_id, "discovery")
            update_worker_stats(conn, worker_id, "discovery", status="stopped")
            conn.commit()
            return False  # signal to the outer loop to exit
        # ────────────────────────────────────────────────────────────────────

        if current_url in seen_pages:
            print(f"Already visited page, stopping: {current_url}")
            break

        seen_pages.add(current_url)
        print(f"\n[Forum page {page_num}] {current_url}")

        if not await interruptible_sleep(signal_conn, worker_id, "discovery", session.get_delay()):
            print(f"[Discovery {worker_id}] stop signal received during delay, stopping cleanly...")
            clear_stop_signal(signal_conn, worker_id, "discovery")
            update_worker_stats(conn, worker_id, "discovery", status="stopped")
            conn.commit()
            return False

        html = await fetch_page_async(session.get_scraper(), current_url)
        if not html:
            session.on_error()
            raise Exception(f"Failed to fetch hub page: {current_url}")

        session.on_success()

        threads = extract_threads_from_forum_page(html, current_url)
        print(f"Found {len(threads)} threads on this page")

        for thread in threads:
            try:
                container_id = upsert_thread_metadata(conn, thread, start_url)
                enqueue_thread_job(conn, container_id, thread["thread_url"])
            except Exception as e:
                print("Error queueing thread:", thread["thread_url"], "|", e)

        mark_hub_page_progress(conn, start_url, current_url, worker_id)
        update_worker_stats(conn, worker_id, "discovery", pages_fetched=1, last_url=current_url)
        conn.commit()

        next_url = get_next_forum_page_url(html, current_url)
        if not next_url:
            print("No next page found. Hub complete.")
            mark_hub_completed(conn, start_url, worker_id)
            conn.commit()
            break

        current_url = next_url
        page_num += 1

    return True  # completed normally


async def crawl_thread(conn, signal_conn, thread_url: str, is_pinned: bool = False, reply_count: int | None = None, view_count: int | None = None, session: WorkerSession | None = None):
    skip_days = get_skip_days()
    if fetched_recently_conn(conn, thread_url, skip_days):
        print(f"Thread fetched in last {skip_days} day(s), skipping: {thread_url}")
        return

    current_page = thread_url
    is_first_page = True
    thread_title = None
    thread_created_at = None
    thread_tags = []
    subforum = None
    category = None
    page_count = None
    post_position_offset = 0

    while True:
        delay = session.get_delay() if session else random.uniform(2.0, 5.0)
        if session and not await interruptible_sleep(signal_conn, session.worker_id, "worker", delay):
            print(f"[Worker {session.worker_id}] stop signal received during delay, stopping cleanly...")
            clear_stop_signal(signal_conn, session.worker_id, "worker")
            update_worker_stats(conn, session.worker_id, "worker", status="stopped")
            raise Exception("stop_signal")
        elif not session:
            await asyncio.sleep(delay)

        scraper = session.get_scraper() if session else make_scraper()
        html = await fetch_page_async(scraper, current_page)

        if not html:
            if session:
                session.on_error()
            raise Exception(f"Failed to fetch thread page: {current_page}")

        if session:
            session.on_success()

        log_fetch_conn(conn, current_page, status="ok")

        if is_first_page:
            thread_title = extract_thread_title(html)
            thread_created_at = extract_thread_created_at(html)
            thread_tags = extract_thread_tags(html)
            breadcrumbs = extract_breadcrumbs(html)
            subforum = breadcrumbs["subforum"]
            category = breadcrumbs["category"]
            page_count = extract_thread_page_count(html)
            is_first_page = False

            if should_exclude_marketplace() and is_marketplace_thread(subforum, category):
                print(f"Skipping marketplace thread: {thread_url}")
                return

        posts = process_posts(html)
        save_posts(
            conn, posts, thread_url,
            is_pinned=is_pinned,
            reply_count=reply_count,
            view_count=view_count,
            thread_title=thread_title,
            thread_created_at=thread_created_at,
            thread_tags=thread_tags,
            subforum=subforum,
            category=category,
            page_count=page_count,
            post_position_offset=post_position_offset,
        )

        post_position_offset += len(posts)
        conn.commit()
        print(f"Fetched {current_page} -> {len(posts)} posts")

        next_page = get_next_page(html)
        if not next_page:
            break
        current_page = next_page


# ---------------------------------------------------------------------------
# Runner entrypoints
# ---------------------------------------------------------------------------

async def run_forum_discovery(worker_id: int = 0):
    forum_hubs = load_forum_hubs()
    if not forum_hubs:
        raise ValueError("No forum hubs found. Check FORUMS_FILE / forums.txt")

    my_hubs = [h for i, h in enumerate(forum_hubs) if i % get_discovery_worker_count() == worker_id]
    if not my_hubs:
        print(f"[Discovery {worker_id}] no hubs assigned, exiting")
        return

    session = WorkerSession(worker_id)
    proxy = session.get_proxy()
    if proxy:
        print(f"[Discovery {worker_id}] using proxy {proxy['host']}:{proxy['port']}")
    else:
        print(f"[Discovery {worker_id}] no proxy")

    conn = get_db_connection()
    signal_conn = get_signal_connection()
    await asyncio.sleep(worker_id * 2)

    # ── Clean up stale state from any previous session ───────────────────────
    cleanup_stale_workers(conn, worker_id, "discovery")
    # ─────────────────────────────────────────────────────────────────────────

    print(f"[Discovery {worker_id}] starting, assigned {len(my_hubs)} hubs...")

    try:
        for hub in my_hubs:
            retries = 0
            max_retries = 3
            while retries < max_retries:
                print(f"\n[Discovery {worker_id}] === HUB: {hub} (attempt {retries + 1})")
                try:
                    completed = await crawl_forum_pages(conn, signal_conn, hub, session=session)
                    if not completed:
                        # Stop signal was handled inside crawl_forum_pages
                        return
                    break
                except Exception as e:
                    retries += 1
                    update_worker_stats(conn, worker_id, "discovery", errors=1, last_url=hub)
                    print(f"[Discovery {worker_id}] error: {e}")
                    if retries >= max_retries:
                        print(f"[Discovery {worker_id}] giving up on {hub}")

            # Between-hub stop signal check via dedicated signal conn
            if check_stop_signal(signal_conn, worker_id, "discovery"):
                print(f"[Discovery {worker_id}] stop signal received, shutting down cleanly...")
                clear_stop_signal(signal_conn, worker_id, "discovery")
                update_worker_stats(conn, worker_id, "discovery", status="stopped")
                break

        # Worker finished all hubs naturally — mark stopped so it disappears
        update_worker_stats(conn, worker_id, "discovery", status="stopped")

    finally:
        try:
            conn.close()
        except Exception:
            pass
        try:
            signal_conn.close()
        except Exception:
            pass


async def run_thread_worker(worker_id: int = 0):
    session = WorkerSession(worker_id)
    proxy = session.get_proxy()
    if proxy:
        print(f"[Worker {worker_id}] using proxy {proxy['host']}:{proxy['port']}")
    else:
        print(f"[Worker {worker_id}] no proxy")

    conn = get_db_connection()
    signal_conn = get_signal_connection()
    await asyncio.sleep(worker_id * 2)

    # ── Clean up stale state from any previous session ───────────────────────
    cleanup_stale_workers(conn, worker_id, "worker")
    # ─────────────────────────────────────────────────────────────────────────

    print(f"[Worker {worker_id}] starting...")

    try:
        while True:
            job = claim_next_thread_job(conn)
            if not job:
                poll_interval = int(os.getenv("WORKER_POLL_INTERVAL", "60"))
                print(f"[Worker {worker_id}] no jobs, waiting {poll_interval}s...")
                if not await interruptible_sleep(signal_conn, worker_id, "worker", poll_interval):
                    print(f"[Worker {worker_id}] stop signal received while idle, shutting down...")
                    clear_stop_signal(signal_conn, worker_id, "worker")
                    update_worker_stats(conn, worker_id, "worker", status="stopped")
                    break
                continue

            print(f"[Worker {worker_id}] crawling: {job['url']}")

            try:
                await crawl_thread(conn, signal_conn, job["url"], session=session)
                mark_queue_done(conn, job["queue_id"])
                update_worker_stats(conn, worker_id, "worker", pages_fetched=1, last_url=job["url"])
            except Exception as e:
                if str(e) == "stop_signal":
                    mark_queue_failed(conn, job["queue_id"], "stopped by signal")
                    break
                session.on_error()
                mark_queue_failed(conn, job["queue_id"], str(e))
                update_worker_stats(conn, worker_id, "worker", errors=1, last_url=job["url"])
                print(f"[Worker {worker_id}] error: {e}")

            if check_stop_signal(signal_conn, worker_id, "worker"):
                print(f"[Worker {worker_id}] stop signal received, shutting down cleanly...")
                clear_stop_signal(signal_conn, worker_id, "worker")
                update_worker_stats(conn, worker_id, "worker", status="stopped")
                break
    finally:
        conn.close()
        try:
            signal_conn.close()
        except Exception:
            pass


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    load_dotenv(override=False)

    mode = sys.argv[1] if len(sys.argv) > 1 else "worker"
    worker_id = int(sys.argv[2]) if len(sys.argv) > 2 else 0
    disc_count = sys.argv[3] if len(sys.argv) > 3 else "1"

    os.environ["DISCOVERY_COUNT"] = disc_count

    print(f"MODE={mode} WORKER_ID={worker_id} DISCOVERY_COUNT={disc_count}")

    if mode == "discovery":
        asyncio.run(run_forum_discovery(worker_id=worker_id))
    else:
        asyncio.run(run_thread_worker(worker_id=worker_id))