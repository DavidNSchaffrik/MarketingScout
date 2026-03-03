import asyncio
import re
import sqlite3
import logging
from urllib.parse import quote_plus, urljoin

import aiohttp
from bs4 import BeautifulSoup
from pydoll.browser.chromium import Chrome

# ---------------- LOGGING ----------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
log = logging.getLogger("leads")

EMAIL_RE = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
PHONE_RE = re.compile(
    r"""
    (?<!\d)
    (?:\+?\d{1,3}[\s.-]?)?
    (?:\(?\d{2,4}\)?[\s.-]?){1,3}
    \d{3,4}
    [\s.-]?
    \d{3,4}
    (?!\d)
    """,
    re.VERBOSE,
)
YEARS_RE = re.compile(r"(\d+)\+?\s+years in business", re.IGNORECASE)

NO_MORE_PLACES_TEXT = "It looks like there aren't any 'Places' matches on this topic"


# ---------------- UTILS ----------------
def looks_like_consent_page(html: str) -> bool:
    h = (html or "").lower()
    return ("consent.google.com" in h) or ("before you continue" in h)


def dump_debug_html(filename: str, html: str):
    try:
        with open(filename, "w", encoding="utf-8", errors="ignore") as f:
            f.write(html or "")
        log.info(f"Dumped debug HTML -> {filename}")
    except Exception as e:
        log.warning(f"Could not write debug HTML {filename}: {e}")


async def fetch_html_browser(tab, url: str, wait_seconds: float = 1.2) -> str:
    await tab.go_to(url)
    await asyncio.sleep(wait_seconds)
    return await tab.page_source


# ---------------- CONSENT CLICK (PYDOLL) ----------------
async def accept_google_consent_if_present(tab) -> bool:
    """
    Uses Pydoll's tab.find(...) per docs. :contentReference[oaicite:2]{index=2}
    Clicks the exact button you showed: aria-label="Accept all"
    """
    html = await tab.page_source
    if not looks_like_consent_page(html):
        return False

    log.warning("Consent page detected. Trying to click 'Accept all'...")

    # Prefer the stable aria-label. Your HTML confirms it.
    # XPath is the least ambiguous / least dependent on CSS class soup.
    accept_btn = await tab.find(
        xpath='//button[@aria-label="Accept all"]',
        timeout=6,
        raise_exc=False,
    )

    if not accept_btn:
        # Backup: jsname you posted
        accept_btn = await tab.find(
            xpath='//button[@jsname="b3VHJd"]',
            timeout=4,
            raise_exc=False,
        )

    if not accept_btn:
        dump_debug_html("debug_consent_page.html", html)
        log.warning("Could not find Accept button. Dumped debug_consent_page.html")
        return False

    await accept_btn.click()
    await asyncio.sleep(1.5)
    log.info("Clicked 'Accept all'.")
    return True


# ---------------- SEARCH PAGE PARSING ----------------
def parse_result_containers(html: str):
    soup = BeautifulSoup(html, "html.parser")
    return soup.select("div.cXedhc")


def extract_cid_from_container(container) -> str | None:
    a = container.select_one("a.rllt__link[data-cid]")
    if not a:
        return None
    cid = a.get("data-cid")
    return cid.strip() if cid else None


def extract_name_from_container(container) -> str:
    name_elem = container.select_one(".OSrXXb")
    return name_elem.get_text(strip=True) if name_elem else "Unknown Name"


def extract_text_from_container(container) -> str:
    return container.get_text(" ", strip=True)


def extract_phones(text: str) -> list[str]:
    return [m.strip() for m in PHONE_RE.findall(text)]


def extract_years_in_business(text: str):
    m = YEARS_RE.search(text)
    return int(m.group(1)) if m else None


def extract_website_from_container(container) -> str:
    # Your old selector kept (usually absent, but harmless)
    link_element = container.select_one(".yYlJEf.Q7PwXb.L48Cpd.brKmxb")
    if link_element and link_element.get("href"):
        return link_element.get("href").strip()
    return "No Website Listed"


# ---------------- MAPS WEBSITE EXTRACTION ----------------
def extract_website_from_maps_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    # Primary selector you asked for
    el = soup.select_one(".rogA2c.ITvuef")
    if el and el.get("href"):
        return el.get("href").strip()

    # fallback: aria label Website
    a = soup.select_one('a[aria-label*="Website"], a[aria-label*="website"]')
    if a and a.get("href"):
        return a.get("href").strip()

    # fallback: first external link (avoid google)
    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if href.startswith("http") and "google." not in href and "g.co" not in href:
            return href

    return "No Website Listed"


async def get_website_from_maps_cid(tab, cid: str, consent_state: dict, dump_limit_state: dict) -> str:
    url = f"https://www.google.com/maps?cid={cid}&hl=en-GB&gl=GB"

    html = await fetch_html_browser(tab, url, wait_seconds=1.2)

    # If consent shows up, accept ONCE (should set cookies), then retry this URL
    if looks_like_consent_page(html):
        if not consent_state["accepted"]:
            ok = await accept_google_consent_if_present(tab)
            consent_state["accepted"] = ok
            # re-open after accepting
            html = await fetch_html_browser(tab, url, wait_seconds=1.2)
        else:
            # already tried accepting; don't loop forever
            log.warning("Consent still present even after acceptance attempt.")
            if dump_limit_state["count"] < dump_limit_state["max"]:
                dump_limit_state["count"] += 1
                dump_debug_html(f"debug_maps_consent_{cid}_{dump_limit_state['count']}.html", html)
            return "No Website Listed"

    # Give Maps a short hydration beat then re-read once
    await asyncio.sleep(0.8)
    html2 = await tab.page_source
    if len(html2) > len(html):
        html = html2

    website = extract_website_from_maps_html(html)
    if website == "No Website Listed" and dump_limit_state["count"] < dump_limit_state["max"]:
        dump_limit_state["count"] += 1
        dump_debug_html(f"debug_maps_no_site_{cid}_{dump_limit_state['count']}.html", html)

    return website


# ---------------- EMAIL SCRAPE ----------------
async def fetch_html_fast(session: aiohttp.ClientSession, url: str) -> str:
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
            if resp.status != 200:
                return ""
            return await resp.text(errors="ignore")
    except Exception:
        return ""


async def get_email_from_website(session: aiohttp.ClientSession, website: str) -> str:
    if not website or website == "No Website Listed":
        return "No email found"

    if not website.startswith("http"):
        website = "https://" + website

    homepage = await fetch_html_fast(session, website)
    emails = set(EMAIL_RE.findall(homepage))
    if emails:
        return sorted(emails)[0]

    contact_url = urljoin(website, "/contact")
    contact_page = await fetch_html_fast(session, contact_url)
    emails = set(EMAIL_RE.findall(contact_page))
    if emails:
        return sorted(emails)[0]

    return "No email found"


# ---------------- DB ----------------
def init_db(db_path="leads.sqlite3"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS leads (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            website TEXT NOT NULL,
            phones TEXT,
            years_in_business INTEGER,
            email TEXT,
            created_at TEXT DEFAULT (datetime('now')),
            UNIQUE(name, website)
        )
        """
    )
    conn.commit()
    conn.close()


def save_results(results, db_path="leads.sqlite3"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    rows = []
    for r in results:
        rows.append(
            (
                r["Name"],
                r["Website"],
                ", ".join(r["Phones"]) if r["Phones"] else "",
                r["Years_in_Business"],
                r["Email"],
            )
        )

    cur.executemany(
        """
        INSERT OR IGNORE INTO leads (name, website, phones, years_in_business, email)
        VALUES (?, ?, ?, ?, ?)
        """,
        rows,
    )

    conn.commit()
    conn.close()


# ---------------- MAIN ----------------
def build_url(query: str, start: int) -> str:
    return f"https://www.google.com/search?q={quote_plus(query)}&udm=1&start={start}"


async def main():
    query = input("Enter query (e.g. 'plumbers in twickenham'): ").strip()
    if not query:
        print("Query cannot be empty.")
        return

    init_db("leads.sqlite3")

    start = 0
    step = 20
    max_pages = 50

    # consent accepted once per run
    consent_state = {"accepted": False}

    # limit debug dumps
    dump_limit_state = {"count": 0, "max": 8}

    async with Chrome() as browser:
        tab = await browser.start()

        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/122.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-GB,en;q=0.9",
        }

        async with aiohttp.ClientSession(headers=headers) as session:
            total_saved = 0

            for page_idx in range(max_pages):
                search_url = build_url(query, start)
                html = await fetch_html_browser(tab, search_url, wait_seconds=1.2)

                if NO_MORE_PLACES_TEXT.lower() in html.lower():
                    log.info(f"Stop: no more Places matches at start={start}")
                    break

                containers = parse_result_containers(html)
                log.info(f"Page {page_idx+1}: found {len(containers)} results (start={start})")

                if not containers:
                    dump_debug_html(f"debug_search_{start}.html", html)
                    log.warning("Stop: 0 results; layout/blocking likely. Dumped debug_search.")
                    break

                results = []
                for idx, container in enumerate(containers, start=1):
                    name = extract_name_from_container(container)
                    text = extract_text_from_container(container)
                    cid = extract_cid_from_container(container)

                    website = extract_website_from_container(container)

                    log.info(f"[{page_idx+1}.{idx}] {name!r} cid={cid!r} website_in_card={website!r}")

                    if (not website or website == "No Website Listed") and cid:
                        website = await get_website_from_maps_cid(
                            tab=tab,
                            cid=cid,
                            consent_state=consent_state,
                            dump_limit_state=dump_limit_state,
                        )

                    results.append(
                        {
                            "Name": name,
                            "Phones": extract_phones(text),
                            "Website": website,
                            "Years_in_Business": extract_years_in_business(text),
                            "Email": None,
                        }
                    )

                for r in results:
                    r["Email"] = await get_email_from_website(session, r["Website"])

                save_results(results, "leads.sqlite3")
                total_saved += len(results)
                log.info(f"Saved {len(results)} rows (total this run: {total_saved})")

                start += step
                await asyncio.sleep(0.5)

    log.info("Done.")


if __name__ == "__main__":
    asyncio.run(main())