import asyncio
import re
import sqlite3
import logging
from urllib.parse import quote_plus, urljoin, urlparse

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

# ---- Email quality filters ----
PLACEHOLDER_DOMAINS = {
    "example.com", "example.org", "example.net",
    "domain.com", "domain.org", "domain.net",
    "yourdomain.com", "your-company.com", "company.com",
    "email.com", "test.com", "localhost",
}
PLACEHOLDER_LOCALPARTS = {
    "example", "test", "testing", "name", "yourname", "youremail", "email",
    "user", "username",
}
BAD_INBOX_PREFIXES = ("noreply", "no-reply", "donotreply", "do-not-reply", "mailer-daemon")
BAD_DOMAINS_SUBSTRINGS = (
    "sentry",         # wix/next sentry noise: <hash>@sentry.wixpress.com
    "wixpress",       # same family
)
GOOD_INBOX_LOCALPARTS = (
    "info", "contact", "hello", "enquiries", "inquiries", "sales", "support",
    "bookings", "office", "accounts", "service", "orders", "shop",
)


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
    html = await tab.page_source
    if not looks_like_consent_page(html):
        return False

    log.warning("Consent page detected. Trying to click 'Accept all'...")

    accept_btn = await tab.find(
        xpath='//button[@aria-label="Accept all"]',
        timeout=6,
        raise_exc=False,
    )

    if not accept_btn:
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
    await asyncio.sleep(1.2)
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
    link_element = container.select_one(".yYlJEf.Q7PwXb.L48Cpd.brKmxb")
    if link_element and link_element.get("href"):
        return link_element.get("href").strip()
    return "No Website Listed"


# ---------------- MAPS WEBSITE EXTRACTION ----------------
def extract_website_from_maps_html(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")

    el = soup.select_one(".rogA2c.ITvuef")
    if el and el.get("href"):
        return el.get("href").strip()

    a = soup.select_one('a[aria-label*="Website"], a[aria-label*="website"]')
    if a and a.get("href"):
        return a.get("href").strip()

    for a in soup.select("a[href]"):
        href = (a.get("href") or "").strip()
        if href.startswith("http") and "google." not in href and "g.co" not in href:
            return href

    return "No Website Listed"


async def get_website_from_maps_cid(tab, cid: str, consent_state: dict, dump_limit_state: dict) -> str:
    url = f"https://www.google.com/maps?cid={cid}&hl=en-GB&gl=GB"
    html = await fetch_html_browser(tab, url, wait_seconds=1.2)

    if looks_like_consent_page(html):
        if not consent_state["accepted"]:
            ok = await accept_google_consent_if_present(tab)
            consent_state["accepted"] = ok
            html = await fetch_html_browser(tab, url, wait_seconds=1.2)
        else:
            log.warning("Consent still present even after acceptance attempt.")
            if dump_limit_state["count"] < dump_limit_state["max"]:
                dump_limit_state["count"] += 1
                dump_debug_html(f"debug_maps_consent_{cid}_{dump_limit_state['count']}.html", html)
            return "No Website Listed"

    await asyncio.sleep(0.8)
    html2 = await tab.page_source
    if len(html2) > len(html):
        html = html2

    website = extract_website_from_maps_html(html)
    if website == "No Website Listed" and dump_limit_state["count"] < dump_limit_state["max"]:
        dump_limit_state["count"] += 1
        dump_debug_html(f"debug_maps_no_site_{cid}_{dump_limit_state['count']}.html", html)

    return website


# ---------------- EMAIL SCRAPE (IMPROVED) ----------------
async def fetch_html_fast(session: aiohttp.ClientSession, url: str) -> str:
    try:
        timeout = aiohttp.ClientTimeout(total=10)
        async with session.get(url, timeout=timeout, allow_redirects=True) as resp:
            if resp.status != 200:
                return ""
            return await resp.text(errors="ignore")
    except Exception:
        return ""


def _normalize_email(e: str) -> str:
    return (e or "").strip().strip(").,;:\"'<>[]{}").lower()


def _domain_from_url(url: str) -> str:
    try:
        host = urlparse(url).netloc.lower()
        if host.startswith("www."):
            host = host[4:]
        return host
    except Exception:
        return ""


def _is_plausible_email(email: str) -> bool:
    email = _normalize_email(email)
    if not email or "@" not in email:
        return False

    local, domain = email.split("@", 1)
    if not local or not domain:
        return False

    if domain in PLACEHOLDER_DOMAINS:
        return False
    if local in PLACEHOLDER_LOCALPARTS:
        return False
    if "." not in domain:
        return False

    if local.startswith(BAD_INBOX_PREFIXES):
        return False

    # kill sentry/wix/telemetry junk
    if any(bad in domain for bad in BAD_DOMAINS_SUBSTRINGS):
        return False

    # kill image-file "emails" like gc-hero-mob@2x.png / Logo_350x@2x.png
    if domain.endswith((".png", ".jpg", ".jpeg", ".gif", ".webp", ".svg")):
        return False

    # also kill ones where the "domain" is obviously a file suffix like "2x.png"
    if domain.count(".") == 1 and domain.split(".")[-1] in {"png", "jpg", "jpeg", "gif", "webp", "svg"}:
        return False

    return True


def _strip_scripts_styles(soup: BeautifulSoup) -> None:
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()


def _extract_emails_from_html(html: str) -> set[str]:
    soup = BeautifulSoup(html or "", "html.parser")
    found = set()

    # Best signal: mailto links
    for a in soup.select('a[href^="mailto:"]'):
        href = a.get("href", "")
        mail = href.split("mailto:", 1)[-1].split("?", 1)[0]
        mail = _normalize_email(mail)
        if mail:
            found.add(mail)

    # Visible text (after stripping scripts/styles)
    _strip_scripts_styles(soup)
    visible_text = soup.get_text(" ", strip=True)
    for m in EMAIL_RE.findall(visible_text):
        found.add(_normalize_email(m))

    # Fallback: raw html
    for m in EMAIL_RE.findall(html or ""):
        found.add(_normalize_email(m))

    return found


def _score_email(email: str, website_domain: str) -> int:
    email = _normalize_email(email)
    local, domain = email.split("@", 1)
    score = 0

    # same-domain is a huge signal
    if website_domain and domain.endswith(website_domain):
        score += 50

    # common business inboxes
    if local in GOOD_INBOX_LOCALPARTS:
        score += 20
    else:
        for p in GOOD_INBOX_LOCALPARTS:
            if local.startswith(p + ".") or local.startswith(p + "-"):
                score += 10
                break

    # penalize generic telemetry-like patterns
    if any(bad in domain for bad in BAD_DOMAINS_SUBSTRINGS):
        score -= 100

    # mild penalty for free providers (still legit sometimes)
    if domain in {"gmail.com", "outlook.com", "hotmail.com", "yahoo.com", "icloud.com"}:
        score -= 5

    return score


async def get_email_from_website(session: aiohttp.ClientSession, website: str) -> str:
    if not website or website == "No Website Listed":
        return "No email found"

    if not website.startswith("http"):
        website = "https://" + website

    website_domain = _domain_from_url(website)

    paths = ["", "/contact", "/contact-us", "/about", "/about-us"]
    candidates: set[str] = set()

    for p in paths:
        url = website if p == "" else urljoin(website, p)
        html = await fetch_html_fast(session, url)
        if not html:
            continue

        candidates |= _extract_emails_from_html(html)

        # Early stop if we already have a same-domain plausible email
        if any(_is_plausible_email(e) and website_domain and e.split("@", 1)[1].endswith(website_domain) for e in candidates):
            break

    valid = [e for e in candidates if _is_plausible_email(e)]
    if not valid:
        return "No email found"

    valid.sort(key=lambda e: _score_email(e, website_domain), reverse=True)
    return valid[0]


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

    consent_state = {"accepted": False}
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