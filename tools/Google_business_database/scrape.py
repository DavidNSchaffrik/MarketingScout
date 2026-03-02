import asyncio
import re
import sqlite3
from urllib.parse import quote_plus

from pydoll.browser.chromium import Chrome
from bs4 import BeautifulSoup

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
    re.VERBOSE
)

YEARS_RE = re.compile(r"(\d+)\+?\s+years in business", re.IGNORECASE)

NO_MORE_PLACES_TEXT = "It looks like there aren't any 'Places' matches on this topic"


async def fetch_html(tab, url: str, wait_seconds: float = 3.0) -> str:
    await tab.go_to(url)
    await asyncio.sleep(wait_seconds)
    return await tab.page_source


def parse_cards(html: str):
    soup = BeautifulSoup(html, "html.parser")
    return soup.select(".VkpGBb")


def extract_phones(text: str) -> list[str]:
    return [m.strip() for m in PHONE_RE.findall(text)]


def extract_name(card):
    name_elem = card.select_one(".OSrXXb")
    return name_elem.get_text(strip=True) if name_elem else "Unknown Name"


def extract_text_regex(card):
    return card.get_text(" ", strip=True)


def extract_website_url(card):
    link_element = card.select_one(".yYlJEf.Q7PwXb.L48Cpd.brKmxb")
    if link_element and link_element.get("href"):
        return link_element.get("href").strip()
    return "No Website Listed"


def extract_years_in_business(text):
    years_match = YEARS_RE.search(text)
    return int(years_match.group(1)) if years_match else None


def init_db(db_path="leads.sqlite3"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    cur.execute("""
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
    """)

    conn.commit()
    conn.close()

def save_results(results, db_path="leads.sqlite3"):
    conn = sqlite3.connect(db_path)
    cur = conn.cursor()

    rows = []
    for r in results:
        rows.append((
            r["Name"],
            r["Website"],
            ", ".join(r["Phones"]) if r["Phones"] else "",
            r["Years_in_Business"],
        ))

    cur.executemany("""
        INSERT OR IGNORE INTO leads (name, website, phones, years_in_business)
        VALUES (?, ?, ?, ?)
    """, rows)

    conn.commit()
    conn.close()


def build_url(query: str, start: int) -> str:
    # keep your udm=1 and just vary start
    return f"https://www.google.com/search?q={quote_plus(query)}&udm=1&start={start}"


async def main():
    query = "plumbers in twickenham"
    db_path = "leads.sqlite3"

    init_db(db_path)

    start = 0
    step = 20

    # hard safety cap so you don't accidentally run forever if the stop text changes
    max_pages = 50

    total_saved_this_run = 0

    async with Chrome() as browser:
        tab = await browser.start()

        for page_idx in range(max_pages):
            url = build_url(query, start)
            html = await fetch_html(tab, url, wait_seconds=3)

            # stop condition (your requirement)
            if NO_MORE_PLACES_TEXT.lower() in html.lower():
                print(f"Stop: no more Places matches at start={start}")
                break

            cards = parse_cards(html)

            # if Google served a weird page / blocked / layout changed, cards may be empty
            # don't silently spin forever
            if not cards:
                print(f"Stop: found 0 cards at start={start}. Page layout/blocking likely.")
                break

            results = []
            for card in cards:
                text = extract_text_regex(card)
                results.append({
                    "Name": extract_name(card),
                    "Phones": extract_phones(text),
                    "Website": extract_website_url(card),
                    "Years_in_Business": extract_years_in_business(text),
                })

            save_results(results, db_path)
            total_saved_this_run += len(results)

            print(f"Page {page_idx+1}: scraped {len(results)} cards (start={start})")

            start += step

            # optional: throttle a bit to reduce bans / weirdness
            await asyncio.sleep(1)

    print(f"Done. Scraped/saved {total_saved_this_run} rows this run (duplicates ignored).")


if __name__ == "__main__":
    asyncio.run(main())