import asyncio
from bs4 import BeautifulSoup
from pydoll.browser.chromium import Chrome
import re

URL = "https://www.blackhatworld.com/seo/guide-future-proof-backlink-strategies-build-once-benefit-forever-2025-edition.1478334/"


def parse_html_for_class(html, selector):
    soup = BeautifulSoup(html, "html.parser")
    return soup.select(f".{selector}")


def extract_like_count(section):
    like_message = section.select_one(".reactionsBar-link")
    if like_message:
        return like_message.get_text(strip=True)
    return "No Likes"


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

    if post["external_item_id"]:
        print(post["external_item_id"])


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
    # later: insert into DB here
    print(f"Saving {len(posts)} posts (stub)")


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