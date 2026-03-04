import asyncio
from bs4 import BeautifulSoup
from pydoll.browser.chromium import Chrome

URL = "https://www.blackhatworld.com/seo/guide-future-proof-backlink-strategies-build-once-benefit-forever-2025-edition.1478334/"

async def main():

    async with Chrome(headless=True) as browser:
        tab = await browser.new_tab()

        await tab.go_to(URL)
        await asyncio.sleep(2)

        html = await tab.page_source

    soup = BeautifulSoup(html, "html.parser")

    containers = soup.select(".message-inner")

    print(len(containers))

    for c in containers:
        print(c.get_text(strip=True))


asyncio.run(main())