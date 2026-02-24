import asyncio
from pydoll.browser.chromium import Chrome
from bs4 import BeautifulSoup


async def main():
    async with Chrome() as browser:
        tab = await browser.start()
        await tab.go_to('https://www.google.com/search?q=plumbers+in+twickenham&sca_esv=846ebd86ebd88482&udm=1&sxsrf=ANbL-n7otNKTRzf0IKxBg1D-B_eNHzkPxQ:1771953677533&ei=Dd6daaCTIMq0hbIP9JrLEQ&start=0&sa=N&sstk=Af77f_f68uKRPPopKhcJ-Cx7MStx8lCrmdaccUEoaxPF2sYSNBn7mTPp2HJUpuPnswbQaFpWKIM2H2ImDbYQfynSbyatyA2Et3kbS4N-JRReGhdz4Q3ekmJDbqt8QqAHGPEhkVWN9O9nNhoAP44joXcN276bx_WNkSOESSEcQJHAKnzloWjTMGbNEowohY8_Tf8d7j18clp3y94MXXNeueUKJU-18gZNe7Ef9Gglv7Y96FlWW6cup_lv__Rex86-9hD1-t8l7ty-I2ur95TCfgNmQEB_mJ7WWiT1FfcKCspEV6EPsloDrKu6frWVvltnoprZzNufbzyPO56nfa5QBHlCwN-gI09L8WCRJOqNh20XSK5njN7x8zVa_wNgVZ7ZhvTH6y4frKfVryhdwq3d9D9N&ved=2ahUKEwjgzYfs0fKSAxVKWkEAHXTNMgI4eBDx0wN6BAgPEAI&biw=1920&bih=919&dpr=1')

        html_snapshot = await tab.page_source
        await asyncio.sleep(3)

        soup = BeautifulSoup(html_snapshot, "html.parser")
        cards = soup.select(".VkpGBb")
        with open("test.html", "w", encoding="utf-8") as f:
            f.write(str(cards))


asyncio.run(main())
