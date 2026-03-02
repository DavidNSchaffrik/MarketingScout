import asyncio
import re
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

async def fetch_html(url: str, wait_seconds: float = 3.0) -> str:
    async with Chrome() as browser:
        tab = await browser.start()
        # Load the Google search URL
        # url = 'https://www.google.com/search?q=plumbers+in+twickenham&sca_esv=846ebd86ebd88482&udm=1&sxsrf=ANbL-n7otNKTRzf0IKxBg1D-B_eNHzkPxQ:1771953677533&ei=Dd6daaCTIMq0hbIP9JrLEQ&start=0&sa=N&sstk=Af77f_f68uKRPPopKhcJ-Cx7MStx8lCrmdaccUEoaxPF2sYSNBn7mTPp2HJUpuPnswbQaFpWKIM2H2ImDbYQfynSbyatyA2Et3kbS4N-JRReGhdz4Q3ekmJDbqt8QqAHGPEhkVWN9O9nNhoAP44joXcN276bx_WNkSOESSEcQJHAKnzloWjTMGbNEowohY8_Tf8d7j18clp3y94MXXNeueUKJU-18gZNe7Ef9Gglv7Y96FlWW6cup_lv__Rex86-9hD1-t8l7ty-I2ur95TCfgNmQEB_mJ7WWiT1FfcKCspEV6EPsloDrKu6frWVvltnoprZzNufbzyPO56nfa5QBHlCwN-gI09L8WCRJOqNh20XSK5njN7x8zVa_wNgVZ7ZhvTH6y4frKfVryhdwq3d9D9N&ved=2ahUKEwjgzYfs0fKSAxVKWkEAHXTNMgI4eBDx0wN6BAgPEAI&biw=1920&bih=919&dpr=1'
        await tab.go_to(url)
        # Wait BEFORE taking the snapshot to ensure JS finishes loading
        await asyncio.sleep(wait_seconds)
        return await tab.page_source


def parse_cards(html: str):
    soup = BeautifulSoup(html, "html.parser")
    return soup.select(".VkpGBb")

def strip_text(element, default: str = "") -> str:
    return element.get_text(strip=True) if element else default
 

def extract_phones(text: str) -> list[str]:
    return [m.strip() for m in PHONE_RE.findall(text)]


def extract_name(card):
    name_elem = card.select_one(".OSrXXb")
    name = name_elem.get_text(strip=True) if name_elem else "Unknown Name"
    return name

def extract_text_regex(card):
    return card.get_text(" ", strip=True)


def extract_website_url(card):
        link_element = card.select_one(".yYlJEf.Q7PwXb.L48Cpd.brKmxb")
        if link_element and link_element.get('href'):
            return link_element.get('href').strip()
        else:
            return "No Website Listed"


def extract_years_in_business(text):
            years_match = YEARS_RE.search(text)
            if years_match:
                return int(years_match.group(1))
            else:
                return None






async def main():
    # List to hold all our extracted data
    results = []

    async with Chrome() as browser:
        tab = await browser.start()
        
        # Load the Google search URL
        url = 'https://www.google.com/search?q=plumbers+in+twickenham&sca_esv=846ebd86ebd88482&udm=1&sxsrf=ANbL-n7otNKTRzf0IKxBg1D-B_eNHzkPxQ:1771953677533&ei=Dd6daaCTIMq0hbIP9JrLEQ&start=0&sa=N&sstk=Af77f_f68uKRPPopKhcJ-Cx7MStx8lCrmdaccUEoaxPF2sYSNBn7mTPp2HJUpuPnswbQaFpWKIM2H2ImDbYQfynSbyatyA2Et3kbS4N-JRReGhdz4Q3ekmJDbqt8QqAHGPEhkVWN9O9nNhoAP44joXcN276bx_WNkSOESSEcQJHAKnzloWjTMGbNEowohY8_Tf8d7j18clp3y94MXXNeueUKJU-18gZNe7Ef9Gglv7Y96FlWW6cup_lv__Rex86-9hD1-t8l7ty-I2ur95TCfgNmQEB_mJ7WWiT1FfcKCspEV6EPsloDrKu6frWVvltnoprZzNufbzyPO56nfa5QBHlCwN-gI09L8WCRJOqNh20XSK5njN7x8zVa_wNgVZ7ZhvTH6y4frKfVryhdwq3d9D9N&ved=2ahUKEwjgzYfs0fKSAxVKWkEAHXTNMgI4eBDx0wN6BAgPEAI&biw=1920&bih=919&dpr=1'
        await tab.go_to(url)

        # Wait BEFORE taking the snapshot to ensure JS finishes loading
        await asyncio.sleep(3)
        html_snapshot = await tab.page_source

        soup = BeautifulSoup(html_snapshot, "html.parser")
        cards = soup.select(".VkpGBb")

        for card in cards:
            # 1. Safely extract Name
            name_elem = card.select_one(".OSrXXb")
            name = name_elem.get_text(strip=True) if name_elem else "Unknown Name"

            # 2. Extract Text for Regex matching
            text = card.get_text(" ", strip=True)

            # 3. Extract Phones
            matches = PHONE_RE.findall(text)
            phones = [m.strip() for m in matches]

            # 4. Safely extract Website
            link_element = card.select_one(".yYlJEf.Q7PwXb.L48Cpd.brKmxb")
            if link_element and link_element.get('href'):
                website = link_element.get('href').strip()
            else:
                website = "No Website Listed"

            # 5. Extract Years in Business
            years_match = YEARS_RE.search(text)
            if years_match:
                years_value = int(years_match.group(1))
            else:
                years_value = None

            # 6. Save to our results list
            results.append({
                "Name": name,
                "Phones": phones,
                "Website": website,
                "Years_in_Business": years_value
            })

    # Print out our clean data nicely
    for entry in results:
        print(f"Company: {entry['Name']}")
        print(f"Phone(s): {', '.join(entry['Phones']) if entry['Phones'] else 'None'}")
        print(f"Website: {entry['Website']}")
        print(f"Years Active: {entry['Years_in_Business']}")
        print("-" * 40)

if __name__ == "__main__":
    asyncio.run(main())