import stealth_requests as requests

resp = requests.get('https://www.blackhatworld.com/seo/why-niche-relevance-still-wins-in-link-building.1794965/page-2')

with open("test.html", "w") as f:
    f.write(resp.text)

