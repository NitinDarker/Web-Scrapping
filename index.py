import json, time, random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse, urlunparse
from concurrent.futures import ThreadPoolExecutor

BASE_URL    = "https://mosdac.gov.in/"
MAX_PAGES   = 10000
MAX_THREADS = 10
DELAY       = 0.01
HEADERS     = {
    "User-Agent": "Mozilla/5.0"
}

visited = set()
results = []
session = requests.Session()

def normalize_url(url):
    parsed = urlparse(url)
    cleaned = parsed._replace(query="", fragment="")
    return urlunparse(cleaned).rstrip('/')

def is_internal(link):
    return urlparse(link).netloc == urlparse(BASE_URL).netloc

def clean_html(soup):
    for tag in soup(['script', 'style', 'header', 'footer', 'nav', 'noscript']):
        tag.decompose()
    return soup

def extract_main_content(soup):
    main = soup.find("div", class_="content clearfix")
    if not main:
        return ""
    return main.get_text(separator=" ", strip=True)

def extract_page_data(url):
    try:
        if url.lower().endswith(".pdf"):
            return None

        r = session.get(url, headers=HEADERS, timeout=5)
        if "text/html" not in r.headers.get("Content-Type", ""):
            return None

        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "lxml")
        clean_html(soup)

        title = soup.title.string.strip() if soup.title else ""
        content = extract_main_content(soup)

        if content:
            results.append({
                "url": url,
                "title": title,
                "content": content
            })
        return soup
    except:
        return None

def crawl(url, executor):
    url = normalize_url(url)
    if url in visited or len(visited) >= MAX_PAGES:
        return
    visited.add(url)
    time.sleep(random.uniform(DELAY, DELAY + 0.1))

    soup = extract_page_data(url)
    if not soup:
        return

    for tag in soup.find_all("a", href=True):
        link = urljoin(url, tag["href"])
        link = normalize_url(link)
        if "javascript:void" in link or not is_internal(link):
            continue
        if link not in visited:
            executor.submit(crawl, link, executor)

def save_output(filename="output.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    print("🚀 Starting crawl...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        crawl(BASE_URL, executor)
    save_output()
    print(f"✅ Done. {len(results)} pages saved to output.json")
