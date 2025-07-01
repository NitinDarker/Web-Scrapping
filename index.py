import os
import time
import random
import requests
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup, XMLParsedAsHTMLWarning
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# Config
BASE_URL = "https://mosdac.gov.in/"
MAX_PAGES = 300
MAX_THREADS = 4
DELAY = 0.2
visited = set()

# Output Folders
TEXT_DIR = "text"
PDF_DIR = "pdfs"
CSV_DIR = "csv"
for folder in [TEXT_DIR, PDF_DIR, CSV_DIR]:
    os.makedirs(folder, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9"
}
session = requests.Session()

def safe_get(url):
    try:
        res = session.get(url, headers=HEADERS, timeout=10)
        if res.status_code == 200:
            res.encoding = res.apparent_encoding
            return res
    except:
        pass
    return None

def is_internal(url):
    return urlparse(url).netloc == urlparse(BASE_URL).netloc

def extract_text(soup, url, page_id):
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(separator="\n", strip=True)
    if text:
        path = os.path.join(TEXT_DIR, f"page_{page_id}.txt")
        with open(path, "w", encoding="utf-8") as f:
            f.write(f"{url}\n\n{text}")
        print(f"[TXT] {path}")

def extract_tables(soup, page_id):
    try:
        tables = pd.read_html(StringIO(str(soup)))
        for i, table in enumerate(tables):
            path = os.path.join(CSV_DIR, f"table_{page_id}_{i}.csv")
            table.to_csv(path, index=False)
            print(f"[CSV] {path}")
    except:
        pass

def download_pdf(url):
    name = os.path.basename(url)
    path = os.path.join(PDF_DIR, name)
    if not os.path.exists(path):
        res = safe_get(url)
        if res:
            with open(path, 'wb') as f:
                f.write(res.content)
            print(f"[PDF] {path}")

def get_soup(res, url):
    content_type = res.headers.get('Content-Type', '').lower()
    if 'xml' in content_type or url.lower().endswith('.xml'):
        return BeautifulSoup(res.text, 'xml')  # Use XML parser
    return BeautifulSoup(res.text, 'lxml')     # Default to HTML parser

def crawl(url, page_id=0, executor=None):
    if url in visited or len(visited) >= MAX_PAGES:
        return
    if "javascript:void" in url or "#" in url:
        return
    visited.add(url)
    time.sleep(random.uniform(DELAY, DELAY + 0.1))

    res = safe_get(url)
    if not res:
        return

    try:
        soup = get_soup(res, url)
    except Exception as e:
        print(f"[!] Failed to parse: {url} ({e})")
        return

    print(f"[+] Crawling: {url}")
    extract_text(soup, url, page_id)
    extract_tables(soup, page_id)

    for tag in soup.find_all("a", href=True):
        link = urljoin(url, tag["href"])
        if link.lower().endswith(".pdf") and executor:
            executor.submit(download_pdf, link)

    for tag in soup.find_all("a", href=True):
        link = urljoin(url, tag["href"])
        if is_internal(link) and link not in visited:
            crawl(link, page_id + 1, executor)

if __name__ == "__main__":
    print("🚀 Starting simple scraper...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        crawl(BASE_URL, 0, executor)
    print("✅ Done.")
