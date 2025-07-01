import os
import time
import random
import requests
import fitz
import pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
from bs4 import XMLParsedAsHTMLWarning
import warnings

warnings.filterwarnings("ignore", category=XMLParsedAsHTMLWarning)

# Config
BASE_URL = "https://mosdac.gov.in/"
MAX_THREADS = 4
MAX_PAGES = 300
TEXT_DIR = "text"
PDF_DIR = "pdfs"
PDF_TEXT_DIR = "pdf_text"
CSV_DIR = "csv"
visited = set()

# Setup
session = requests.Session()
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:113.0) Gecko/20100101 Firefox/113.0",
    "Accept-Language": "en-US,en;q=0.9"
}
for d in [TEXT_DIR, PDF_DIR, PDF_TEXT_DIR, CSV_DIR]:
    os.makedirs(d, exist_ok=True)

def safe_get(url, retries=2, delay=0.5):
    for i in range(retries):
        try:
            res = session.get(url, headers=HEADERS, timeout=10)
            if res.status_code == 200:
                res.encoding = res.apparent_encoding
                return res
        except:
            pass
        time.sleep(delay * (2 ** i))
    return None

def is_internal(url, base):
    return urlparse(url).netloc == urlparse(base).netloc

def extract_text(soup, url, page_id):
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    try:
        text = soup.get_text(separator="\n", strip=True)
        text = text.encode("utf-8", errors="ignore").decode("utf-8", errors="ignore")
        filename = os.path.join(TEXT_DIR, f"page_{page_id}.txt")
        with open(filename, 'w', encoding='utf-8') as f:
            f.write(f"### {url} ###\n\n{text}")
        print(f"[TXT] Saved text: {filename}")
    except Exception as e:
        print(f"[!] Text extraction failed for {url} → {e}")

def extract_tables(soup, page_url, page_id):
    try:
        tables = pd.read_html(StringIO(str(soup)))
        for i, table in enumerate(tables):
            path = os.path.join(CSV_DIR, f"table_{page_id}_{i}.csv")
            table.to_csv(path, index=False)
            print(f"[CSV] Saved table: {path}")
    except:
        pass

def download_pdf(url):
    filename = os.path.join(PDF_DIR, os.path.basename(url))
    if os.path.exists(filename):
        return filename
    r = safe_get(url)
    if r:
        with open(filename, 'wb') as f:
            for chunk in r.iter_content(1024):
                f.write(chunk)
        print(f"[PDF] Downloaded: {filename}")
        return filename
    return None

def extract_text_from_pdf(filepath):
    if not filepath: return
    out_file = os.path.join(PDF_TEXT_DIR, os.path.basename(filepath).replace('.pdf', '.txt'))
    if os.path.exists(out_file): return
    try:
        doc = fitz.open(filepath)
        text = "\n".join(page.get_text() for page in doc)
        with open(out_file, 'w', encoding='utf-8') as f:
            f.write(text)
        print(f"[TXT] Extracted PDF text: {out_file}")
    except Exception as e:
        print(f"[!] PDF parse failed: {filepath} -> {e}")

def get_soup(res, url):
    ct = res.headers.get('Content-Type', '').lower()
    return BeautifulSoup(res.text, features='xml' if 'xml' in ct or '.xml' in url.lower() else 'lxml')

def crawl(url, base, page_id=0, executor=None):
    if url in visited or len(visited) >= MAX_PAGES:
        return
    if "javascript:void" in url or "#" in url:
        return
    visited.add(url)

    time.sleep(random.uniform(0.1, 0.3))
    r = safe_get(url)
    if not r:
        return

    soup = get_soup(r, url)
    print(f"[+] Crawling: {url}")
    extract_text(soup, url, page_id)

    if executor:
        executor.submit(extract_tables, soup, url, page_id)

    for tag in soup.find_all('a', href=True):
        link = urljoin(url, tag['href'])
        if link.lower().endswith('.pdf'):
            if executor:
                future = executor.submit(download_pdf, link)
                future.add_done_callback(lambda fut: extract_text_from_pdf(fut.result()) if fut.result() else None)

    for tag in soup.find_all('a', href=True):
        link = urljoin(url, tag['href'])
        if is_internal(link, base) and link not in visited:
            crawl(link, base, page_id + 1, executor)

if __name__ == "__main__":
    print("MOSDAC Scraper Running...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        crawl(BASE_URL, BASE_URL, 0, executor)
    print("\n✅ Done — All content extracted.")
