import json, time, random
import requests
from bs4 import BeautifulSoup
from pathlib import PurePosixPath
from urllib.parse import urlparse, urlunparse, urljoin

BASE_URL    = "https://mosdac.gov.in/"
MAX_PAGES   = 10000
DELAY       = 0.01
HEADERS     = {
    "User-Agent": "Mozilla/5.0"
}

visited = set()
results = []
session = requests.Session()
FORBIDDEN_EXTENSIONS = {'.zip', '.tar', '.gz', '.rar', '.7z', '.pdf', '.xml', '.exe', '.msi', '.tar.gz', '.tgz', '.html'}

def normalize_url(url):
    absolute_url = urljoin(BASE_URL, url)
    parsed = urlparse(absolute_url)
    path = parsed.path.rstrip('/')
    if not path:
        path = '/'
    cleaned = parsed._replace(path=path, query="", fragment="")
    return urlunparse(cleaned)

def is_internal(link):
    absolute = urljoin(BASE_URL, link)
    return urlparse(absolute).netloc == urlparse(BASE_URL).netloc

def has_forbidden_extension(url):
    path = urlparse(url).path.lower()
    suffixes = PurePosixPath(path).suffixes
    combined = ''.join(suffixes)
    return combined in FORBIDDEN_EXTENSIONS


def clean_html(soup):
    for tag in soup(['script', 'style', 'noscript']):
        tag.decompose()
    return soup

def extract_main_content(soup):
    main = soup.find("div", class_="content clearfix")
    if not main:
        return ""
    return main.get_text(separator=" ", strip=True)

def extract_page_data(url):
    try:
        r = session.get(url, headers=HEADERS, timeout=5)
        if "text/html" not in r.headers.get("Content-Type", ""):
            return None

        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "lxml")
        clean_html(soup)

        raw_title = soup.title.string.strip() if soup.title else ""
        suffix = " | Meteorological & Oceanographic Satellite Data Archival Centre"
        title = raw_title.removesuffix(suffix).strip()
        
        content = extract_main_content(soup)

        links = []
        for tag in soup.find_all("a", href=True):
            href = tag["href"]
            absolute = urljoin(url, href)
            normalized = normalize_url(absolute)
            if (
                "javascript:void" in normalized
                or not is_internal(normalized)
                or "/download" in urlparse(normalized).path
                or "/node/" in urlparse(normalized).path
                or "/auth/" in urlparse(normalized).path
                or "/internal/" in urlparse(normalized).path
                or has_forbidden_extension(normalized)
            ):
                continue
            links.append(normalized)

        if content:
            results.append({
                "url": url,
                "title": title,
                "content": content,
                "links": sorted(set(links))
            })
            
        return soup
    except:
        return None

def crawl(url):
    url = normalize_url(url)
    print("Crawling... " + url)
    if url in visited or len(visited) >= MAX_PAGES:
        return
    visited.add(url)
    time.sleep(random.uniform(DELAY, DELAY + 0.01))

    soup = extract_page_data(url)
    if not soup:
        return

    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        absolute = urljoin(url, href)
        normalized = normalize_url(absolute)
        if (
            "javascript:void" in normalized
            or not is_internal(normalized)
            or "/download" in urlparse(normalized).path
            or "/node/" in urlparse(normalized).path
            or "/auth/" in urlparse(normalized).path
            or "/internal/" in urlparse(normalized).path
            or has_forbidden_extension(normalized)
        ):
            continue
        if normalized not in visited:
            crawl(normalized)

# Dumps the results into output.json
def save_output(filename="output.json"):
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    print("Starting Crawler...")
    crawl(BASE_URL)
    save_output()
    print(f"✅ Done. {len(results)} pages saved to output.json")
