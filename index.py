import json, time, random
import requests
from bs4 import BeautifulSoup
from pathlib import PurePosixPath
from urllib.parse import urlparse, urlunparse, urljoin, parse_qsl, urlencode

BASE_URL = "https://mosdac.gov.in/"
MAX_PAGES = 10000
DELAY = 0.01
HEADERS = {
    "User-Agent": "Mozilla/5.0"
}

visited = set()
results = []
session = requests.Session()

FORBIDDEN_EXTENSIONS = {'.zip', '.tar', '.gz', '.rar', '.7z', '.pdf', '.xml', '.exe', '.msi', '.tar.gz', '.tgz', '.html'}

def normalize_url(url):
    """
    Normalizes a given URL by joining it with BASE_URL, removing query parameters and fragments,
    and standardizing the path.
    """
    absolute_url = urljoin(BASE_URL, url)
    parsed = urlparse(absolute_url)

    # Remove all query parameters and fragments
    path = parsed.path.rstrip('/')
    if not path:
        path = '/'
    cleaned = parsed._replace(path=path, query="", fragment="")
    return urlunparse(cleaned)

def has_forbidden_extension(url):
    """
    Checks if the URL ends with any of the blocked file extensions (e.g., .zip, .pdf, .tar.gz, etc.).
    """
    path = urlparse(url).path.lower()
    suffixes = PurePosixPath(path).suffixes
    combined = ''.join(suffixes)
    return combined in FORBIDDEN_EXTENSIONS

def is_internal(link):
    """
    Verifies if a given link belongs to the same domain as BASE_URL.
    """
    absolute = urljoin(BASE_URL, link)
    return urlparse(absolute).netloc == urlparse(BASE_URL).netloc

def clean_html(soup):
    """
    Removes unwanted HTML elements from the BeautifulSoup-parsed HTML to reduce noise.
    Elements removed: <script>, <style>, <noscript>
    """
    for tag in soup(['script', 'style', 'noscript']):
        tag.decompose()
    return soup

def extract_main_content(soup):
    """
    Extracts the main textual content from a webpage, targeting the <div class="content clearfix"> block.
    Returns cleaned, whitespace-stripped text.
    """
    main = soup.find("div", class_="content clearfix")
    if not main:
        return ""
    return main.get_text(separator=" ", strip=True)

def extract_valid_links(soup, base_url):
    """
    Scans all <a href=""> elements on the page, filters them using domain and extension rules,
    and returns a list of clean, normalized URLs.
    """
    links = []
    for tag in soup.find_all("a", href=True):
        href = tag["href"]
        absolute = urljoin(base_url, href)
        normalized = normalize_url(absolute)
        parsed = urlparse(normalized).path
        if (
            "javascript:void" in normalized
            or not is_internal(normalized)
            or "/download" in parsed
            or "/auth/" in parsed
            or "/internal/" in parsed
            or "/483/" in parsed
            or "/464/" in parsed
            or "/940z/" in parsed
            or has_forbidden_extension(normalized)
        ):
            continue
        links.append(normalized)
    return links

def fetch_hindi_content(url):
    """
    Attempts to retrieve the Hindi version of a page by adding '?language=hi' to the current URL.
    If valid, extracts the main Hindi content.
    """
    parsed = urlparse(url)
    query = dict(parse_qsl(parsed.query))
    if query.get("language") == "hi":
        return None

    hindi_url = urlunparse(parsed._replace(query="language=hi"))
    try:
        r = session.get(hindi_url, headers=HEADERS, timeout=5)
        if "text/html" not in r.headers.get("Content-Type", ""):
            return None
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "lxml")
        clean_html(soup)
        return extract_main_content(soup)
    except:
        return None

def extract_page_data(url):
    """
    Handles downloading and parsing a single page:
    - Cleans HTML
    - Extracts title, content, Hindi content (if any), and valid internal links
    - Appends the result to the global `results` list
    """
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
        links = extract_valid_links(soup, url)

        if content:
            entry = {
                "url": url,
                "title": title,
                "content": content,
                "content_hindi": fetch_hindi_content(url) or "",  # ensure key order
                "links": sorted(set(links))
            }

            results.append(entry)

        return soup
    except:
        return None

def crawl(url):
    """
    Recursive DFS-style crawler with duplicate avoidance.
    Normalizes and validates each link before visiting.
    """
    url = normalize_url(url)
    print("Crawling... " + url)
    if url in visited or len(visited) >= MAX_PAGES:
        return
    visited.add(url)
    time.sleep(random.uniform(DELAY, DELAY + 0.01))

    soup = extract_page_data(url)
    if not soup:
        return

    for link in extract_valid_links(soup, url):
        if link not in visited:
            crawl(link)

def save_output(filename="output.json"):
    """
    Dumps the collected results into a JSON file with UTF-8 encoding and pretty-printed indentation.
    """
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

if __name__ == "__main__":
    print("Starting Crawler...")
    crawl(BASE_URL)
    save_output()
    print(f"✅ Done. {len(results)} pages saved to output.json")
