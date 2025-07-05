import os, json, time, random
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor

# ─────────────────────────────
# CONFIGURATION
# ─────────────────────────────

BASE_URL    = "https://mosdac.gov.in/"  # Starting point of the crawl
MAX_PAGES   = 100                       # Limit to how many pages we crawl
MAX_THREADS = 6                         # Number of threads to speed up crawling
DELAY       = 0.1                       # Delay between requests to be polite
HEADERS     = {                         # Request headers to mimic a real browser
    "User-Agent": "Mozilla/5.0"
}

# ─────────────────────────────
# GLOBAL VARIABLES
# ─────────────────────────────

visited = set()        # Tracks which URLs we've already crawled
results = []           # Stores the final data we extract
session = requests.Session()  # Reuse a session for efficiency

# ─────────────────────────────
# CHECK IF URL IS INTERNAL
# ─────────────────────────────
def is_internal(link):
    """Check if a URL belongs to the same domain as the base URL."""
    return urlparse(link).netloc == urlparse(BASE_URL).netloc

# ─────────────────────────────
# CLEAN HTML TO REMOVE UNWANTED TAGS
# ─────────────────────────────
def clean_html(soup):
    """Remove scripts, styles, headers, navbars, etc. from HTML."""
    for tag in soup(["script", "style", "header", "footer", "nav", "aside", "noscript"]):
        tag.decompose()
    return soup

# ─────────────────────────────
# EXTRACT PAGE DATA
# ─────────────────────────────
def extract_page_data(url):
    """Download and extract title and text content from a single page."""
    try:
        r = session.get(url, headers=HEADERS, timeout=10)
        if r.status_code != 200:
            return
        r.encoding = r.apparent_encoding
        soup = BeautifulSoup(r.text, "lxml")
        clean_html(soup)

        # Get title and main content
        title = soup.title.string.strip() if soup.title else ""
        content = soup.get_text(separator="\n", strip=True)

        # Store if page has meaningful content
        if content:
            results.append({
                "url": url,
                "title": title,
                "content": content
            })
        return soup
    except:
        return None

# ─────────────────────────────
# CRAWL A SINGLE PAGE
# ─────────────────────────────
def crawl(url, executor):
    """Recursive function to crawl a page and its internal links."""
    if url in visited or len(visited) >= MAX_PAGES:
        return
    visited.add(url)
    time.sleep(random.uniform(DELAY, DELAY + 0.1))  # Add delay to avoid hammering the server

    soup = extract_page_data(url)
    if not soup:
        return

    # Find and crawl internal links
    for tag in soup.find_all("a", href=True):
        link = urljoin(url, tag["href"])
        if "#" in link or "javascript:void" in link:
            continue
        if is_internal(link) and link not in visited:
            executor.submit(crawl, link, executor)

# ─────────────────────────────
# SAVE FINAL DATA TO JSON FILE
# ─────────────────────────────
def save_output(filename="output.json"):
    """Write the results list to a JSON file."""
    with open(filename, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

# ─────────────────────────────
# MAIN EXECUTION BLOCK
# ─────────────────────────────
if __name__ == "__main__":
    print("🚀 Starting crawl...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        crawl(BASE_URL, executor)
    save_output()
    print(f"✅ Done. {len(results)} pages saved to output.json")
