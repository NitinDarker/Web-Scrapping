import os, time, random, requests, pandas as pd
from io import StringIO
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

# ─────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────

BASE_URL    = "https://mosdac.gov.in/"
MAX_PAGES   = 500
MAX_THREADS = 8
DELAY       = 0.05

TEXT_DIR, PDF_DIR, CSV_DIR = "text", "pdfs", "csv"
for d in [TEXT_DIR, PDF_DIR, CSV_DIR]:
    os.makedirs(d, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Accept-Language": "en-US,en;q=0.9"
}
session = requests.Session()
visited = set()
site_graph = defaultdict(set)  # Stores edges: {source -> {target1, target2}}

# ─────────────────────────────────────────────
# UTILITY FUNCTIONS
# ─────────────────────────────────────────────

def safe_get(url):
    try:
        r = session.get(url, headers=HEADERS, timeout=10)
        if r.status_code == 200:
            r.encoding = r.apparent_encoding
            return r
    except:
        pass
    return None

def is_internal(u):
    return urlparse(u).netloc == urlparse(BASE_URL).netloc

def remove_common_layout_tags(soup):
    for tag in soup(["header", "nav", "footer", "script", "style", "noscript", "aside", "form"]):
        tag.decompose()
    for tag in soup.find_all("a", class_="language link"):
        tag.decompose()

def get_main_content(soup):
    candidates = [
        soup.find("div", id="content"),
        soup.find("section"),
        soup.find("div", class_="container"),
        soup.find("main"),
        soup.find("body")
    ]
    for c in candidates:
        if c: return c
    return soup

# ─────────────────────────────────────────────
# EXTRACTION FUNCTIONS
# ─────────────────────────────────────────────

def extract_text(soup, url, page_id):
    remove_common_layout_tags(soup)
    main = get_main_content(soup)
    text = main.get_text(separator="\n", strip=True)
    if text.strip():
        fn = os.path.join(TEXT_DIR, f"_temp_{page_id}.txt")
        with open(fn, "w", encoding="utf-8") as f:
            f.write(f"URL: {url}\n\n{text}\n\n{'-'*80}\n")

def extract_tables(soup, page_id):
    try:
        for i, tbl in enumerate(pd.read_html(StringIO(str(soup)))):
            fn = os.path.join(CSV_DIR, f"_temp_{page_id}_{i}.csv")
            tbl.to_csv(fn, index=False)
    except:
        pass

def download_pdf(url):
    fn = os.path.basename(url.split("?")[0])
    path = os.path.join(PDF_DIR, fn if fn.endswith(".pdf") else f"{fn}.pdf")
    # if not os.path.exists(path):
    #     r = safe_get(url)
    #     if r and "application/pdf" in r.headers.get("Content-Type", ""):
    #         with open(path, "wb") as f:
    #             f.write(r.content)
            # print(f"[PDF] {path}")

# ─────────────────────────────────────────────
# CRAWLING LOGIC
# ─────────────────────────────────────────────

def crawl(url, page_id=0, executor=None):
    if url in visited or len(visited) >= MAX_PAGES or "javascript:void" in url or "#" in url:
        return
    visited.add(url)
    time.sleep(random.uniform(DELAY, DELAY + 0.05))

    r = safe_get(url)
    if not r:
        return
    soup = BeautifulSoup(r.text, "lxml")

    print(f"[+] Crawling: {url}")
    extract_text(soup, url, page_id)
    extract_tables(soup, page_id)

    for tag in soup.find_all("a", href=True):
        link = urljoin(url, tag["href"])
        text = tag.get_text(strip=True).lower()
        if ".pdf" in link.lower() or text.endswith(".pdf"):
            if executor:
                executor.submit(download_pdf, link)

    for tag in soup.find_all("a", href=True):
        link = urljoin(url, tag["href"])
        if is_internal(link):
            site_graph[url].add(link)  # Record edge
            if link not in visited:
                crawl(link, page_id + 1, executor)

# ─────────────────────────────────────────────
# FILE CONSOLIDATION
# ─────────────────────────────────────────────

def consolidate_text_files():
    max_bytes = 1024 * 1024
    count, size, chunk = 0, 0, ""
    for fname in sorted(os.listdir(TEXT_DIR)):
        if not fname.startswith("_temp_"): continue
        path = os.path.join(TEXT_DIR, fname)
        content = open(path, encoding="utf-8").read()
        if size + len(content.encode("utf-8")) > max_bytes:
            with open(os.path.join(TEXT_DIR, f"combined_{count}.txt"), "w", encoding="utf-8") as out:
                out.write(chunk)
            count += 1
            size = 0
            chunk = ""
        chunk += content
        size += len(content.encode("utf-8"))
        os.remove(path)
    if chunk:
        with open(os.path.join(TEXT_DIR, f"combined_{count}.txt"), "w", encoding="utf-8") as out:
            out.write(chunk)

def consolidate_csv_files():
    csv_files = [f for f in os.listdir(CSV_DIR) if f.startswith("_temp_") and f.endswith(".csv")]
    combined_df, file_count = [], 0
    for fname in sorted(csv_files):
        path = os.path.join(CSV_DIR, fname)
        try:
            df = pd.read_csv(path)
            combined_df.append(df)
            os.remove(path)
            if sum(len(d) for d in combined_df) >= 1000:
                pd.concat(combined_df).to_csv(os.path.join(CSV_DIR, f"combined_{file_count}.csv"), index=False)
                combined_df = []
                file_count += 1
        except:
            pass
    if combined_df:
        pd.concat(combined_df).to_csv(os.path.join(CSV_DIR, f"combined_{file_count}.csv"), index=False)

# ─────────────────────────────────────────────
# EXPORT GRAPH STRUCTURE TO CSV
# ─────────────────────────────────────────────

def export_graph_csv(filename="site_structure.csv"):
    with open(filename, "w", encoding="utf-8") as f:
        f.write("source,target\n")
        for src, tgts in site_graph.items():
            for tgt in tgts:
                f.write(f"{src},{tgt}\n")
    print(f"📊 Graph exported to {filename}")

# ─────────────────────────────────────────────
# MAIN EXECUTION
# ─────────────────────────────────────────────

if __name__ == "__main__":
    print("🚀 Starting MOSDAC scraper...")
    with ThreadPoolExecutor(max_workers=MAX_THREADS) as executor:
        crawl(BASE_URL, 0, executor)
    print("✅ Crawling complete. Consolidating files...")

    consolidate_text_files()
    consolidate_csv_files()
    export_graph_csv()

    print("✅ All files and graph written successfully.")
