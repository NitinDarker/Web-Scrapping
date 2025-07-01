# 🕸️ Web Scraping: mosdac.gov.in

This repository contains a Python-based web scraping script to extract static data (text, tables, PDFs) from [MOSDAC.gov.in](https://mosdac.gov.in/). It collects:

- Page content as `.txt`
- HTML tables as `.csv`
- Linked PDFs

> ✅ Includes both the **script** and **extracted dataset**, so you can either run the scraper yourself or just use the ready-made data.

---

## 🛠️ How to Run the Scraper

1. **Clone the repository**:

   ```bash
   git clone https://github.com/NitinDarker/Web-Scrapping.git

   cd Web-Scrapping
   ```

2. Install required Python packages:
   ```bash
   pip install requests beautifulsoup4 pandas lxml
    ```


3. Run the script:

    ```bash
    python main.py
    ```

### This will:

- Crawl through internal pages of MOSDAC

- Save cleaned text data into text/

- Save HTML tables into csv/

- Download PDFs into pdfs/


## 🚨 In Case of Failures

If the scraper doesn't run (due to rate-limiting, site changes, or network issues), you can still use the extracted data available in:

    /text/ for page content

    /csv/ for tables

    /pdfs/ for documents

# 🙋 Author

Nitin Sharma (NitinDarker)