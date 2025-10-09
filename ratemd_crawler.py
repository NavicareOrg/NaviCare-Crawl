# ratemd_crawler_cloudscraper.py
"""
Segmented crawler for RateMDs facility listing.
- Supports page range scraping via command-line arguments
- Uses cloudscraper to handle Cloudflare JS challenge
- Saves results to ratemds_facilities_<start>_<end>.json
"""
import os
import sys
import time
import random
import re
import json
import logging
import html as _html
from typing import List, Dict, Optional

import cloudscraper
from bs4 import BeautifulSoup
from requests import Response

# -------------------------
# Logging configuration
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# -------------------------
# Config
# -------------------------
BASE_URL = "https://www.ratemds.com"
LISTING_URL = f"{BASE_URL}/facilities/?country=ca&province=on"
INVALID_CHARS_REGEX = r'[\u0000-\u001F\uFEFF]'

# Retry settings
MAX_RETRIES = 4
BASE_BACKOFF = 1.5

# Optional proxy
PROXY = os.environ.get("PROXY", None)

# User agents
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.90 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.5993.90 Safari/537.36",
    "Mozilla/5.0 (iPhone; CPU iPhone OS 17_0 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.0 Mobile/15E148 Safari/604.1",
    "Mozilla/5.0 (iPad; CPU OS 16_5 like Mac OS X) AppleWebKit/605.1.15 (KHTML, like Gecko) CriOS/117.0.5938.62 Mobile/15E148 Safari/604.1",
]

BASE_HEADERS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Referer": "https://www.google.com",
}

# -------------------------
# Create cloudscraper session
# -------------------------
scraper = cloudscraper.create_scraper()
scraper.headers.update(BASE_HEADERS)
if PROXY:
    scraper.proxies.update({"http": PROXY, "https": PROXY})
    logging.info(f"Using proxy from environment: {PROXY}")

def update_user_agent():
    ua = random.choice(USER_AGENTS)
    scraper.headers.update({"User-Agent": ua})

def log_response_details(url: str, resp: Optional[Response]):
    if resp is None:
        logging.error(f"No response object for {url}")
        return
    logging.error(f"Request to {url} returned status {resp.status_code}")
    logging.error(f"Final URL (after redirects): {resp.url}")
    content = resp.text or ""
    if any(k in content.lower() for k in ("cloudflare", "captcha", "access denied", "verify you are human")):
        logging.error("Response indicates Cloudflare / CAPTCHA / bot-protection page.")
    logging.error(f"Response content length: {len(content)} bytes")

def get_with_retries(url: str, timeout: int = 20) -> Optional[Response]:
    """GET with retries + exponential backoff."""
    for attempt in range(MAX_RETRIES):
        try:
            update_user_agent()
            logging.info(f"GET {url} (attempt {attempt+1}/{MAX_RETRIES})")
            resp = scraper.get(url, timeout=timeout)
            if resp.status_code == 200:
                return resp
            log_response_details(url, resp)
        except Exception as e:
            logging.error(f"Request error for {url}: {e}")
        backoff = BASE_BACKOFF * (2 ** attempt) + random.uniform(0, 1.0)
        logging.info(f"Sleeping {backoff:.2f}s before next retry.")
        time.sleep(backoff)
    logging.error(f"All retries failed for {url}")
    return None

# -------------------------
# Parsing helpers
# -------------------------
def scrape_detail(detail_url: str) -> Dict:
    """Scrape the detail page for phone, address, rating, and review count."""
    time.sleep(random.uniform(0.5, 1.5))
    resp = get_with_retries(detail_url, timeout=15)
    if not resp:
        logging.error(f"Failed to fetch detail page: {detail_url}")
        return {"phone": "N/A", "address": "N/A", "rating": "N/A", "review_count": "N/A"}

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    data = {}

    # Phone: Extract from JSON-LD scripts
    json_ld_scripts = soup.find_all("script", type="application/ld+json")
    data["phone"] = "N/A"
    for json_ld_script in json_ld_scripts:
        raw_json_string = json_ld_script.string or json_ld_script.get_text()
        if not raw_json_string:
            continue

        raw_json_string = raw_json_string.strip()
        cleaned_string = re.sub(INVALID_CHARS_REGEX, '', raw_json_string)
        cleaned_string = _html.unescape(cleaned_string)
        cleaned_string = cleaned_string.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
        cleaned_string = re.sub(r',\s*}', '}', cleaned_string)
        cleaned_string = re.sub(r',\s*]', ']', cleaned_string)
        cleaned_string = re.sub(r'^\s*//.*?\n', '', cleaned_string, flags=re.MULTILINE)
        cleaned_string = re.sub(r'/\*.*?\*/', '', cleaned_string, flags=re.DOTALL)

        start_index = cleaned_string.find('{') if '{' in cleaned_string else cleaned_string.find('[')
        end_index = cleaned_string.rfind('}') if '}' in cleaned_string else cleaned_string.rfind(']')
        if start_index == -1 or end_index == -1 or end_index <= start_index:
            continue
        json_string = cleaned_string[start_index:end_index + 1]

        try:
            json_data = json.loads(json_string)
            if isinstance(json_data, dict) and json_data.get("@type") == "MedicalClinic" and "telephone" in json_data:
                data["phone"] = json_data.get("telephone", "N/A")

                if "address" in json_data and isinstance(json_data["address"], dict):
                    address_locality = json_data["address"].get("addressLocality", "")
                    if address_locality and ", " in address_locality:
                        data["city"], data["province"] = address_locality.split(", ", 1)
                    else:
                        data["city"] = address_locality or "N/A"
                        data["province"] = "N/A"

                if "geo" in json_data and isinstance(json_data["geo"], dict):
                    data["longitude"] = json_data["geo"].get("longitude", "N/A")
                    data["latitude"] = json_data["geo"].get("latitude", "N/A")

            if data["phone"] != "N/A":
                break
        except (json.JSONDecodeError, Exception) as e:
            logging.error(f"Error parsing JSON-LD for {detail_url}: {e}")
    else:
        phone_pattern = re.compile(r"\(?\d{3}\)?[\s-]*\d{3}-\d{4}")
        phone_elements = soup.find_all(["div", "span", "a"], string=phone_pattern)
        if phone_elements:
            data["phone"] = phone_elements[0].get_text(strip=True)
        else:
            phone = soup.find("div", class_="phone")
            if phone:
                data["phone"] = re.sub(r"[^\d\s\-\(\)+]", "", phone.get_text(strip=True))
            else:
                ph = soup.select_one("a[href^='tel:']")
                data["phone"] = ph.get_text(strip=True) if ph else "N/A"

    # Rating & reviews
    reviews = soup.find("span", class_="reviews")
    if reviews:
        rating_tag = reviews.find("span", recursive=False)
        data["rating"] = rating_tag.get_text(strip=True) if rating_tag and rating_tag.get_text(strip=True).replace(".", "").isdigit() else "N/A"

        review_container = reviews.find_all("span", recursive=False)[1] if len(reviews.find_all("span", recursive=False)) > 1 else None
        if review_container:
            review_count_tag = review_container.find("span", string=re.compile(r"^\d+$"))
            data["review_count"] = review_count_tag.get_text(strip=True) if review_count_tag else "N/A"
        else:
            data["review_count"] = "N/A"
    else:
        data["rating"] = "N/A"
        data["review_count"] = "N/A"

    time.sleep(random.uniform(1.0, 3.0))
    return data

def scrape_page(page_number: int) -> List[Dict]:
    url = f"{LISTING_URL}&page={page_number}"
    resp = get_with_retries(url, timeout=20)
    if not resp:
        logging.error(f"Failed to fetch list page {page_number}")
        return []

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    clinics = []

    for item in soup.find_all("div", class_="search-item"):
        a = item.find("a", class_="search-item-location-link")
        if not a or "href" not in a.attrs:
            continue
        href = a["href"]
        name_tag = a.find("h2", class_="search-item-info search-item-location-name")
        name = name_tag.get_text(strip=True) if name_tag else a.get_text(strip=True) or "Unnamed Clinic"
        detail_url = BASE_URL + href
        logging.info(f"Found clinic: {name} -> {detail_url}")

        detail_data = scrape_detail(detail_url)
        clinics.append({
            "name": name,
            "detail_url": detail_url,
            "phone": detail_data.get("phone", "N/A"),
            "city": detail_data.get("city", "N/A"),
            "province": detail_data.get("province", "N/A"),
            "longitude": detail_data.get("longitude", "N/A"),
            "latitude": detail_data.get("latitude", "N/A"),
            "rating": detail_data.get("rating", "N/A"),
            "review_count": detail_data.get("review_count", "N/A"),
        })

    return clinics

def get_total_pages() -> int:
    """Fetch the first listing page and determine how many pages exist."""
    resp = get_with_retries(LISTING_URL, timeout=20)
    if not resp:
        logging.error("Could not fetch the listing first page.")
        return 0

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    links = soup.select('ul[class*="pagination"] a[href*="page="]')
    total_pages = 0
    if links:
        for a in links:
            if "href" in a.attrs:
                m = re.search(r"page=(\d+)", a["href"])
                if m:
                    total_pages = max(total_pages, int(m.group(1)))
    if total_pages == 0:
        total_text = soup.get_text(" ", strip=True)
        m = re.search(r"Page\s*\d+\s*of\s*(\d+)", total_text, re.IGNORECASE)
        if m:
            total_pages = int(m.group(1))

    logging.info(f"Total pages determined: {total_pages}")
    return total_pages

# -------------------------
# Main orchestration with page range support
# -------------------------
def main(start_page: int = 1, end_page: int = None):
    """
    Scrape pages from start_page to end_page (inclusive).
    If end_page is None, scrape all pages.
    """
    logging.info(f"Starting cloudscraper-based crawler (pages {start_page} to {end_page or 'end'})")
    
    if end_page is None:
        total_pages = get_total_pages()
        if total_pages <= 0:
            logging.error("Could not determine total pages. Exiting.")
            return
        end_page = total_pages
    
    # Validate range
    if start_page < 1 or end_page < start_page:
        logging.error(f"Invalid page range: {start_page} to {end_page}")
        return

    all_clinics = []
    for p in range(start_page, end_page + 1):
        logging.info(f"Scraping list page {p}/{end_page}")
        clinics = scrape_page(p)
        if not clinics:
            logging.warning(f"No clinics scraped from page {p}. Continuing.")
        all_clinics.extend(clinics)
        time.sleep(random.uniform(2.0, 5.0))

    # Save with page range in filename
    output_file = f"ratemds_facilities_{start_page}_{end_page}.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_clinics, f, ensure_ascii=False, indent=2)

    logging.info(f"Finished. Total clinics collected: {len(all_clinics)}. Saved to {output_file}.")

if __name__ == "__main__":
    # Parse command-line arguments
    if len(sys.argv) == 1:
        # No arguments: scrape all pages
        main()
    elif len(sys.argv) == 3:
        # Two arguments: start and end page
        try:
            start = int(sys.argv[1])
            end = int(sys.argv[2])
            main(start_page=start, end_page=end)
        except ValueError:
            print("Usage: python ratemd_crawler.py [start_page end_page]")
            print("Example: python ratemd_crawler.py 1 500")
            sys.exit(1)
    else:
        print("Usage: python ratemd_crawler.py [start_page end_page]")
        print("Example: python ratemd_crawler.py 1 500")
        sys.exit(1)