# ratemd_crawler_cloudscraper.py
"""
Play: cloudscraper-based crawler for RateMDs facility listing.
- Uses cloudscraper to handle Cloudflare JS challenge.
- Rotates User-Agents, has exponential backoff retries, optional proxy support.
- Preserves structure: get_total_pages -> scrape_page -> scrape_detail
- Saves results to ratemds_facilities.json

Install:
    pip install cloudscraper beautifulsoup4

Note:
- Check robots.txt and legal/ToS considerations before running for commercial use.
- This is intended for one-off / limited extraction. If cloudscraper still fails,
  consider contacting the site owner or using a licensed data source.
"""
import os
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
OUTPUT_FILE = "ratemds_facilities.json"
INVALID_CHARS_REGEX = r'[\u0000-\u001F\uFEFF]'  # Control characters

# Retry settings
MAX_RETRIES = 4
BASE_BACKOFF = 1.5  # seconds, will do base * (2**attempt) +/- jitter

# Optional proxy (set via environment variable PROXY e.g. "http://user:pass@host:port")
PROXY = os.environ.get("PROXY", None)

# User agents to rotate
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
    logging.info(f"Set User-Agent: {ua}")

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
    try:
        cookies = scraper.cookies.get_dict()
        logging.info(f"Session cookies: {cookies}")
    except Exception:
        pass
    try:
        req_headers = resp.request.headers
        logging.info("Headers sent with request:")
        for k, v in req_headers.items():
            logging.info(f"  {k}: {v}")
    except Exception:
        pass

def get_with_retries(url: str, timeout: int = 20) -> Optional[Response]:
    """GET with retries + exponential backoff. Returns Response or None."""
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
    time.sleep(random.uniform(0.5, 1.5))  # small pre-wait
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
            logging.debug(f"Empty JSON-LD script for {detail_url}")
            continue

        # Clean and normalize JSON string
        # Try to handle both {} and [] cases
        raw_json_string = raw_json_string.strip()
        cleaned_string = re.sub(INVALID_CHARS_REGEX, '', raw_json_string)
        cleaned_string = _html.unescape(cleaned_string)
        cleaned_string = cleaned_string.replace('\n', '\\n').replace('\r', '\\r').replace('\t', '\\t')
        # Fix common JSON issues: trailing commas, etc.
        cleaned_string = re.sub(r',\s*}', '}', cleaned_string)
        cleaned_string = re.sub(r',\s*]', ']', cleaned_string)
        # Remove comments or stray text before/after JSON
        cleaned_string = re.sub(r'^\s*//.*?\n', '', cleaned_string, flags=re.MULTILINE)
        cleaned_string = re.sub(r'/\*.*?\*/', '', cleaned_string, flags=re.DOTALL)

        # Try to find the first valid JSON object or array
        start_index = cleaned_string.find('{') if '{' in cleaned_string else cleaned_string.find('[')
        end_index = cleaned_string.rfind('}') if '}' in cleaned_string else cleaned_string.rfind(']')
        if start_index == -1 or end_index == -1 or end_index <= start_index:
            logging.error(f"JSON-LD structure missing {{}} or [] for {detail_url}")
            logging.debug(f"Problematic JSON-LD content (first 500 chars): {cleaned_string[:500]}")
            continue
        json_string = cleaned_string[start_index:end_index + 1]

        try:
            json_data = json.loads(json_string)
            # Handle both single object and array cases
            if isinstance(json_data, list):
                # Search through array for MedicalClinic object
                for item in json_data:
                    if isinstance(item, dict) and item.get("@type") == "MedicalClinic" and "telephone" in item:
                        data["phone"] = item.get("telephone", "N/A")
                        logging.info(f"Found phone in JSON-LD array: {data['phone']}")
                        break
                else:
                    continue
            elif isinstance(json_data, dict) and json_data.get("@type") == "MedicalClinic" and "telephone" in json_data:
                data["phone"] = json_data.get("telephone", "N/A")
                logging.info(f"Found phone in JSON-LD object: {data['phone']}")
            if data["phone"] != "N/A":
                break
        except json.JSONDecodeError as e:
            logging.error(f"JSON Decode Failed for {detail_url}. Error Detail: {e}")
            logging.debug(f"Problematic JSON-LD content (first 500 chars): {json_string[:500]}")
        except Exception as e:
            logging.error(f"Unexpected error parsing JSON-LD for {detail_url}: {e}")
            logging.debug(f"Problematic JSON-LD content (first 500 chars): {json_string[:500]}")
    else:
        # Fallback: Search for phone number pattern in div, span, or a
        phone_pattern = re.compile(r"\(?\d{3}\)?[\s-]*\d{3}-\d{4}")
        phone_elements = soup.find_all(["div", "span", "a"], string=phone_pattern)
        if phone_elements:
            data["phone"] = phone_elements[0].get_text(strip=True)
            logging.info(f"Found phone in HTML: {data['phone']}")
        else:
            # Last resort: Check for <div class="phone"> or tel: link
            phone = soup.find("div", class_="phone")
            if phone:
                data["phone"] = re.sub(r"[^\d\s\-\(\)+]", "", phone.get_text(strip=True))
                logging.info(f"Found phone in <div class='phone'>: {data['phone']}")
            else:
                ph = soup.select_one("a[href^='tel:']")
                data["phone"] = ph.get_text(strip=True) if ph else "N/A"
                if data["phone"] != "N/A":
                    logging.info(f"Found phone in tel: link: {data['phone']}")

    # Address (unchanged)
    address = soup.find("div", class_="address")
    data["address"] = address.get_text(strip=True) if address else "N/A"

    # Rating & reviews (unchanged)
    reviews = soup.find("span", class_="reviews")
    if reviews:
        # Rating: first direct child span
        rating_tag = reviews.find("span", recursive=False)
        data["rating"] = rating_tag.get_text(strip=True) if rating_tag and rating_tag.get_text(strip=True).replace(".", "").isdigit() else "N/A"

        # Review count: find the span nested under the second direct child span
        review_container = reviews.find_all("span", recursive=False)[1] if len(reviews.find_all("span", recursive=False)) > 1 else None
        if review_container:
            review_count_tag = review_container.find("span", string=re.compile(r"^\d+$"))
            data["review_count"] = review_count_tag.get_text(strip=True) if review_count_tag else "N/A"
        else:
            data["review_count"] = "N/A"
    else:
        data["rating"] = "N/A"
        data["review_count"] = "N/A"

    # Small polite delay
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

    # find list items - adjust selectors if site changes
    for item in soup.find_all("div", class_="search-item"):
        a = item.find("a", class_="search-item-location-link")
        if not a or "href" not in a.attrs:
            continue
        href = a["href"]
        # name
        name_tag = a.find("h2", class_="search-item-info search-item-location-name")
        name = name_tag.get_text(strip=True) if name_tag else a.get_text(strip=True) or "Unnamed Clinic"
        detail_url = BASE_URL + href
        logging.info(f"Found clinic: {name} -> {detail_url}")

        detail_data = scrape_detail(detail_url)
        clinics.append({
            "name": name,
            "detail_url": detail_url,
            "phone": detail_data.get("phone", "N/A"),
            "address": detail_data.get("address", "N/A"),
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
    logging.debug(f"Listing page HTML (first 1000 chars):\n{html[:1000]}")
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

def test_scrape_page(page_number: int):
    # 调用 scrape_page 函数来爬取页面内容
    clinics = scrape_page(page_number)
    # 保存结果到文件
    with open(f'test_page_{page_number}.json', 'w', encoding='utf-8') as f:
        json.dump(clinics, f, ensure_ascii=False, indent=2)

# -------------------------
# Main orchestration
# -------------------------
def main():
    logging.info("Starting cloudscraper-based crawler.")
    try:
        r = scraper.get(BASE_URL + "/robots.txt", timeout=8)
        if r.status_code == 200:
            logging.info("Robots.txt fetched (first 500 chars):\n" + r.text[:500])
        else:
            logging.info("Could not retrieve robots.txt or non-200 status.")
    except Exception as e:
        logging.info(f"Could not fetch robots.txt: {e}")

    total_pages = get_total_pages()
    if total_pages <= 0:
        logging.error("Could not determine total pages. Exiting.")
        return

    all_clinics = []
    for p in range(1, total_pages + 1):
        logging.info(f"Scraping list page {p}/{total_pages}")
        clinics = scrape_page(p)
        if not clinics:
            logging.warning(f"No clinics scraped from page {p}. Continuing.")
        all_clinics.extend(clinics)
        time.sleep(random.uniform(2.0, 5.0))

    with open(OUTPUT_FILE, "w", encoding="utf-8") as f:
        json.dump(all_clinics, f, ensure_ascii=False, indent=2)

    logging.info(f"Finished. Total clinics collected: {len(all_clinics)}. Saved to {OUTPUT_FILE}.")

if __name__ == "__main__":
    test_scrape_page(1)