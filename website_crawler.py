# website_crawler.py
"""
Website crawler for RateMDs facilities.
- Reads existing ratemd.json file
- Visits each facility's detail page to extract website URL
- Updates JSON with website information
- Uses cloudscraper to handle Cloudflare JS challenge
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
from urllib.parse import urlparse

import cloudscraper
from bs4 import BeautifulSoup
from requests import Response
from dotenv import load_dotenv

# Supabase client helper
sys.path.append(os.path.dirname(os.path.abspath(__file__)))
from utils.supabase_client import SupabaseClient

# -------------------------
# Logging configuration
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")

# -------------------------
# Config
# -------------------------
BASE_URL = "https://www.ratemds.com"
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

def extract_website_url(detail_url: str) -> str:
    """Extract website URL from facility detail page."""
    time.sleep(random.uniform(0.5, 1.5))
    resp = get_with_retries(detail_url, timeout=15)
    if not resp:
        logging.error(f"Failed to fetch detail page: {detail_url}")
        return "N/A"

    html = resp.text
    soup = BeautifulSoup(html, "html.parser")
    
    # Look for website link with specific pattern
    # Pattern: <a rel="nofollow" target="_blank" href="..." title="..."> Visit Website</a>
    website_links = soup.find_all("a", rel="nofollow", target="_blank")
    
    for link in website_links:
        href = link.get("href", "")
        title = link.get("title", "")
        text = link.get_text(strip=True)
        
        # Check if this looks like a website link
        if ("website" in title.lower() or "visit website" in text.lower()) and href:
            # Validate that href looks like a URL
            if href.startswith(("http://", "https://")):
                logging.info(f"Found website URL: {href}")
                return href
    
    # Alternative: look for any external links that might be websites
    external_links = soup.find_all("a", href=re.compile(r"^https?://"))
    for link in external_links:
        href = link.get("href", "")
        text = link.get_text(strip=True).lower()
        
        # Skip obvious non-website links
        if any(skip in href.lower() for skip in ["facebook.com", "twitter.com", "linkedin.com", "instagram.com", "youtube.com"]):
            continue
            
        # If it looks like a main website link
        if len(href) > 10 and not any(social in href.lower() for social in ["facebook", "twitter", "linkedin", "instagram", "youtube"]):
            logging.info(f"Found potential website URL: {href}")
            return href
    
    logging.info(f"No website found for {detail_url}")
    return "N/A"

def generate_slug_from_url(detail_url: str) -> Optional[str]:
    """Generate a slug from the RateMDs detail URL.
    Uses the last non-empty path segment when available.
    """
    if not detail_url:
        return None
    try:
        parsed = urlparse(detail_url)
        parts = [p for p in parsed.path.strip('/').split('/') if p]
        if parts:
            return parts[-1]
    except Exception:
        return None
    return None

def update_facility_website(supabase_client: SupabaseClient, website_url: str, facility: Dict) -> bool:
    """Update the facility website in Supabase.

    Matching priority:
    1) slug derived from detail_url
    2) name + city + province (if available)
    """
    if not website_url or website_url == "N/A":
        return False

    try:
        client = supabase_client.client

        detail_url = facility.get("detail_url") or ""
        slug = generate_slug_from_url(detail_url)

        update_data = {"website": website_url}

        # Try match by slug first
        if slug:
            resp = (
                client.table("facilities")
                .update(update_data)
                .eq("slug", slug)
                .execute()
            )
            # If updated at least one row, success
            if getattr(resp, "data", None):
                return True

        # Fallback: match by name + city + province if provided
        name = facility.get("name")
        city = facility.get("city")
        province = facility.get("province")
        if name and city and province:
            resp2 = (
                client.table("facilities")
                .update(update_data)
                .eq("name", name)
                .eq("city", city)
                .eq("province", province)
                .execute()
            )
            if getattr(resp2, "data", None):
                return True

        logging.warning(f"No matching facility found for update (slug={slug}, name={name}, city={city}, province={province})")
        return False
    except Exception as e:
        logging.error(f"Error updating facility website: {e}")
        return False

def load_facilities(filename: str) -> List[Dict]:
    """Load facilities from JSON file."""
    try:
        with open(filename, "r", encoding="utf-8") as f:
            facilities = json.load(f)
        logging.info(f"Loaded {len(facilities)} facilities from {filename}")
        return facilities
    except Exception as e:
        logging.error(f"Error loading {filename}: {e}")
        return []

def save_facilities(facilities: List[Dict], filename: str):
    """Save facilities to JSON file."""
    try:
        with open(filename, "w", encoding="utf-8") as f:
            json.dump(facilities, f, ensure_ascii=False, indent=2)
        logging.info(f"Saved {len(facilities)} facilities to {filename}")
    except Exception as e:
        logging.error(f"Error saving {filename}: {e}")

def main():
    """Main function to crawl website URLs and update Supabase facilities table."""
    # Load environment variables (SUPABASE_URL, SUPABASE_KEY)
    load_dotenv()

    # Ensure Supabase env is set
    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_KEY"):
        logging.error("SUPABASE_URL and SUPABASE_KEY environment variables are required")
        return

    # Initialize Supabase client
    supabase_client = SupabaseClient()

    input_file = "ratemd.json"

    # Load existing facilities from JSON (authoritative list for crawling)
    facilities = load_facilities(input_file)
    if not facilities:
        logging.error("No facilities loaded. Exiting.")
        return

    # Process each facility
    total_facilities = len(facilities)
    updated_count = 0
    for i, facility in enumerate(facilities, 1):
        facility_name = facility.get("name", "Unknown")
        detail_url = facility.get("detail_url", "")

        if not detail_url:
            logging.warning(f"Facility {facility_name} has no detail URL. Skipping.")
            continue

        logging.info(f"Processing facility {i}/{total_facilities}: {facility_name}")

        # Extract website URL
        website_url = extract_website_url(detail_url)

        # Update Supabase
        if update_facility_website(supabase_client, website_url, facility):
            updated_count += 1
            logging.info(f"Updated website for: {facility_name}")
        else:
            logging.info(f"No update applied for: {facility_name}")

        # Add delay between requests
        time.sleep(random.uniform(2.0, 5.0))

        # Progress log every 50 facilities
        if i % 50 == 0:
            logging.info(f"Progress: {i}/{total_facilities} processed, {updated_count} updated in DB")

    logging.info(f"Completed processing all {total_facilities} facilities. {updated_count} rows updated in DB")

if __name__ == "__main__":
    main()
