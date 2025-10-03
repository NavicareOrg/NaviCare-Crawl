#!/usr/bin/env python3
"""
NaviCare Pharmacy Crawler
Scrapes pharmacy data from Cortico Health API and transforms it for NaviCare database
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import time
from urllib.parse import urljoin

from supabase_client import SupabaseClient
from data_transformer import DataValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class PharmacyCrawlConfig:
    """Configuration for the pharmacy crawler"""
    base_url: str = "http://cerebro-release.cortico.ca/api/summary/pharmacies/"
    batch_size: int = 50  # Smaller batches for Supabase
    max_concurrent: int = 3  # Conservative for Supabase API limits
    delay_between_requests: float = 1.0  # seconds
    max_retries: int = 3

class PharmacyTransformer:
    """Transforms Pharmacy API data to NaviCare format"""
    
    @staticmethod
    def transform_pharmacy(pharmacy_data: Dict) -> Dict:
        """Transform Pharmacy API data to NaviCare facility format"""
        # Defensive: if the source payload is not a dict, return safe defaults
        if not isinstance(pharmacy_data, dict):
            logger.warning(f"transform_pharmacy received non-dict pharmacy_data: {type(pharmacy_data)}. Returning empty facility data.")
            return {
                'name': '',
                'slug': '',
                'facility_type': 'pharmacy',
                'website': None,
                'email': None,
                'phone': None,
                'address_line1': None,
                'city': None,
                'province': None,
                'country': None,
                'longitude': None,
                'latitude': None,
                'accepts_new_patients': False,
                'is_bookable_online': False,
                'has_telehealth': False,
                'status': 'active'
            }

        # Generate slug from pharmacy name
        pharmacy_name = pharmacy_data.get('name', '')
        slug = pharmacy_data.get('slug') or PharmacyTransformer._generate_slug(pharmacy_name)
        
        # Extract coordinates
        longitude = pharmacy_data.get('longitude')
        latitude = pharmacy_data.get('latitude')
        
        # Extract delivery information
        is_delivery_pharmacy = pharmacy_data.get('is_delivery_pharmacy', False)
        
        return {
            'name': pharmacy_name.strip() if pharmacy_name else '',
            'slug': slug,
            'facility_type': 'pharmacy',
            'website': pharmacy_data.get('website'),
            'email': pharmacy_data.get('email'),
            'phone': PharmacyTransformer._clean_phone(pharmacy_data.get('phone_number')),
            'address_line1': pharmacy_data.get('address', ''),
            'city': pharmacy_data.get('city', ''),
            'province': pharmacy_data.get('province', ''),
            'country': pharmacy_data.get('country', 'Canada'),
            'longitude': longitude,
            'latitude': latitude,
            'accepts_new_patients': False,  # Pharmacies typically don't accept new patients directly
            'is_bookable_online': bool(pharmacy_data.get('website')),
            'has_telehealth': False,  # Pharmacies typically don't offer telehealth
            'status': 'active'
        }

    @staticmethod
    def transform_booking_channels(facility_id: str, pharmacy_data: Dict) -> List[Dict]:
        """Transform Pharmacy booking data to booking channels"""
        channels = []
        
        # Add website as a booking channel if available
        website = pharmacy_data.get('website')
        if website:
            channels.append({
                'facility_id': facility_id,
                'channel_type': 'web',
                'label': 'Pharmacy Website',
                'url': website,
                'external_provider': 'cortico',
                'is_active': True,
                'last_checked_at': datetime.now(timezone.utc).isoformat()
            })
        
        # Add phone as a booking channel if available
        phone = pharmacy_data.get('phone_number')
        if phone:
            channels.append({
                'facility_id': facility_id,
                'channel_type': 'phone',
                'label': 'Phone Contact',
                'phone': PharmacyTransformer._clean_phone(phone),
                'is_active': True,
                'last_checked_at': datetime.now(timezone.utc).isoformat()
            })
        
        # Add email as a booking channel if available
        email = pharmacy_data.get('email')
        if email:
            channels.append({
                'facility_id': facility_id,
                'channel_type': 'email',
                'label': 'Email Contact',
                'email': email,
                'is_active': True,
                'last_checked_at': datetime.now(timezone.utc).isoformat()
            })
        
        return channels

    @staticmethod
    def transform_operating_hours(facility_id: str, operating_hours: Optional[Dict]) -> List[Dict]:
        """Transform operating hours map to facility hours records"""
        from data_transformer import CorticoTransformer  # Import for helper methods
        return CorticoTransformer.transform_operating_hours(facility_id, operating_hours)

    @staticmethod
    def _generate_slug(name: str) -> str:
        """Generate URL-friendly slug from facility name"""
        import re
        if not name:
            return f"pharmacy-{datetime.now().timestamp()}"
        
        slug = re.sub(r'[^\w\s-]', '', name.lower())
        slug = re.sub(r'[-\s]+', '-', slug)
        return slug.strip('-')

    @staticmethod
    def _clean_phone(phone: str) -> Optional[str]:
        """Clean and format phone number"""
        if not phone:
            return None
        
        # Remove all non-digit characters
        digits_only = ''.join(filter(str.isdigit, phone))
        
        # Validate length (assuming North American format)
        if len(digits_only) == 10:
            return f"({digits_only[:3]}) {digits_only[3:6]}-{digits_only[6:]}"
        elif len(digits_only) == 11 and digits_only[0] == '1':
            return f"({digits_only[1:4]}) {digits_only[4:7]}-{digits_only[7:]}"
        
        return phone  # Return original if can't format


class PharmacyCrawler:
    def __init__(self, config: PharmacyCrawlConfig):
        self.config = config
        self.db_client = None
        self.session = None
        self.stats = {
            'total_processed': 0,
            'facilities_created': 0,
            'facilities_updated': 0,
            'booking_channels_created': 0,
            'facility_hours_records_created': 0,
            'errors': 0,
            'validation_errors': 0
        }

    async def __aenter__(self):
        """Async context manager entry"""
        # Initialize Supabase client
        self.db_client = SupabaseClient()
        
        # Test connection
        if not await self.db_client.test_connection():
            raise Exception("Failed to connect to Supabase")
        
        # Create HTTP session
        timeout = aiohttp.ClientTimeout(total=30)
        connector = aiohttp.TCPConnector(limit=self.config.max_concurrent)
        self.session = aiohttp.ClientSession(timeout=timeout, connector=connector)
        
        logger.info("Pharmacy Crawler initialized successfully")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
        
        # Print final statistics
        await self._print_final_stats()
        
        logger.info(f"Pharmacy Crawler shutdown. Final stats: {self.stats}")

    async def fetch_page(self, page_url: str) -> Optional[Dict]:
        """Fetch a single page from the API with retry logic"""
        for attempt in range(self.config.max_retries):
            try:
                async with self.session.get(page_url) as response:
                    if response.status == 200:
                        data = await response.json()
                        return data
                    elif response.status == 429:
                        # Rate limited, wait longer
                        wait_time = (attempt + 1) * 2
                        logger.warning(f"Rate limited, waiting {wait_time}s before retry")
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        logger.error(f"HTTP {response.status} for URL: {page_url}")
                        if attempt < self.config.max_retries - 1:
                            await asyncio.sleep(1)
                        continue
            except Exception as e:
                logger.error(f"Error fetching {page_url} (attempt {attempt + 1}): {e}")
                if attempt < self.config.max_retries - 1:
                    await asyncio.sleep(1)
                continue
        
        self.stats['errors'] += 1
        return None

    async def process_pharmacy(self, pharmacy_record: Dict):
        """Process a single pharmacy record"""
        try:
            # Transform pharmacy data
            facility_data = PharmacyTransformer.transform_pharmacy(pharmacy_record)
            
            # Validate facility data
            is_valid, validation_errors = DataValidator.validate_facility(facility_data)
            if not is_valid:
                logger.warning(f"Validation failed for pharmacy {facility_data.get('name')}: {validation_errors}")
                self.stats['validation_errors'] += 1
                return
            
            # Check if this is a new or updated facility
            existing = await self.db_client.find_existing_facility(
                facility_data.get('slug', ''),
                facility_data.get('name', ''),
                facility_data.get('city', ''),
                facility_data.get('province', '')
            )
            
            # Upsert facility
            facility_id = await self.db_client.upsert_facility(facility_data)
            
            if existing:
                self.stats['facilities_updated'] += 1
            else:
                self.stats['facilities_created'] += 1
            
            # Process booking channels
            booking_channels = PharmacyTransformer.transform_booking_channels(facility_id, pharmacy_record)
            for channel in booking_channels:
                if await self.db_client.insert_booking_channel(channel):
                    self.stats['booking_channels_created'] += 1
            
            # Process operating hours
            await self.process_facility_hours(facility_id, pharmacy_record.get('operating_hours'))
            
            self.stats['total_processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing pharmacy {pharmacy_record.get('name', 'Unknown')}: {e}")
            self.stats['errors'] += 1

    async def process_facility_hours(self, facility_id: str, operating_hours: Optional[Dict]):
        """Process operating hours for a facility"""
        try:
            hour_records = PharmacyTransformer.transform_operating_hours(facility_id, operating_hours)

            if await self.db_client.replace_facility_hours(facility_id, hour_records):
                self.stats['facility_hours_records_created'] += len(hour_records)
            else:
                logger.warning(f"Failed to upsert operating hours for facility {facility_id}")

        except Exception as e:
            logger.error(f"Error processing operating hours for facility {facility_id}: {e}")

    async def crawl_page_range(self, start_page: int, end_page: int):
        """Crawl a specific range of pages"""
        logger.info(f"Starting Pharmacy API crawl for pages {start_page} to {end_page}")
        start_time = time.time()
        
        processed_pages = 0
        
        # Process each page in the range
        for page_number in range(start_page, end_page + 1):
            page_url = f"{self.config.base_url}?format=json&page={page_number}"
            logger.info(f"Fetching page {page_number}: {page_url}")
            
            # Fetch page data
            page_data = await self.fetch_page(page_url)
            if not page_data:
                logger.error(f"Failed to fetch page {page_number}, skipping")
                continue
            
            # Process all records in this page
            results = page_data.get('results', [])
            logger.info(f"Processing {len(results)} pharmacies from page {page_number}")
            
            # Process pharmacies in smaller batches to avoid overwhelming Supabase
            for i in range(0, len(results), self.config.batch_size):
                batch = results[i:i + self.config.batch_size]
                
                # Process batch concurrently but with limited concurrency
                semaphore = asyncio.Semaphore(self.config.max_concurrent)
                
                async def process_with_semaphore(record):
                    async with semaphore:
                        await self.process_pharmacy(record)
                
                tasks = [process_with_semaphore(record) for record in batch]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                # Progress logging
                if self.stats['total_processed'] % 50 == 0:
                    logger.info(f"Progress: {self.stats['total_processed']} processed, "
                              f"{self.stats['facilities_created']} created, "
                              f"{self.stats['facilities_updated']} updated, "
                              f"{self.stats['errors']} errors")
                
                # Rate limiting between batches
                if self.config.delay_between_requests > 0:
                    await asyncio.sleep(self.config.delay_between_requests)
            
            processed_pages += 1
            logger.info(f"Completed page {page_number}")
            
            # Add a small delay between pages
            await asyncio.sleep(self.config.delay_between_requests)
        
        elapsed = time.time() - start_time
        logger.info(f"Pharmacy crawl completed {processed_pages} pages in {elapsed:.2f} seconds")

    async def crawl_all(self):
        """Main crawling method - processes all pages"""
        logger.info("Starting Pharmacy API crawl")
        start_time = time.time()
        
        current_url = f"{self.config.base_url}?format=json"
        page_count = 0
        
        while current_url:
            page_count += 1
            logger.info(f"Fetching page {page_count}: {current_url}")
            
            # Fetch page data
            page_data = await self.fetch_page(current_url)
            if not page_data:
                logger.error(f"Failed to fetch page {page_count}, stopping crawl")
                break
            
            # Process all records in this page
            results = page_data.get('results', [])
            logger.info(f"Processing {len(results)} pharmacies from page {page_count}")
            
            # Process pharmacies in smaller batches to avoid overwhelming Supabase
            for i in range(0, len(results), self.config.batch_size):
                batch = results[i:i + self.config.batch_size]
                
                # Process batch concurrently but with limited concurrency
                semaphore = asyncio.Semaphore(self.config.max_concurrent)
                
                async def process_with_semaphore(record):
                    async with semaphore:
                        await self.process_pharmacy(record)
                
                tasks = [process_with_semaphore(record) for record in batch]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                # Progress logging
                if self.stats['total_processed'] % 50 == 0:
                    logger.info(f"Progress: {self.stats['total_processed']} processed, "
                              f"{self.stats['facilities_created']} created, "
                              f"{self.stats['facilities_updated']} updated, "
                              f"{self.stats['errors']} errors")
                
                # Rate limiting between batches
                if self.config.delay_between_requests > 0:
                    await asyncio.sleep(self.config.delay_between_requests)
            
            # Move to next page
            links = page_data.get('links', {})
            current_url = links.get('next')
            
            # Log page completion
            total_pages = page_data.get('total_pages', 'unknown')
            logger.info(f"Completed page {page_count} of {total_pages}")
        
        elapsed = time.time() - start_time
        logger.info(f"Pharmacy crawl completed in {elapsed:.2f} seconds")

    async def _print_final_stats(self):
        """Print comprehensive final statistics"""
        logger.info("=" * 60)
        logger.info("FINAL PHARMACY CRAWL STATISTICS")
        logger.info("=" * 60)
        
        # Print our internal stats
        for key, value in self.stats.items():
            logger.info(f"{key.replace('_', ' ').title()}: {value}")
        
        # Get database stats
        try:
            db_stats = await self.db_client.get_facility_stats()
            if db_stats:
                logger.info("\nDATABASE STATISTICS")
                logger.info("-" * 30)
                logger.info(f"Total Facilities in DB: {db_stats.get('total_facilities', 'Unknown')}")
                
                facility_types = db_stats.get('facility_types', {})
                if facility_types:
                    logger.info("\nFacilities by Type:")
                    for facility_type, count in sorted(facility_types.items()):
                        logger.info(f"  {facility_type}: {count}")
        
        except Exception as e:
            logger.error(f"Error fetching database stats: {e}")
        
        logger.info("=" * 60)

    async def crawl_single_page(self, page_number: int = 1):
        """Crawl a single page for testing purposes"""
        logger.info(f"Starting single page pharmacy crawl (page {page_number})")
        
        url = f"{self.config.base_url}?format=json&page={page_number}"
        page_data = await self.fetch_page(url)
        
        if not page_data:
            logger.error(f"Failed to fetch page {page_number}")
            return
        
        results = page_data.get('results', [])
        logger.info(f"Processing {len(results)} pharmacies from page {page_number}")
        
        for record in results:
            await self.process_pharmacy(record)
        
        logger.info(f"Single page pharmacy crawl completed. Stats: {self.stats}")

async def main():
    """Main function for testing"""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Verify environment variables
    if not os.getenv('SUPABASE_URL') or not os.getenv('SUPABASE_KEY'):
        logger.error("SUPABASE_URL and SUPABASE_KEY environment variables are required")
        return
    
    config = PharmacyCrawlConfig(
        delay_between_requests=0.5,  # Be respectful to the API
        max_concurrent=2,  # Conservative for testing
        batch_size=25  # Small batches for testing
    )
    
    async with PharmacyCrawler(config) as crawler:
        # For testing, crawl just one page
        await crawler.crawl_single_page(page_number=1)
        
        # For full crawl, use:
        # await crawler.crawl_all()

if __name__ == "__main__":
    asyncio.run(main())