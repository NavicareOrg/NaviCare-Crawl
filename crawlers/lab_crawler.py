#!/usr/bin/env python3
"""
NaviCare Lab Crawler
Scrapes laboratory data from Cortico Health API and transforms it for NaviCare database
"""

import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
import time
from urllib.parse import urljoin

from utils.supabase_client import SupabaseClient
from utils.data_transformer import DataValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class LabCrawlConfig:
    """Configuration for the lab crawler"""
    base_url: str = "http://cerebro-release.cortico.ca/api/laboratories/"
    batch_size: int = 50  # Smaller batches for Supabase
    max_concurrent: int = 3  # Conservative for Supabase API limits
    delay_between_requests: float = 1.0  # seconds
    max_retries: int = 3

class LabTransformer:
    """Transforms Lab API data to NaviCare format"""
    
    @staticmethod
    def transform_lab(lab_data: Dict) -> Dict:
        """Transform Lab API data to NaviCare facility format"""
        # Defensive: if the source payload is not a dict, return safe defaults
        if not isinstance(lab_data, dict):
            logger.warning(f"transform_lab received non-dict lab_data: {type(lab_data)}. Returning empty facility data.")
            return {
                'name': '',
                'slug': '',
                'facility_type': 'laboratory',
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

        # Generate slug from lab name
        lab_name = lab_data.get('name', '')
        slug = lab_data.get('slug') or LabTransformer._generate_slug(lab_name)
        
        # Extract coordinates
        longitude = lab_data.get('longitude')
        latitude = lab_data.get('latitude')
        
        # Extract metadata
        metadata = lab_data.get('metadata', {})
        
        return {
            'name': lab_name.strip() if lab_name else '',
            'slug': slug,
            'facility_type': 'laboratory',
            'website': metadata.get('website') or lab_data.get('website'),
            'email': lab_data.get('email'),
            'phone': LabTransformer._clean_phone(lab_data.get('phone_number')),
            'address_line1': lab_data.get('address', ''),
            'city': lab_data.get('city', ''),
            'province': lab_data.get('province', ''),
            'country': lab_data.get('country', 'Canada'),
            'longitude': longitude,
            'latitude': latitude,
            'accepts_new_patients': False,  # Labs typically don't accept new patients directly
            'is_bookable_online': bool(metadata.get('website')),
            'has_telehealth': False,  # Labs typically don't offer telehealth
            'status': 'active'
        }

    @staticmethod
    def transform_booking_channels(facility_id: str, lab_data: Dict) -> List[Dict]:
        """Transform Lab booking data to booking channels"""
        channels = []
        
        # Add website as a booking channel if available
        metadata = lab_data.get('metadata', {})
        website = metadata.get('website') or lab_data.get('website')
        if website:
            channels.append({
                'facility_id': facility_id,
                'channel_type': 'web',
                'label': 'Lab Website',
                'url': website,
                'external_provider': 'cortico',
                'is_active': True,
                'last_checked_at': datetime.now(timezone.utc).isoformat()
            })
        
        # Add phone as a booking channel if available
        phone = lab_data.get('phone_number')
        if phone:
            channels.append({
                'facility_id': facility_id,
                'channel_type': 'phone',
                'label': 'Phone Contact',
                'phone': LabTransformer._clean_phone(phone),
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
            return f"lab-{datetime.now().timestamp()}"
        
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


class LabCrawler:
    def __init__(self, config: LabCrawlConfig):
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
        
        logger.info("Lab Crawler initialized successfully")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
        
        # Print final statistics
        await self._print_final_stats()
        
        logger.info(f"Lab Crawler shutdown. Final stats: {self.stats}")

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

    async def process_lab(self, lab_record: Dict):
        """Process a single lab record"""
        try:
            # Transform lab data
            facility_data = LabTransformer.transform_lab(lab_record)
            
            # Validate facility data
            is_valid, validation_errors = DataValidator.validate_facility(facility_data)
            if not is_valid:
                logger.warning(f"Validation failed for lab {facility_data.get('name')}: {validation_errors}")
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
            booking_channels = LabTransformer.transform_booking_channels(facility_id, lab_record)
            for channel in booking_channels:
                if await self.db_client.insert_booking_channel(channel):
                    self.stats['booking_channels_created'] += 1
            
            # Process specialties
            specialties = lab_record.get('specialties', [])
            if specialties:
                await self.db_client.link_facility_specialties(facility_id, specialties)
            
            # Process operating hours
            await self.process_facility_hours(facility_id, lab_record.get('operating_hours'))
            
            self.stats['total_processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing lab {lab_record.get('name', 'Unknown')}: {e}")
            self.stats['errors'] += 1

    async def process_facility_hours(self, facility_id: str, operating_hours: Optional[Dict]):
        """Process operating hours for a facility"""
        try:
            hour_records = LabTransformer.transform_operating_hours(facility_id, operating_hours)

            if await self.db_client.replace_facility_hours(facility_id, hour_records):
                self.stats['facility_hours_records_created'] += len(hour_records)
            else:
                logger.warning(f"Failed to upsert operating hours for facility {facility_id}")

        except Exception as e:
            logger.error(f"Error processing operating hours for facility {facility_id}: {e}")

    async def crawl_page_range(self, start_page: int, end_page: int):
        """Crawl a specific range of pages"""
        logger.info(f"Starting Lab API crawl for pages {start_page} to {end_page}")
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
            logger.info(f"Processing {len(results)} labs from page {page_number}")
            
            # Process labs in smaller batches to avoid overwhelming Supabase
            for i in range(0, len(results), self.config.batch_size):
                batch = results[i:i + self.config.batch_size]
                
                # Process batch concurrently but with limited concurrency
                semaphore = asyncio.Semaphore(self.config.max_concurrent)
                
                async def process_with_semaphore(record):
                    async with semaphore:
                        await self.process_lab(record)
                
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
        logger.info(f"Lab crawl completed {processed_pages} pages in {elapsed:.2f} seconds")

    async def crawl_all(self):
        """Main crawling method - processes all pages"""
        logger.info("Starting Lab API crawl")
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
            logger.info(f"Processing {len(results)} labs from page {page_count}")
            
            # Process labs in smaller batches to avoid overwhelming Supabase
            for i in range(0, len(results), self.config.batch_size):
                batch = results[i:i + self.config.batch_size]
                
                # Process batch concurrently but with limited concurrency
                semaphore = asyncio.Semaphore(self.config.max_concurrent)
                
                async def process_with_semaphore(record):
                    async with semaphore:
                        await self.process_lab(record)
                
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
        logger.info(f"Lab crawl completed in {elapsed:.2f} seconds")

    async def _print_final_stats(self):
        """Print comprehensive final statistics"""
        logger.info("=" * 60)
        logger.info("FINAL LAB CRAWL STATISTICS")
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
        logger.info(f"Starting single page lab crawl (page {page_number})")
        
        url = f"{self.config.base_url}?format=json&page={page_number}"
        page_data = await self.fetch_page(url)
        
        if not page_data:
            logger.error(f"Failed to fetch page {page_number}")
            return
        
        results = page_data.get('results', [])
        logger.info(f"Processing {len(results)} labs from page {page_number}")
        
        for record in results:
            await self.process_lab(record)
        
        logger.info(f"Single page lab crawl completed. Stats: {self.stats}")

async def main():
    """Main function for testing"""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Verify environment variables
    if not os.getenv('SUPABASE_URL') or not os.getenv('SUPABASE_KEY'):
        logger.error("SUPABASE_URL and SUPABASE_KEY environment variables are required")
        return
    
    config = LabCrawlConfig(
        delay_between_requests=0.5,  # Be respectful to the API
        max_concurrent=2,  # Conservative for testing
        batch_size=25  # Small batches for testing
    )
    
    async with LabCrawler(config) as crawler:
        # For testing, crawl just one page
        await crawler.crawl_single_page(page_number=1)
        
        # For full crawl, use:
        # await crawler.crawl_all()

if __name__ == "__main__":
    asyncio.run(main())