#!/usr/bin/env python3
"""
NaviCare Cortico API Crawler
Scrapes clinic data from Cortico Health API and transforms it for NaviCare database
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
from data_transformer import CorticoTransformer, DataValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@dataclass
class CrawlConfig:
    """Configuration for the crawler"""
    base_url: str = "http://cerebro-release.cortico.ca/api/collected-clinics-public/"
    batch_size: int = 50  # Smaller batches for Supabase
    max_concurrent: int = 3  # Conservative for Supabase API limits
    delay_between_requests: float = 1.0  # seconds
    max_retries: int = 3
    cleanup_old_observations: bool = True

class CorticoCrawler:
    def __init__(self, config: CrawlConfig):
        self.config = config
        self.db_client = None
        self.session = None
        self.stats = {
            'total_processed': 0,
            'facilities_created': 0,
            'facilities_updated': 0,
            'observations_created': 0,
            'service_offerings_created': 0,
            'booking_channels_created': 0,
            'availability_records_created': 0,
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
        
        logger.info("Crawler initialized successfully")
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        """Async context manager exit"""
        if self.session:
            await self.session.close()
        
        # Print final statistics
        await self._print_final_stats()
        
        logger.info(f"Crawler shutdown. Final stats: {self.stats}")

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

    async def process_facility(self, cortico_record: Dict):
        """Process a single facility record"""
        try:
            # Transform facility data
            facility_data = CorticoTransformer.transform_facility(cortico_record)
            
            # Validate facility data
            is_valid, validation_errors = DataValidator.validate_facility(facility_data)
            if not is_valid:
                logger.warning(f"Validation failed for facility {facility_data.get('name')}: {validation_errors}")
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
            
            # Create observation
            observation_data = CorticoTransformer.transform_observation(facility_id, cortico_record)
            is_valid, validation_errors = DataValidator.validate_observation(observation_data)
            
            if is_valid:
                await self.db_client.insert_observation(observation_data)
                self.stats['observations_created'] += 1
            else:
                logger.warning(f"Observation validation failed: {validation_errors}")
                self.stats['validation_errors'] += 1
            
            # Process booking channels
            booking_channels = CorticoTransformer.transform_booking_channels(facility_id, cortico_record)
            for channel in booking_channels:
                if await self.db_client.insert_booking_channel(channel):
                    self.stats['booking_channels_created'] += 1
            
            # Process specialties
            specialties = cortico_record.get('specialties', [])
            if specialties:
                await self.db_client.link_facility_specialties(facility_id, specialties)
            
            # Process service offerings
            await self.process_service_offerings(facility_id, cortico_record.get('workflows', []))
            
            # Process availability data
            await self.process_availability(facility_id, cortico_record.get('availability', {}))
            
            self.stats['total_processed'] += 1
            
        except Exception as e:
            logger.error(f"Error processing facility {cortico_record.get('clinic_name', 'Unknown')}: {e}")
            self.stats['errors'] += 1

    async def process_service_offerings(self, facility_id: str, workflows: List[Dict]):
        """Process service offerings for a facility"""
        service_offerings = CorticoTransformer.transform_service_offerings(facility_id, workflows)
        
        for offering in service_offerings:
            try:
                # Resolve service slug to service ID
                service = await self.db_client.get_service_by_slug(offering['service_slug'])
                if not service:
                    # create a new service if not found
                    service_data = {
                        'slug': offering['service_slug'],
                        'display_name': offering.get('display_name', ''),
                        'category': offering.get('workflow_type', '')
                    }
                    service_id = await self.db_client.create_service(service_data)
                    if not service_id:
                        logger.warning(f"Failed to create service for slug: {offering['service_slug']}")
                        continue
                    service = {'id': service_id}

                # Replace service_slug with service_id
                offering_data = {k: v for k, v in offering.items() if k not in ('service_slug', 'display_name', 'workflow_type')}
                offering_data['service_id'] = service['id']
                
                if await self.db_client.upsert_facility_service_offering(offering_data):
                    self.stats['service_offerings_created'] += 1
                    
            except Exception as e:
                logger.error(f"Error processing service offering: {e}")

    async def process_availability(self, facility_id: str, availability_data: Dict):
        """Process availability data for a facility"""
        availability_records = CorticoTransformer.transform_availability(facility_id, availability_data)
        if not availability_records:
            return
        try:
            if await self.db_client.insert_availability(availability_records):
                self.stats['availability_records_created'] += 1
        except Exception as e:
            logger.error(f"Error processing availability record: {e}")

    async def crawl_all(self):
        """Main crawling method - processes all pages"""
        logger.info("Starting Cortico API crawl")
        start_time = time.time()
        
        # Clean up old observations if enabled
        if self.config.cleanup_old_observations:
            deleted_count = await self.db_client.cleanup_old_observations(days_old=7)
            logger.info(f"Cleaned up {deleted_count} old observations")
        
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
            logger.info(f"Processing {len(results)} facilities from page {page_count}")
            
            # Process facilities in smaller batches to avoid overwhelming Supabase
            for i in range(0, len(results), self.config.batch_size):
                batch = results[i:i + self.config.batch_size]
                
                # Process batch concurrently but with limited concurrency
                semaphore = asyncio.Semaphore(self.config.max_concurrent)
                
                async def process_with_semaphore(record):
                    async with semaphore:
                        await self.process_facility(record)
                
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
        logger.info(f"Crawl completed in {elapsed:.2f} seconds")

    async def _print_final_stats(self):
        """Print comprehensive final statistics"""
        logger.info("=" * 60)
        logger.info("FINAL CRAWL STATISTICS")
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
                logger.info(f"Total Observations in DB: {db_stats.get('total_observations', 'Unknown')}")
                
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
        logger.info(f"Starting single page crawl (page {page_number})")
        
        url = f"{self.config.base_url}?format=json&page={page_number}"
        page_data = await self.fetch_page(url)
        
        if not page_data:
            logger.error(f"Failed to fetch page {page_number}")
            return
        
        results = page_data.get('results', [])
        logger.info(f"Processing {len(results)} facilities from page {page_number}")
        
        for record in results:
            await self.process_facility(record)
        
        logger.info(f"Single page crawl completed. Stats: {self.stats}")

async def main():
    """Main function for testing"""
    import os
    from dotenv import load_dotenv
    
    load_dotenv()
    
    # Verify environment variables
    if not os.getenv('SUPABASE_URL') or not os.getenv('SUPABASE_KEY'):
        logger.error("SUPABASE_URL and SUPABASE_KEY environment variables are required")
        return
    
    config = CrawlConfig(
        delay_between_requests=0.5,  # Be respectful to the API
        max_concurrent=2,  # Conservative for testing
        batch_size=25  # Small batches for testing
    )
    
    async with CorticoCrawler(config) as crawler:
        # For testing, crawl just one page
        await crawler.crawl_single_page(1)
        
        # For full crawl, use:
        # await crawler.crawl_all()

if __name__ == "__main__":
    asyncio.run(main())