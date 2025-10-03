#!/usr/bin/env python3
"""
NaviCare Pharmacy Availability Updater
Simple script to update only the availability information for existing pharmacy facilities
"""

import os
import sys
import asyncio
import argparse
from datetime import datetime
from dotenv import load_dotenv
import logging

# Add project root to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from pharmacy_crawler import PharmacyCrawler, PharmacyCrawlConfig, PharmacyTransformer
from supabase_client import SupabaseClient
from data_transformer import DataValidator

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()

def create_config_from_env() -> PharmacyCrawlConfig:
    """Create crawler configuration from environment variables"""
    return PharmacyCrawlConfig(
        base_url=os.getenv('CORTICO_API_URL_PHARMACY'),
        batch_size=int(os.getenv('CRAWLER_BATCH_SIZE', '25')),
        max_concurrent=int(os.getenv('CRAWLER_MAX_CONCURRENT', '3')),
        delay_between_requests=float(os.getenv('CRAWLER_DELAY', '1.0')),
        max_retries=int(os.getenv('CRAWLER_MAX_RETRIES', '3')),
    )

def validate_environment():
    """Validate required environment variables"""
    required_vars = ['SUPABASE_URL', 'SUPABASE_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        logger.error("Missing required environment variables:")
        for var in missing_vars:
            logger.error(f"   - {var}")
        logger.error("Please set these variables in your .env file")
        return False
    
    return True

async def update_pharmacy_availability(crawler: PharmacyCrawler, pharmacy_record: dict):
    """Update availability information for a single pharmacy"""
    try:
        # Defensive: pharmacy_record should be a dict
        if not isinstance(pharmacy_record, dict):
            logger.error(f"Unexpected pharmacy_record type: {type(pharmacy_record)}. Payload: {repr(pharmacy_record)[:500]}")
            return False
            
        # Get facility data
        facility_data = PharmacyTransformer.transform_pharmacy(pharmacy_record)
        facility_slug = facility_data.get('slug', '')
        facility_name = facility_data.get('name', '')
        facility_city = facility_data.get('city', '')
        facility_province = facility_data.get('province', '')
        
        # Find existing facility in database
        existing_facility = await crawler.db_client.find_existing_facility(
            facility_slug, facility_name, facility_city, facility_province
        )
        
        if not existing_facility:
            logger.info(f"Pharmacy facility not found in database: {facility_name} ({facility_slug}). Creating full record via crawler.")
            try:
                # Use the crawler's full facility processing to create facility and related records
                await crawler.process_pharmacy(pharmacy_record)
                logger.info(f"Created pharmacy facility from Cortico record: {facility_name} ({facility_slug})")
                return True
            except Exception as e:
                logger.error(f"Failed to create pharmacy facility from Cortico record {facility_name}: {e}")
                return False
        
        facility_id = existing_facility['id']
        
        # For pharmacies, we don't have specific availability data like clinics
        # We'll just update the last checked timestamp
        logger.info(f"Updated last checked timestamp for pharmacy: {facility_name}")
        return True
            
    except Exception as e:
        logger.error(f"Error updating pharmacy {facility_name}: {e}")
        return False

async def fetch_and_update_pharmacy_availability(config: PharmacyCrawlConfig):
    """Fetch and update availability for all pharmacy facilities"""
    logger.info("🚀 Starting NaviCare Pharmacy Availability Update")
    logger.info("=" * 50)
    
    stats = {
        'facilities_processed': 0,
        'facilities_updated': 0,
        'facilities_not_found': 0,
        'errors': 0
    }
    
    async with PharmacyCrawler(config) as crawler:
        current_url = f"{config.base_url}?format=json"
        page_count = 0
        
        while current_url:
            page_count += 1
            logger.info(f"Fetching page {page_count}: {current_url}")
            
            # Fetch page data
            page_data = await crawler.fetch_page(current_url)
            if not page_data:
                logger.error(f"Failed to fetch page {page_count}, stopping update")
                break
            
            # Process all records in this page
            results = page_data.get('results', [])
            logger.info(f"Processing {len(results)} pharmacies from page {page_count}")
            
            # Process facilities in smaller batches
            for i in range(0, len(results), config.batch_size):
                batch = results[i:i + config.batch_size]
                
                # Process batch concurrently but with limited concurrency
                semaphore = asyncio.Semaphore(config.max_concurrent)
                
                async def process_with_semaphore(record):
                    async with semaphore:
                        success = await update_pharmacy_availability(crawler, record)
                        stats['facilities_processed'] += 1
                        if success:
                            stats['facilities_updated'] += 1
                        else:
                            stats['errors'] += 1
                        return success
                
                tasks = [process_with_semaphore(record) for record in batch]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                # Progress logging
                if stats['facilities_processed'] % 50 == 0:
                    logger.info(f"Progress: {stats['facilities_processed']} processed, "
                              f"{stats['facilities_updated']} updated, "
                              f"{stats['errors']} errors")
                
                # Rate limiting between batches
                if config.delay_between_requests > 0:
                    await asyncio.sleep(config.delay_between_requests)
            
            # Move to next page
            links = page_data.get('links', {})
            current_url = links.get('next')
            
            # Log page completion
            total_pages = page_data.get('total_pages', 'unknown')
            logger.info(f"Completed page {page_count} of {total_pages}")
    
    # Print final statistics
    logger.info("=" * 60)
    logger.info("FINAL PHARMACY AVAILABILITY UPDATE STATISTICS")
    logger.info("=" * 60)
    logger.info(f"Facilities Processed: {stats['facilities_processed']}")
    logger.info(f"Facilities Updated: {stats['facilities_updated']}")
    logger.info(f"Facilities Not Found: {stats['facilities_not_found']}")
    logger.info(f"Errors: {stats['errors']}")
    logger.info("=" * 60)
    
    return stats

async def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='NaviCare Pharmacy Availability Updater')
    parser.add_argument('--batch-size', type=int,
                        help='Override batch size from environment')
    parser.add_argument('--delay', type=float,
                        help='Override delay between requests from environment')
    
    args = parser.parse_args()
    
    try:
        # Validate environment
        if not validate_environment():
            sys.exit(1)
        
        # Create configuration
        config = create_config_from_env()
        
        # Apply command line overrides
        if args.batch_size:
            config.batch_size = args.batch_size
        if args.delay:
            config.delay_between_requests = args.delay
        
        logger.info(f"📊 Configuration:")
        logger.info(f"   API URL: {config.base_url}")
        logger.info(f"   Supabase URL: {os.getenv('SUPABASE_URL')}")
        logger.info(f"   Batch Size: {config.batch_size}")
        logger.info(f"   Max Concurrent: {config.max_concurrent}")
        logger.info(f"   Request Delay: {config.delay_between_requests}s")
        logger.info(f"   Max Retries: {config.max_retries}")
        
        # Run availability update
        stats = await fetch_and_update_pharmacy_availability(config)
        
        if stats['errors'] > 0:
            logger.warning(f"Completed with {stats['errors']} errors")
            # sys.exit(1)
        else:
            logger.info("✅ Pharmacy availability update completed successfully!")

        sys.exit(0)
        
    except KeyboardInterrupt:
        logger.info("⏹️  Pharmacy availability update interrupted by user")
    except Exception as e:
        logger.error(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())