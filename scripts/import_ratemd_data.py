#!/usr/bin/env python3
"""
Import RateMD data into Supabase facilities table
This script reads data from ratemd.json and inserts/updates facilities in the database.
"""

import os
import sys
import json
import logging
import asyncio
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from urllib.parse import urlparse
import re
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Add the parent directory to the path to import utils
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.supabase_client import SupabaseClient

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('ratemd_import.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class RateMDImporter:
    def __init__(self):
        """Initialize the RateMD importer"""
        self.supabase_client = SupabaseClient()
        self.stats = {
            'total_processed': 0,
            'existing_updated': 0,
            'new_created': 0,
            'errors': 0,
            'skipped': 0
        }

    def generate_slug(self, name: str, detail_url: str) -> str:
        """Generate a slug from facility name and URL"""
        # Extract slug from URL if possible
        if detail_url:
            try:
                parsed_url = urlparse(detail_url)
                path_parts = parsed_url.path.strip('/').split('/')
                if len(path_parts) >= 2:
                    # Extract the last meaningful part of the URL
                    url_slug = path_parts[-1]
                    if url_slug and url_slug != 'clinic':
                        return url_slug
            except Exception as e:
                logger.debug(f"Failed to extract slug from URL {detail_url}: {e}")
        
        # Fallback to generating slug from name
        slug = re.sub(r'[^\w\s-]', '', name.lower())
        slug = re.sub(r'[-\s]+', '-', slug)
        return slug.strip('-')

    def parse_rating_data(self, rating: str, review_count: str) -> tuple[Optional[float], Optional[int]]:
        """Parse rating and review count, handling 'N/A' values"""
        parsed_rating = None
        parsed_count = None
        
        if rating and rating != "N/A":
            try:
                parsed_rating = float(rating)
            except ValueError:
                logger.warning(f"Invalid rating value: {rating}")
        
        if review_count and review_count != "N/A":
            try:
                parsed_count = int(review_count)
            except ValueError:
                logger.warning(f"Invalid review count value: {review_count}")
        
        return parsed_rating, parsed_count

    def map_ratemd_to_facility(self, ratemd_data: Dict) -> Dict:
        """Map RateMD data to facility table structure"""
        rating, review_count = self.parse_rating_data(
            ratemd_data.get('rating'), 
            ratemd_data.get('review_count')
        )
        
        facility_data = {
            'name': ratemd_data.get('name'),
            'slug': self.generate_slug(
                ratemd_data.get('name', ''), 
                ratemd_data.get('detail_url', '')
            ),
            'facility_type': 'clinic',
            'phone': ratemd_data.get('phone'),
            'city': ratemd_data.get('city'),
            'province': ratemd_data.get('province'),
            'country': 'Canada',
            'longitude': ratemd_data.get('longitude'),
            'latitude': ratemd_data.get('latitude'),
            'rating_avg': rating,
            'rating_count': review_count,
            'rating_source': 'ratemd',
            'rating_updated_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Remove None values to avoid database issues
        return {k: v for k, v in facility_data.items() if v is not None}

    async def find_existing_facility(self, facility_data: Dict) -> Optional[Dict]:
        """Find existing facility by name and location"""
        try:
            # Try to find by name, city, and province
            response = (
                self.supabase_client.client.table("facilities")
                .select("id, name, slug, rating_avg, rating_count")
                .eq("name", facility_data['name'])
                .eq("city", facility_data['city'])
                .eq("province", facility_data['province'])
                .limit(1)
                .execute()
            )
            
            return response.data[0] if response.data else None
            
        except Exception as e:
            logger.error(f"Error finding existing facility: {e}")
            return None

    async def update_existing_facility(self, facility_id: str, facility_data: Dict) -> bool:
        """Update existing facility with rating data"""
        try:
            # Only update rating-related fields
            update_data = {
                'rating_avg': facility_data.get('rating_avg'),
                'rating_count': facility_data.get('rating_count'),
                'rating_source': facility_data.get('rating_source'),
                'rating_updated_at': facility_data.get('rating_updated_at'),
                'updated_at': datetime.now(timezone.utc).isoformat()
            }
            
            # Remove None values
            update_data = {k: v for k, v in update_data.items() if v is not None}
            
            response = (
                self.supabase_client.client.table("facilities")
                .update(update_data)
                .eq("id", facility_id)
                .execute()
            )
            
            return len(response.data) > 0
            
        except Exception as e:
            logger.error(f"Error updating facility {facility_id}: {e}")
            return False

    async def create_new_facility(self, facility_data: Dict) -> bool:
        """Create new facility"""
        try:
            # Add created_at timestamp
            facility_data['created_at'] = datetime.now(timezone.utc).isoformat()
            facility_data['updated_at'] = datetime.now(timezone.utc).isoformat()
            
            response = (
                self.supabase_client.client.table("facilities")
                .insert(facility_data)
                .execute()
            )
            
            return len(response.data) > 0
            
        except Exception as e:
            logger.error(f"Error creating facility: {e}")
            return False

    async def process_facility(self, ratemd_data: Dict) -> bool:
        """Process a single facility record"""
        try:
            self.stats['total_processed'] += 1
            
            # Map data to facility structure
            facility_data = self.map_ratemd_to_facility(ratemd_data)
            
            # Validate required fields
            if not facility_data.get('name'):
                logger.warning("Skipping facility with no name")
                self.stats['skipped'] += 1
                return False
            
            # Find existing facility
            existing = await self.find_existing_facility(facility_data)
            
            if existing:
                # Update existing facility with rating data
                success = await self.update_existing_facility(existing['id'], facility_data)
                if success:
                    self.stats['existing_updated'] += 1
                    logger.info(f"Updated facility: {facility_data['name']}")
                    return True
                else:
                    self.stats['errors'] += 1
                    return False
            else:
                # Create new facility
                success = await self.create_new_facility(facility_data)
                if success:
                    self.stats['new_created'] += 1
                    logger.info(f"Created facility: {facility_data['name']}")
                    return True
                else:
                    self.stats['errors'] += 1
                    return False
                    
        except Exception as e:
            logger.error(f"Error processing facility {ratemd_data.get('name', 'Unknown')}: {e}")
            self.stats['errors'] += 1
            return False

    async def import_data(self, json_file_path: str, batch_size: int = 100):
        """Import data from JSON file"""
        try:
            logger.info(f"Starting import from {json_file_path}")
            
            # Load JSON data
            with open(json_file_path, 'r', encoding='utf-8') as f:
                ratemd_data = json.load(f)
            
            logger.info(f"Loaded {len(ratemd_data)} records from JSON file")
            
            # Process in batches
            for i in range(0, len(ratemd_data), batch_size):
                batch = ratemd_data[i:i + batch_size]
                logger.info(f"Processing batch {i//batch_size + 1}/{(len(ratemd_data) + batch_size - 1)//batch_size}")
                
                # Process batch concurrently
                tasks = [self.process_facility(record) for record in batch]
                await asyncio.gather(*tasks, return_exceptions=True)
                
                # Log progress
                if (i + batch_size) % 1000 == 0 or i + batch_size >= len(ratemd_data):
                    logger.info(f"Processed {min(i + batch_size, len(ratemd_data))}/{len(ratemd_data)} records")
            
            logger.info("Import completed successfully")
            self.print_stats()
            
        except FileNotFoundError:
            logger.error(f"JSON file not found: {json_file_path}")
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON file: {e}")
        except Exception as e:
            logger.error(f"Unexpected error during import: {e}")

    def print_stats(self):
        """Print import statistics"""
        logger.info("=== Import Statistics ===")
        logger.info(f"Total processed: {self.stats['total_processed']}")
        logger.info(f"Existing facilities updated: {self.stats['existing_updated']}")
        logger.info(f"New facilities created: {self.stats['new_created']}")
        logger.info(f"Errors: {self.stats['errors']}")
        logger.info(f"Skipped: {self.stats['skipped']}")
        logger.info("========================")

async def main():
    """Main function"""
    # Check environment variables
    if not os.environ.get("SUPABASE_URL") or not os.environ.get("SUPABASE_KEY"):
        logger.error("SUPABASE_URL and SUPABASE_KEY environment variables are required")
        sys.exit(1)
    
    # Get JSON file path from command line argument or use default
    json_file_path = sys.argv[1] if len(sys.argv) > 1 else "ratemd.json"
    
    if not os.path.exists(json_file_path):
        logger.error(f"JSON file not found: {json_file_path}")
        sys.exit(1)
    
    # Create importer and run
    importer = RateMDImporter()
    
    # Test connection first
    if not await importer.supabase_client.test_connection():
        logger.error("Failed to connect to Supabase")
        sys.exit(1)
    
    # Run import
    await importer.import_data(json_file_path)

if __name__ == "__main__":
    asyncio.run(main())

