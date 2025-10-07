#!/usr/bin/env python3
"""
Test script for Lab Crawler
"""

import asyncio
import os
from dotenv import load_dotenv
from crawlers import LabCrawler, LabCrawlConfig

# Load environment variables
load_dotenv()

async def test_lab_crawler():
    """Test the lab crawler with a single page"""
    # Verify environment variables
    if not os.getenv('SUPABASE_URL') or not os.getenv('SUPABASE_KEY'):
        print("❌ Error: SUPABASE_URL and SUPABASE_KEY environment variables are required")
        return
    
    # Create configuration for testing
    config = LabCrawlConfig(
        base_url=os.getenv('CORTICO_API_URL_LAB'),
        batch_size=5,  # Small batch for testing
        max_concurrent=2,  # Conservative for testing
        delay_between_requests=1.0,  # Be respectful to the API
        max_retries=3
    )
    
    print("🧪 Testing Lab Crawler")
    print("=" * 30)
    
    try:
        async with LabCrawler(config) as crawler:
            # Test single page crawl
            print("Starting single page crawl...")
            await crawler.crawl_single_page(page_number=1)
            print("✅ Single page crawl completed successfully!")
            
            # Print final statistics
            print("\n📊 Final Statistics:")
            for key, value in crawler.stats.items():
                print(f"  {key.replace('_', ' ').title()}: {value}")
                
    except Exception as e:
        print(f"❌ Error during test: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    asyncio.run(test_lab_crawler())