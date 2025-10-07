#!/usr/bin/env python3
"""
Test script for all crawlers
"""

import asyncio
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

async def test_clinic_crawler():
    """Test the clinic crawler"""
    try:
        from crawlers import CorticoCrawler, CrawlConfig
        config = CrawlConfig(
            base_url=os.getenv('CORTICO_API_URL'),
            batch_size=2,  # Small batch for testing
            max_concurrent=2,  # Conservative for testing
            delay_between_requests=1.0,  # Be respectful to the API
            max_retries=3
        )
        
        print("🧪 Testing Clinic Crawler")
        print("=" * 30)
        
        async with CorticoCrawler(config) as crawler:
            # Test single page crawl
            print("Starting single page crawl...")
            await crawler.crawl_single_page(page_number=1)
            print("✅ Clinic crawler test completed successfully!")
            
    except Exception as e:
        print(f"❌ Error testing clinic crawler: {e}")
        import traceback
        traceback.print_exc()

async def test_lab_crawler():
    """Test the lab crawler"""
    try:
        from crawlers import LabCrawler, LabCrawlConfig
        config = LabCrawlConfig(
            base_url=os.getenv('CORTICO_API_URL_LAB'),
            batch_size=2,  # Small batch for testing
            max_concurrent=2,  # Conservative for testing
            delay_between_requests=1.0,  # Be respectful to the API
            max_retries=3
        )
        
        print("🧪 Testing Lab Crawler")
        print("=" * 30)
        
        async with LabCrawler(config) as crawler:
            # Test single page crawl
            print("Starting single page crawl...")
            await crawler.crawl_single_page(page_number=1)
            print("✅ Lab crawler test completed successfully!")
            
    except Exception as e:
        print(f"❌ Error testing lab crawler: {e}")
        import traceback
        traceback.print_exc()

async def test_pharmacy_crawler():
    """Test the pharmacy crawler"""
    try:
        from crawlers import PharmacyCrawler, PharmacyCrawlConfig
        config = PharmacyCrawlConfig(
            base_url=os.getenv('CORTICO_API_URL_PHARMACY'),
            batch_size=2,  # Small batch for testing
            max_concurrent=2,  # Conservative for testing
            delay_between_requests=1.0,  # Be respectful to the API
            max_retries=3
        )
        
        print("🧪 Testing Pharmacy Crawler")
        print("=" * 30)
        
        async with PharmacyCrawler(config) as crawler:
            # Test single page crawl
            print("Starting single page crawl...")
            await crawler.crawl_single_page(page_number=1)
            print("✅ Pharmacy crawler test completed successfully!")
            
    except Exception as e:
        print(f"❌ Error testing pharmacy crawler: {e}")
        import traceback
        traceback.print_exc()

async def main():
    """Run all tests"""
    print("🚀 Starting Crawler Tests")
    print("=" * 50)
    
    # Verify environment variables
    required_vars = ['SUPABASE_URL', 'SUPABASE_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these variables in your .env file")
        return
    
    # Run tests
    await test_clinic_crawler()
    print()
    await test_lab_crawler()
    print()
    await test_pharmacy_crawler()
    
    print("\n🎉 All tests completed!")

if __name__ == "__main__":
    asyncio.run(main())