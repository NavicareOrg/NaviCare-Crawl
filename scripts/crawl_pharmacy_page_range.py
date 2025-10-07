#!/usr/bin/env python3
"""
NaviCare Pharmacy Crawler Runner for Page Range
Script to run the Pharmacy crawler for a specific page range
"""

import os
import sys
import asyncio
import argparse
from dotenv import load_dotenv
from crawlers import PharmacyCrawler, PharmacyCrawlConfig

# Load environment variables
load_dotenv()

def create_config_from_env() -> PharmacyCrawlConfig:
    """Create crawler configuration from environment variables"""
    return PharmacyCrawlConfig(
        base_url=os.getenv('CORTICO_API_URL_PHARMACY'),
        batch_size=int(os.getenv('CRAWLER_BATCH_SIZE', '50')),
        max_concurrent=int(os.getenv('CRAWLER_MAX_CONCURRENT', '5')),
        delay_between_requests=float(os.getenv('CRAWLER_DELAY', '0.5')),
        max_retries=int(os.getenv('CRAWLER_MAX_RETRIES', '3')),
    )

def validate_environment():
    """Validate required environment variables"""
    required_vars = ['SUPABASE_URL', 'SUPABASE_KEY']
    missing_vars = [var for var in required_vars if not os.getenv(var)]
    
    if missing_vars:
        print("❌ Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these variables in your .env file")
        return False
    
    return True

async def run_page_range_crawl(config: PharmacyCrawlConfig, start_page: int, end_page: int):
    """Run the crawl for a specific page range"""
    print(f"🚀 Starting NaviCare Pharmacy Page Range Crawl (Pages {start_page}-{end_page})")
    print("=" * 50)
    
    async with PharmacyCrawler(config) as crawler:
        await crawler.crawl_page_range(start_page, end_page)
    
    print("✅ Pharmacy page range crawl completed successfully!")

async def main():
    """Main runner function"""
    parser = argparse.ArgumentParser(description='NaviCare Pharmacy Crawler - Page Range')
    parser.add_argument('--start-page', type=int, default=1,
                        help='Start page number (default: 1)')
    parser.add_argument('--end-page', type=int, required=True,
                        help='End page number (inclusive)')
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
        
        print(f"📊 Configuration:")
        print(f"   API URL: {config.base_url}")
        print(f"   Supabase URL: {os.getenv('SUPABASE_URL')}")
        print(f"   Batch Size: {config.batch_size}")
        print(f"   Max Concurrent: {config.max_concurrent}")
        print(f"   Request Delay: {config.delay_between_requests}s")
        print(f"   Max Retries: {config.max_retries}")
        print(f"   Page Range: {args.start_page}-{args.end_page}")
        print()
        
        # Run page range crawl
        await run_page_range_crawl(config, args.start_page, args.end_page)
        
    except KeyboardInterrupt:
        print("\n⏹️  Crawling interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())