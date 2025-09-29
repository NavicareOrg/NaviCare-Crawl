#!/usr/bin/env python3
"""
NaviCare Crawler Runner
Simple script to run the Cortico crawler with environment configuration
"""

import os
import sys
import asyncio
import argparse
from dotenv import load_dotenv
from cortico_crawler import CorticoCrawler, CrawlConfig

# Load environment variables
load_dotenv()

def create_config_from_env() -> CrawlConfig:
    """Create crawler configuration from environment variables"""
    return CrawlConfig(
        base_url=os.getenv('CORTICO_API_URL'),
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
        print("❌ Error: Missing required environment variables:")
        for var in missing_vars:
            print(f"   - {var}")
        print("\nPlease set these variables in your .env file")
        return False
    
    return True

async def run_full_crawl(config: CrawlConfig):
    """Run the full crawl of all pages"""
    print("🚀 Starting NaviCare Cortico Full Crawl")
    print("=" * 50)
    
    async with CorticoCrawler(config) as crawler:
        await crawler.crawl_all()
    
    print("✅ Full crawl completed successfully!")

async def run_test_crawl(config: CrawlConfig, page_number: int = 1):
    """Run a test crawl of a single page"""
    print(f"🧪 Starting NaviCare Cortico Test Crawl (Page {page_number})")
    print("=" * 50)
    
    async with CorticoCrawler(config) as crawler:
        await crawler.crawl_single_page(page_number)
    
    print("✅ Test crawl completed successfully!")

async def main():
    """Main runner function"""
    parser = argparse.ArgumentParser(description='NaviCare Cortico Crawler')
    parser.add_argument('--mode', choices=['full', 'test'], default='test',
                        help='Crawl mode: full (all pages) or test (single page)')
    parser.add_argument('--page', type=int, default=1,
                        help='Page number for test mode (default: 1)')
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
        print(f"   Mode: {args.mode}")
        print(f"   API URL: {config.base_url}")
        print(f"   Supabase URL: {os.getenv('SUPABASE_URL')}")
        print(f"   Batch Size: {config.batch_size}")
        print(f"   Max Concurrent: {config.max_concurrent}")
        print(f"   Request Delay: {config.delay_between_requests}s")
        print(f"   Max Retries: {config.max_retries}")
        print()
        
        # Run appropriate crawl mode
        if args.mode == 'full':
            await run_full_crawl(config)
        else:
            await run_test_crawl(config, args.page)
        
    except KeyboardInterrupt:
        print("\n⏹️  Crawling interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    asyncio.run(main())