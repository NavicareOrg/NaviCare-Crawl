#!/usr/bin/env python3
"""
Medical Facility Services Crawler - Proof of Concept
Using Crawl4AI to extract services from Canadian medical facility websites
"""

import asyncio
import json
import logging
import os
from typing import Dict, List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMConfig, BrowserConfig, CacheMode
from crawl4ai.extraction_strategy import LLMExtractionStrategy

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class MedicalService(BaseModel):
    """Pydantic model for a medical service"""
    name: str = Field(..., description="Name of the medical service")
    category: str = Field(..., description="Category of the service (emergency, primary_care, specialty, etc.)")
    description: str = Field(default="", description="Description of the service")
    department: str = Field(default="", description="Department or unit providing the service")

class MedicalFacility(BaseModel):
    """Pydantic model for structured extraction of medical facility data"""
    facility_name: str = Field(..., description="Official name of the medical facility")
    facility_type: str = Field(..., description="Type of facility (hospital, clinic, urgent_care, medical_center, etc.)")
    services: List[MedicalService] = Field(default=[], description="List of medical services offered")
    departments: List[str] = Field(default=[], description="List of departments/specialties available")
    address: str = Field(default="", description="Physical address of the facility")
    phone: str = Field(default="", description="Main phone number")

class MedicalServicesCrawler:
    """AI-powered crawler for extracting medical services information"""
    
    def __init__(self, provider: str = "openai/gpt-4o-mini", api_token: str = None):
        self.provider = provider
        self.api_token = api_token or os.getenv("OPENAI_API_KEY")
        self.extraction_instruction = self._build_extraction_instruction()
        self.results = []
        
    def _build_extraction_instruction(self) -> str:
        """Build the LLM extraction instruction for medical services"""
        return """
        You are analyzing a Canadian medical facility website. Extract comprehensive information about the medical services offered by this facility.

        Focus on:
        1. The official name and type of the medical facility
        2. All medical services mentioned (emergency care, primary care, specialties, diagnostics, surgical services, etc.)
        3. Departments and specialized units
        4. Contact information (address, phone)

        Guidelines:
        - Extract both English and French service names if the site is bilingual
        - Include emergency services, walk-in clinics, specialist consultations, diagnostic services
        - Look for services in navigation menus, service pages, department listings, and main content
        - Categorize services appropriately (emergency, primary_care, specialty, diagnostic, surgical, mental_health, rehabilitation, other)
        - For departments, include both clinical departments and support services
        - Be comprehensive but avoid duplicating similar services
        - If limited information is available, extract what you can find

        Return detailed information about all services and departments mentioned on the website.
        """

    async def crawl_facility(self, url: str) -> Optional[Dict]:
        """Crawl a single medical facility website"""
        try:
            logger.info(f"Crawling: {url}")
            
            # Configure browser
            browser_config = BrowserConfig(
                headless=True,
                extra_args=["--no-sandbox", "--disable-dev-shm-usage"]
            )
            
            # Configure LLM extraction
            extra_args = {
                "temperature": 0.1,
                "max_tokens": 3000,
                "top_p": 0.9
            }
            
            # Configure crawler
            crawler_config = CrawlerRunConfig(
                cache_mode=CacheMode.BYPASS,
                word_count_threshold=10,
                page_timeout=60000,
                extraction_strategy=LLMExtractionStrategy(
                    llm_config=LLMConfig(
                        provider=self.provider,
                        api_token=self.api_token
                    ),
                    schema=MedicalFacility.model_json_schema(),
                    extraction_type="schema",
                    instruction=self.extraction_instruction,
                    extra_args=extra_args,
                ),
            )
            
            async with AsyncWebCrawler(config=browser_config) as crawler:
                result = await crawler.arun(url=url, config=crawler_config)
                
                if result.success and result.extracted_content:
                    print("✅ Extraction successful")
                    print(result.extracted_content)
                    try:
                        # Parse the extracted JSON
                        if isinstance(result.extracted_content, str):
                            extracted_data = json.loads(result.extracted_content)
                        else:
                            extracted_data = result.extracted_content

                        if isinstance(extracted_data, list):
                            if len(extracted_data) == 1 and isinstance(extracted_data[0], dict):
                                extracted_data = extracted_data[0]
                            elif len(extracted_data) > 1:
                                # If multiple facilities extracted (unexpected for one URL), take the first or handle as needed
                                logger.warning(f"Multiple facilities extracted from {url}, using the first one")
                                extracted_data = extracted_data[0]
                            else:
                                raise ValueError("Extracted data is an empty list or invalid")
                        elif not isinstance(extracted_data, dict):
                            raise ValueError(f"Unexpected extracted_data type: {type(extracted_data)}")
                                            
                        # Add metadata
                        extracted_data['source_url'] = url
                        extracted_data['crawl_timestamp'] = datetime.now().isoformat()
                        extracted_data['success'] = True
                        extracted_data['content_length'] = len(result.markdown) if result.markdown else 0
                        
                        services_count = len(extracted_data.get('services', []))
                        logger.info(f"✅ Successfully extracted data from {url}")
                        logger.info(f"   Facility: {extracted_data.get('facility_name', 'Unknown')}")
                        logger.info(f"   Services found: {services_count}")
                        
                        return extracted_data
                        
                    except json.JSONDecodeError as e:
                        logger.error(f"❌ JSON decode error for {url}: {e}")
                        return {
                            'source_url': url,
                            'success': False,
                            'error': f'JSON parsing failed: {str(e)}',
                            'raw_content': str(result.extracted_content)[:500],
                            'crawl_timestamp': datetime.now().isoformat()
                        }
                        
                else:
                    error_msg = result.error if hasattr(result, 'error') else "Unknown crawling error"
                    logger.error(f"❌ Failed to crawl {url}: {error_msg}")
                    return {
                        'source_url': url,
                        'success': False,
                        'error': error_msg,
                        'crawl_timestamp': datetime.now().isoformat()
                    }
                    
        except Exception as e:
            logger.error(f"❌ Exception while crawling {url}: {str(e)}")
            return {
                'source_url': url,
                'success': False,
                'error': f'Exception: {str(e)}',
                'crawl_timestamp': datetime.now().isoformat()
            }

    async def crawl_multiple_facilities(self, urls: List[str], max_concurrent: int = 2) -> List[Dict]:
        """Crawl multiple facilities with concurrency control"""
        semaphore = asyncio.Semaphore(max_concurrent)
        
        async def crawl_with_semaphore(url: str) -> Dict:
            async with semaphore:
                await asyncio.sleep(1)  # Rate limiting
                return await self.crawl_facility(url)
        
        logger.info(f"Starting to crawl {len(urls)} facilities with max {max_concurrent} concurrent requests")
        
        # Execute crawling tasks
        tasks = [crawl_with_semaphore(url) for url in urls]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        processed_results = []
        for i, result in enumerate(results):
            if isinstance(result, Exception):
                processed_results.append({
                    'source_url': urls[i],
                    'success': False,
                    'error': f'Task exception: {str(result)}',
                    'crawl_timestamp': datetime.now().isoformat()
                })
            else:
                processed_results.append(result)
        
        self.results = processed_results
        return processed_results

    def save_results(self, filename: str = None) -> str:
        """Save crawling results to JSON file"""
        if not filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"medical_facilities_crawl_{timestamp}.json"
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(self.results, f, indent=2, ensure_ascii=False)
        
        logger.info(f"💾 Results saved to {filename}")
        return filename

    def generate_summary_report(self) -> Dict:
        """Generate a summary report of the crawling results"""
        if not self.results:
            return {"error": "No results to analyze"}
        
        successful_crawls = [r for r in self.results if r.get('success', False)]
        failed_crawls = [r for r in self.results if not r.get('success', False)]
        
        total_services = sum(len(r.get('services', [])) for r in successful_crawls)
        
        service_categories = {}
        facility_types = {}
        
        for result in successful_crawls:
            # Count facility types
            facility_type = result.get('facility_type', 'unknown')
            facility_types[facility_type] = facility_types.get(facility_type, 0) + 1
            
            # Count service categories
            for service in result.get('services', []):
                if isinstance(service, dict):
                    category = service.get('category', 'unknown')
                    service_categories[category] = service_categories.get(category, 0) + 1
        
        return {
            "total_facilities_attempted": len(self.results),
            "successful_extractions": len(successful_crawls),
            "failed_extractions": len(failed_crawls),
            "success_rate": f"{len(successful_crawls)/len(self.results)*100:.1f}%" if self.results else "0%",
            "total_services_extracted": total_services,
            "average_services_per_facility": f"{total_services/len(successful_crawls):.1f}" if successful_crawls else "0",
            "facility_types_distribution": facility_types,
            "service_categories_distribution": service_categories,
            "failed_urls": [r['source_url'] for r in failed_crawls]
        }

    def print_sample_results(self, max_facilities: int = 2):
        """Print sample results for review"""
        successful_results = [r for r in self.results if r.get('success', False)]
        
        print(f"\n🔍 Sample Results (showing up to {max_facilities} facilities):")
        print("=" * 60)
        
        for i, result in enumerate(successful_results[:max_facilities]):
            print(f"\n📍 Facility {i+1}: {result.get('facility_name', 'Unknown')}")
            print(f"   Type: {result.get('facility_type', 'Unknown')}")
            print(f"   URL: {result.get('source_url', '')}")
            
            services = result.get('services', [])
            print(f"   Services ({len(services)}):")
            for j, service in enumerate(services[:5]):  # Show first 5 services
                if isinstance(service, dict):
                    name = service.get('name', 'Unknown')
                    category = service.get('category', 'Unknown')
                    print(f"     {j+1}. {name} ({category})")
            
            if len(services) > 5:
                print(f"     ... and {len(services) - 5} more services")
            
            departments = result.get('departments', [])
            if departments:
                print(f"   Departments: {', '.join(departments[:3])}")
                if len(departments) > 3:
                    print(f"     ... and {len(departments) - 3} more departments")

async def main():
    """Main function to demonstrate the crawler"""
    
    # Check for API key
    if not os.getenv("OPENAI_API_KEY"):
        print("❌ Error: OPENAI_API_KEY environment variable not set")
        print("Please set your OpenAI API key:")
        print("export OPENAI_API_KEY='your-api-key-here'")
        return
    
    print("🏥 Medical Facility Services Crawler - PoC")
    print("=" * 50)
    
    # Test URLs - Canadian medical facilities
    test_urls = [
        "https://sunnybrook.ca/",
        "https://www.uhn.ca/",
        "https://www.sickkids.ca/",
        "https://www.ottawahospital.on.ca/",
        "https://www.lhsc.on.ca/",
        "https://quickcarewalkinclinic.com/",
        "http://medcentrehealth.com/",
        "https://www.northumberland.ca/en/living-here/port-hope-medical-walk-in-clinic.aspx",
        "https://www.newvisionhealth.ca/",
        "https://accessalliance.ca/programs-services/primary-health-care-services/non-insured-walk-in-clinic/"
    ]
    
    # Initialize crawler
    crawler = MedicalServicesCrawler(
        provider="openai/gpt-4o-mini",  # Using the correct provider format
        api_token=os.getenv("OPENAI_API_KEY")
    )
    
    print(f"\n🚀 Starting crawl of {len(test_urls)} medical facilities...")
    
    # Crawl facilities
    results = await crawler.crawl_multiple_facilities(test_urls, max_concurrent=2)
    
    # Print sample results
    crawler.print_sample_results(max_facilities=2)
    
    # Generate and display summary
    summary = crawler.generate_summary_report()
    print(f"\n📊 Crawling Summary:")
    print(f"   Total facilities attempted: {summary['total_facilities_attempted']}")
    print(f"   Successful extractions: {summary['successful_extractions']}")
    print(f"   Success rate: {summary['success_rate']}")
    print(f"   Total services extracted: {summary['total_services_extracted']}")
    print(f"   Average services per facility: {summary['average_services_per_facility']}")
    
    if summary.get('facility_types_distribution'):
        print("   Facility types found:")
        for ftype, count in summary['facility_types_distribution'].items():
            print(f"     - {ftype}: {count}")
    
    if summary.get('failed_urls'):
        print("   Failed URLs:")
        for url in summary['failed_urls']:
            print(f"     - {url}")
    
    # Save results
    filename = crawler.save_results()
    print(f"\n💾 Complete results saved to: {filename}")
    
    print(f"\n✨ Crawling completed! Check the JSON file for full results.")
    
    return results

if __name__ == "__main__":
    # Check dependencies
    try:
        from crawl4ai import AsyncWebCrawler, CrawlerRunConfig, LLMConfig, BrowserConfig, CacheMode
    except ImportError as e:
        print("❌ Missing dependencies. Please install with:")
        print("pip install crawl4ai[all]")
        print(f"Error: {e}")
        exit(1)
    
    print("🔧 Dependencies check passed")
    print("📝 Make sure to set your OPENAI_API_KEY environment variable")
    print("🔄 Alternative: Use 'ollama/llama2' provider for local models")
    print("")
    
    # Run the crawler
    asyncio.run(main())