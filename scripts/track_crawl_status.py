#!/usr/bin/env python3
"""
NaviCare Crawl Status Tracker
Script to track and report on the status of segmented crawls
"""

import os
import sys
import json
import argparse
from datetime import datetime, timezone
from supabase import create_client
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def create_supabase_client():
    """Create Supabase client"""
    url = os.getenv('SUPABASE_URL')
    key = os.getenv('SUPABASE_KEY')
    
    if not url or not key:
        raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables are required")
    
    return create_client(url, key)

def get_crawl_statistics(client):
    """Get crawl statistics from database"""
    try:
        # Get total facilities count
        facilities_response = client.table("facilities").select("id", count="exact").execute()
        total_facilities = facilities_response.count
        
        # Get recent observations count
        # Calculate date 7 days ago
        week_ago = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
        week_ago_iso = week_ago.isoformat()
        
        observations_response = client.table("facility_observations").select("id", count="exact").gte("observed_at", week_ago_iso).execute()
        recent_observations = observations_response.count
        
        # Get facility types distribution
        types_response = client.table("facilities").select("facility_type").execute()
        facility_types = {}
        for facility in types_response.data:
            facility_type = facility['facility_type']
            facility_types[facility_type] = facility_types.get(facility_type, 0) + 1
        
        return {
            'total_facilities': total_facilities,
            'recent_observations': recent_observations,
            'facility_types': facility_types,
            'last_updated': datetime.now(timezone.utc).isoformat()
        }
    except Exception as e:
        print(f"Error fetching statistics: {e}")
        return None

def save_status_report(stats, filename=None):
    """Save status report to file"""
    if not filename:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"crawl_status_report_{timestamp}.json"
    
    try:
        with open(filename, 'w') as f:
            json.dump(stats, f, indent=2)
        print(f"Status report saved to {filename}")
        return filename
    except Exception as e:
        print(f"Error saving status report: {e}")
        return None

def print_status_report(stats):
    """Print formatted status report"""
    if not stats:
        print("No statistics available")
        return
    
    print("\n" + "="*50)
    print("NAVICARE CRAWL STATUS REPORT")
    print("="*50)
    print(f"Last Updated: {stats.get('last_updated', 'Unknown')}")
    print(f"Total Facilities: {stats.get('total_facilities', 0)}")
    print(f"Recent Observations (7 days): {stats.get('recent_observations', 0)}")
    
    print("\nFacilities by Type:")
    print("-"*30)
    facility_types = stats.get('facility_types', {})
    for facility_type, count in sorted(facility_types.items()):
        print(f"  {facility_type}: {count}")
    
    print("="*50)

def main():
    """Main function"""
    parser = argparse.ArgumentParser(description='NaviCare Crawl Status Tracker')
    parser.add_argument('--save', action='store_true', help='Save report to file')
    parser.add_argument('--filename', help='Filename for saving report')
    
    args = parser.parse_args()
    
    try:
        # Create Supabase client
        client = create_supabase_client()
        
        # Get crawl statistics
        stats = get_crawl_statistics(client)
        
        # Print status report
        print_status_report(stats)
        
        # Save to file if requested
        if args.save:
            save_status_report(stats, args.filename)
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()