"""
NaviCare Supabase Database Client
Handles all database operations using Supabase Python client
"""

import os
import logging
from typing import Dict, List, Optional, Any
from datetime import datetime, timezone
from supabase import create_client, Client
from postgrest import APIError
import json

logger = logging.getLogger(__name__)

class SupabaseClient:
    def __init__(self):
        """Initialize Supabase client"""
        self.url = os.environ.get("SUPABASE_URL")
        self.key = os.environ.get("SUPABASE_KEY")
        
        if not self.url or not self.key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY environment variables are required")
        
        self.client: Client = create_client(self.url, self.key)
        logger.info("Supabase client initialized successfully")

    async def create_service(self, service_data: Dict) -> Optional[str]:
        """Create a new service and return its ID"""
        try:
            response = (
                self.client.table("services")
                .insert(service_data)
                .execute()
            )
            
            if response.data:
                service_id = response.data[0]['id']
                logger.debug(f"Created service: {service_data.get('display_name')}")
                return service_id
            else:
                raise Exception("Failed to insert service - no ID returned")
                
        except APIError as e:
            logger.error(f"Error creating service {service_data.get('display_name')}: {e}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error creating service: {e}")
            return None
    
    
    async def find_existing_facility(self, slug: str, name: str, city: str, province: str) -> Optional[Dict]:
        """Find existing facility by slug or name/location combination"""
        try:
            # First try by slug
            response = (
                self.client.table("facilities")
                .select("id, name, slug")
                .eq("slug", slug)
                .limit(1)
                .execute()
            )
            
            if response.data:
                return response.data[0]
            
            # If not found by slug, try by name and location
            response = (
                self.client.table("facilities")
                .select("id, name, slug")
                .eq("name", name)
                .eq("city", city)
                .eq("province", province)
                .limit(1)
                .execute()
            )
            
            return response.data[0] if response.data else None
            
        except APIError as e:
            logger.error(f"Error finding existing facility: {e}")
            return None

    async def upsert_facility(self, facility_data: Dict) -> str:
        """Insert or update facility and return facility ID"""
        try:
            # Check if facility exists
            existing = await self.find_existing_facility(
                facility_data.get('slug', ''),
                facility_data.get('name', ''),
                facility_data.get('city', ''),
                facility_data.get('province', '')
            )
            
            if existing:
                # Update existing facility
                facility_id = existing['id']
                update_data = {**facility_data, 'updated_at': datetime.now(timezone.utc).isoformat()}
                
                response = (
                    self.client.table("facilities")
                    .update(update_data)
                    .eq("id", facility_id)
                    .execute()
                )
                
                logger.debug(f"Updated facility: {facility_data.get('name')}")
                return facility_id
                
            else:
                # Insert new facility
                insert_data = {**facility_data, 'created_at': datetime.now(timezone.utc).isoformat()}
                
                response = (
                    self.client.table("facilities")
                    .insert(insert_data)
                    .execute()
                )
                
                if response.data:
                    facility_id = response.data[0]['id']
                    logger.debug(f"Created facility: {facility_data.get('name')}")
                    return facility_id
                else:
                    raise Exception("Failed to insert facility - no ID returned")
                    
        except APIError as e:
            logger.error(f"Error upserting facility {facility_data.get('name')}: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error upserting facility: {e}")
            raise

    async def insert_observation(self, observation_data: Dict) -> bool:
        """Insert facility observation"""
        try:
            response = (
                self.client.table("facility_observations")
                .insert(observation_data)
                .execute()
            )
            
            return len(response.data) > 0
            
        except APIError as e:
            logger.error(f"Error inserting observation: {e}")
            return False

    async def get_specialty_by_name(self, name: str) -> Optional[Dict]:
        """Get specialty by name, create if not exists"""
        try:
            # First try to find existing specialty
            response = (
                self.client.table("specialties")
                .select("id, name")
                .eq("name", name)
                .limit(1)
                .execute()
            )
            
            if response.data:
                return response.data[0]
            
            # If not found, create new specialty
            response = (
                self.client.table("specialties")
                .insert({"name": name})
                .execute()
            )
            
            return response.data[0] if response.data else None
            
        except APIError as e:
            logger.error(f"Error getting/creating specialty {name}: {e}")
            return None

    async def link_facility_specialties(self, facility_id: str, specialties: List[str]) -> bool:
        """Link specialties to a facility"""
        try:
            # First remove existing links
            self.client.table("facility_specialties").delete().eq("facility_id", facility_id).execute()
            
            # Get or create specialties and create links
            for specialty_name in specialties:
                specialty = await self.get_specialty_by_name(specialty_name)
                if specialty:
                    self.client.table("facility_specialties").insert({
                        "facility_id": facility_id,
                        "specialty_id": specialty["id"]
                    }).execute()
            
            return True
            
        except APIError as e:
            logger.error(f"Error linking specialties for facility {facility_id}: {e}")
            return False

    async def get_service_by_slug(self, slug: str) -> Optional[Dict]:
        """Get service by slug"""
        try:
            response = (
                self.client.table("services")
                .select("id, slug, display_name")
                .eq("slug", slug)
                .limit(1)
                .execute()
            )
            
            return response.data[0] if response.data else None
            
        except APIError as e:
            logger.error(f"Error fetching service by slug {slug}: {e}")
            return None

    async def upsert_facility_service_offering(self, offering_data: Dict) -> bool:
        """Insert or update facility service offering"""
        try:
            # Check if offering exists
            response = (
                self.client.table("facility_service_offerings")
                .select("facility_id, service_id")
                .eq("facility_id", offering_data['facility_id'])
                .eq("service_id", offering_data['service_id'])
                .limit(1)
                .execute()
            )
            
            if response.data:
                # Update existing offering
                update_response = (
                    self.client.table("facility_service_offerings")
                    .update(offering_data)
                    .eq("facility_id", offering_data['facility_id'])
                    .eq("service_id", offering_data['service_id'])
                    .execute()
                )
                return len(update_response.data) > 0
            else:
                # Insert new offering
                insert_response = (
                    self.client.table("facility_service_offerings")
                    .insert(offering_data)
                    .execute()
                )
                return len(insert_response.data) > 0
                
        except APIError as e:
            logger.error(f"Error upserting service offering: {e}")
            return False

    async def insert_booking_channel(self, channel_data: Dict) -> bool:
        """Insert facility booking channel"""
        try:
            response = (
                self.client.table("facility_booking_channels")
                .insert(channel_data)
                .execute()
            )
            
            return len(response.data) > 0
            
        except APIError as e:
            logger.error(f"Error inserting booking channel: {e}")
            return False

    async def insert_availability(self, availability_data: Dict) -> bool:
        """Insert facility service availability"""
        try:
            response = (
                self.client.table("facility_availability")
                .insert(availability_data)
                .execute()
            )
            
            return len(response.data) > 0
            
        except APIError as e:
            logger.error(f"Error inserting availability: {e}")
            return False

    async def replace_facility_hours(self, facility_id: str, hours: List[Dict]) -> bool:
        """Replace facility operating hours with new records"""
        try:
            self.client.table("facility_hours").delete().eq("facility_id", facility_id).execute()

            if not hours:
                return True

            sanitized: List[Dict[str, Any]] = []
            for record in hours:
                if not record:
                    continue

                weekday = record.get('weekday')
                open_time = record.get('open_time')
                close_time = record.get('close_time')
                if weekday is None or not open_time or not close_time:
                    continue

                slot = record.get('slot') or 1

                weekday_label = record.get('weekday_label')
                if not weekday_label and isinstance(weekday, int) and 0 <= weekday <= 6:
                    weekday_label = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'][weekday]

                sanitized.append({
                    'facility_id': facility_id,
                    'weekday': weekday,
                    'weekday_label': weekday_label,
                    'open_time': open_time,
                    'close_time': close_time,
                    'notes': record.get('notes'),
                    'slot': slot
                })

            if not sanitized:
                logger.debug(f"No valid operating hours to insert for facility {facility_id}")
                return True

            sanitized.sort(key=lambda r: (r['weekday'], r['slot'], r['open_time']))

            response = (
                self.client.table("facility_hours")
                .insert(sanitized)
                .execute()
            )

            return len(response.data) > 0

        except APIError as e:
            logger.error(f"Error replacing facility hours for {facility_id}: {e}")
            return False

    async def get_facility_stats(self) -> Dict:
        """Get basic statistics about facilities in the database"""
        try:
            # Count total facilities
            total_response = (
                self.client.table("facilities")
                .select("id", count="exact")
                .execute()
            )
            
            # Count by facility type
            type_response = (
                self.client.table("facilities")
                .select("facility_type")
                .execute()
            )
            
            # Count observations
            obs_response = (
                self.client.table("facility_observations")
                .select("id", count="exact")
                .execute()
            )
            
            facility_types = {}
            for facility in type_response.data:
                facility_type = facility['facility_type']
                facility_types[facility_type] = facility_types.get(facility_type, 0) + 1
            
            return {
                'total_facilities': total_response.count,
                'total_observations': obs_response.count,
                'facility_types': facility_types
            }
            
        except APIError as e:
            logger.error(f"Error fetching stats: {e}")
            return {}

    async def cleanup_old_observations(self, days_old: int = 7) -> int:
        """Clean up old observations to prevent database bloat"""
        try:
            cutoff_date = datetime.now(timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)
            cutoff_date = cutoff_date.replace(day=cutoff_date.day - days_old)
            
            response = (
                self.client.table("facility_observations")
                .delete()
                .lt("observed_at", cutoff_date.isoformat())
                .execute()
            )
            
            deleted_count = len(response.data) if response.data else 0
            logger.info(f"Cleaned up {deleted_count} old observations")
            return deleted_count
            
        except APIError as e:
            logger.error(f"Error cleaning up old observations: {e}")
            return 0

    async def test_connection(self) -> bool:
        """Test the Supabase connection"""
        try:
            response = (
                self.client.table("facilities")
                .select("id")
                .limit(1)
                .execute()
            )
            
            logger.info("Supabase connection test successful")
            return True
            
        except Exception as e:
            logger.error(f"Supabase connection test failed: {e}")
            return False
