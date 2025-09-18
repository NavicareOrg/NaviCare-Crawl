"""
NaviCare Data Transformer
Handles transformation of external API data to NaviCare database format
"""

import re
import json
from typing import Dict, List, Optional
from datetime import datetime, timezone
import logging

logger = logging.getLogger(__name__)

class CorticoTransformer:
    """Transforms Cortico API data to NaviCare format"""
    
    WORKFLOW_SERVICE_MAPPING = {
        'family-doctor': 'family-medicine',
        'rapid-access-telehealth': 'walk-in',
        'flu-shot': 'flu-shot',
        'covid-vaccination': 'flu-shot',  # Map to general vaccination
        'terminal': 'walk-in',
        'walk-in': 'walk-in',
        'urgent-care': 'walk-in',
        'mental-health': 'mental-health',
        'physiotherapy': 'physiotherapy',
        'dental': 'dental-cleaning',
        'vision': 'eye-exam',
        'pharmacy': 'prescription-refill',
    }
    
    @staticmethod
    def transform_facility(cortico_data: Dict) -> Dict:
        """Transform Cortico API data to NaviCare facility format"""
        
        # Defensive: Ensure lists/dicts are not None
        specialties = cortico_data.get('specialties') or []
        workflows = cortico_data.get('workflows') or []
        
        # Map facility type based on specialties and workflows
        facility_type = CorticoTransformer._determine_facility_type(
            specialties,
            workflows
        )
        
        # Extract coordinates with None handling
        point_data = cortico_data.get('point') or {}
        coordinates = point_data.get('coordinates') or [None, None]
        longitude = coordinates[0] if isinstance(coordinates, list) and len(coordinates) > 0 else None
        latitude = coordinates[1] if isinstance(coordinates, list) and len(coordinates) > 1 else None
        
        # Generate slug from clinic name
        clinic_name = cortico_data.get('clinic_name', '')
        slug = cortico_data.get('clinic_slug') or CorticoTransformer._generate_slug(clinic_name)
        
        return {
            'name': clinic_name.strip() if clinic_name else '',
            'slug': slug,
            'facility_type': facility_type,
            'website': cortico_data.get('website'),
            'email': cortico_data.get('email'),
            'phone': CorticoTransformer._clean_phone(cortico_data.get('phone_number')),
            'address_line1': cortico_data.get('clinic_address', ''),
            'city': cortico_data.get('clinic_city', ''),
            'province': cortico_data.get('clinic_province', ''),
            'country': cortico_data.get('clinic_country', 'Canada'),
            'longitude': longitude,
            'latitude': latitude,
            'accepts_new_patients': cortico_data.get('accepts_new_patients', False),
            'is_bookable_online': cortico_data.get('is_bookable_online', False),
            'has_telehealth': cortico_data.get('has_telehealth', False),
            'status': 'active'
        }

    @staticmethod
    def transform_observation(facility_id: str, cortico_data: Dict) -> Dict:
        """Transform Cortico data to facility observation format"""
        # No changes needed here, as it uses simple gets with defaults
        return {
            'facility_id': facility_id,
            'source': 'cortico',
            'source_record_id': str(cortico_data.get('id', '')),
            'booking_url': cortico_data.get('booking_url'),
            'host': cortico_data.get('host'),
            'accepts_new_patients': cortico_data.get('accepts_new_patients'),
            'is_bookable_online': cortico_data.get('is_bookable_online'),
            'has_telehealth': cortico_data.get('has_telehealth'),
            'raw_json': cortico_data,  # Supabase handles JSON automatically
            'observed_at': datetime.now(timezone.utc).isoformat(),
            'confidence': 0.85  # High confidence for Cortico data
        }

    @staticmethod
    def transform_booking_channels(facility_id: str, cortico_data: Dict) -> List[Dict]:
        """Transform Cortico booking data to booking channels"""
        channels = []
        
        booking_url = cortico_data.get('booking_url')
        if booking_url:
            channels.append({
                'facility_id': facility_id,
                'channel_type': 'web',
                'label': 'Online Booking',
                'url': booking_url,
                'external_provider': 'cortico',
                'is_active': True,
                'last_checked_at': datetime.now(timezone.utc).isoformat()
            })
        
        phone = cortico_data.get('phone_number')
        if phone:
            channels.append({
                'facility_id': facility_id,
                'channel_type': 'phone',
                'label': 'Phone Booking',
                'phone': CorticoTransformer._clean_phone(phone),
                'is_active': True,
                'last_checked_at': datetime.now(timezone.utc).isoformat()
            })
        
        return channels

    @staticmethod
    def transform_service_offerings(facility_id: str, workflows: List[Dict]) -> List[Dict]:
        """Transform Cortico workflows to service offerings"""
        # Defensive: Ensure workflows is a list
        workflows = workflows or []
        offerings = []
        
        for workflow in workflows:
            if not isinstance(workflow, dict):
                continue  # Skip invalid workflow entries
            service_slug = CorticoTransformer.WORKFLOW_SERVICE_MAPPING.get(
                workflow.get('slug', '')
            )
            
            if service_slug:
                offerings.append({
                    'facility_id': facility_id,
                    'service_slug': service_slug,  # Will be resolved to service_id later
                    'has_in_person': workflow.get('has_clinic', False),
                    'has_phone': workflow.get('has_phone', False),
                    'has_video': workflow.get('has_video', False),
                    'has_home_visit': workflow.get('has_home_visit', False),
                    'allow_new_patients': workflow.get('allow_new_patients', False),
                    'scope_description': workflow.get('scope_description', '')
                })
        
        return offerings

    @staticmethod
    def transform_availability(facility_id: str, availability_data: Dict) -> List[Dict]:
        """Transform Cortico availability data to availability records"""
        # Defensive: Ensure availability_data is a dict
        availability_data = availability_data or {}
        availability_records = []
        
        for workflow_channel, next_available in availability_data.items():
            if not next_available:
                continue
            
            # Parse workflow and channel from the key (e.g., "family-doctor_clinic")
            parts = workflow_channel.split('_')
            if len(parts) != 2:
                continue
            
            workflow_slug, channel = parts
            service_slug = CorticoTransformer.WORKFLOW_SERVICE_MAPPING.get(workflow_slug)
            
            if service_slug:
                availability_records.append({
                    'facility_id': facility_id,
                    'service_slug': service_slug,  # Will be resolved to service_id later
                    'channel_type': channel,  # 'clinic', 'phone', 'virtual'
                    'next_available_at': next_available,
                    'observed_at': datetime.now(timezone.utc).isoformat(),
                    'source': 'cortico'
                })
        
        return availability_records

    @staticmethod
    def _determine_facility_type(specialties: List[str], workflows: List[Dict]) -> str:
        """Determine facility type based on specialties and workflows"""
        # Defensive: Ensure inputs are iterables
        specialties = specialties or []
        workflows = workflows or []
        
        specialties_str = ' '.join(str(s).lower() for s in specialties if s is not None)
        
        # Check for specific facility types
        if 'emergency' in specialties_str:
            return 'emergency_room'
        elif 'urgent' in specialties_str:
            return 'urgent_care'
        elif 'walk-in' in specialties_str or 'walk in' in specialties_str:
            return 'walk_in_clinic'
        elif 'dental' in specialties_str:
            return 'dental_clinic'
        elif 'mental health' in specialties_str or 'psychology' in specialties_str:
            return 'mental_health_center'
        elif 'rehabilitation' in specialties_str or 'physiotherapy' in specialties_str:
            return 'rehabilitation_center'
        elif 'pharmacy' in specialties_str:
            return 'pharmacy'
        elif 'vision' in specialties_str or 'eye' in specialties_str:
            return 'vision_center'
        
        # Check workflows for additional clues
        workflow_types = [w.get('workflow_type', '') for w in workflows if isinstance(w, dict)]
        workflow_slugs = [w.get('slug', '') for w in workflows if isinstance(w, dict)]
        
        if 'terminal-walk-in' in workflow_types or 'terminal' in workflow_slugs:
            return 'walk_in_clinic'
        elif 'urgent-care' in workflow_slugs:
            return 'urgent_care'
        
        # Default to clinic for most cases
        return 'clinic'

    @staticmethod
    def _generate_slug(name: str) -> str:
        """Generate URL-friendly slug from facility name"""
        if not name:
            return f"facility-{datetime.now().timestamp()}"
        
        slug = re.sub(r'[^\w\s-]', '', name.lower())
        slug = re.sub(r'[-\s]+', '-', slug)
        return slug.strip('-')

    @staticmethod
    def _clean_phone(phone: str) -> Optional[str]:
        """Clean and format phone number"""
        if not phone:
            return None
        
        # Remove all non-digit characters
        digits_only = ''.join(filter(str.isdigit, phone))
        
        # Validate length (assuming North American format)
        if len(digits_only) == 10:
            return f"({digits_only[:3]}) {digits_only[3:6]}-{digits_only[6:]}"
        elif len(digits_only) == 11 and digits_only[0] == '1':
            return f"({digits_only[1:4]}) {digits_only[4:7]}-{digits_only[7:]}"
        
        return phone  # Return original if can't format

class DataValidator:
    """Validates transformed data before database insertion"""
    
    @staticmethod
    def validate_facility(facility_data: Dict) -> tuple[bool, List[str]]:
        """Validate facility data"""
        errors = []
        
        if not facility_data.get('name'):
            errors.append("Facility name is required")
        
        if not facility_data.get('slug'):
            errors.append("Facility slug is required")
        
        if not facility_data.get('facility_type'):
            errors.append("Facility type is required")
        
        # Validate coordinates if present
        longitude = facility_data.get('longitude')
        latitude = facility_data.get('latitude')
        
        if longitude is not None:
            try:
                lng_float = float(longitude)
                if not (-180 <= lng_float <= 180):
                    errors.append("Longitude must be between -180 and 180")
            except (ValueError, TypeError):
                errors.append("Invalid longitude format")
        
        if latitude is not None:
            try:
                lat_float = float(latitude)
                if not (-90 <= lat_float <= 90):
                    errors.append("Latitude must be between -90 and 90")
            except (ValueError, TypeError):
                errors.append("Invalid latitude format")
        
        return len(errors) == 0, errors

    @staticmethod
    def validate_observation(observation_data: Dict) -> tuple[bool, List[str]]:
        """Validate observation data"""
        errors = []
        
        if not observation_data.get('facility_id'):
            errors.append("Facility ID is required")
        
        if not observation_data.get('source'):
            errors.append("Source is required")
        
        if not observation_data.get('observed_at'):
            errors.append("Observed at timestamp is required")
        
        return len(errors) == 0, errors