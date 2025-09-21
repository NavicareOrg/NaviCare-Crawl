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
    def transform_operating_hours(facility_id: str, operating_hours: Optional[Dict]) -> List[Dict]:
        """Transform operating hours map to facility hours records"""
        if not operating_hours:
            return []

        weekday_map = {
            'monday': 0,
            'tuesday': 1,
            'wednesday': 2,
            'thursday': 3,
            'friday': 4,
            'saturday': 5,
            'sunday': 6,
        }

        canonical_weekday_labels = {value: key.capitalize() for key, value in weekday_map.items()}

        skip_patterns = [
            r'^-$',
            r'^closed$',
            r'^by appointment',
            r'^call',
            r'^contact',
            r'^n/?a$',
            r'^tbd',
            r'hours vary',
        ]

        hours_records: List[Dict] = []

        if isinstance(operating_hours, list):
            day_items = enumerate(operating_hours)
        else:
            day_items = operating_hours.items()

        for raw_day, raw_schedule in day_items:
            if raw_day is None:
                continue

            original_day_label = str(raw_day).strip()
            day_key = original_day_label.lower()
            weekday = weekday_map.get(day_key)
            if weekday is None:
                try:
                    weekday = int(day_key)
                except ValueError:
                    continue
                else:
                    if not 0 <= weekday <= 6:
                        continue

            weekday_label = canonical_weekday_labels.get(weekday)
            if not weekday_label:
                weekday_label = original_day_label.title() if original_day_label else None

            if not raw_schedule:
                continue

            schedule = str(raw_schedule).strip()
            if not schedule:
                continue

            normalized = CorticoTransformer._normalize_schedule_text(schedule)
            normalized_lower = normalized.lower()
            if not normalized_lower:
                continue

            if any(re.search(pattern, normalized_lower) for pattern in skip_patterns):
                continue

            if '24/7' in normalized_lower or re.search(r'24\s*(hours?|hrs?)', normalized_lower):
                hours_records.append({
                    'facility_id': facility_id,
                    'weekday': weekday,
                    'weekday_label': weekday_label,
                    'open_time': '00:00',
                    'close_time': '23:59',
                    'notes': None,
                    'slot': 1
                })
                continue

            normalized = normalized.replace(' and ', ', ')
            segments = [segment.strip() for segment in re.split(r'[,;/]', normalized) if segment.strip()]
            day_segments = []

            for segment in segments:
                parsed = CorticoTransformer._parse_hour_segment(segment)
                if not parsed:
                    continue

                open_time, close_time = parsed
                if not open_time or not close_time:
                    continue
                if open_time >= close_time:
                    continue
                if (open_time, close_time) in day_segments:
                    continue

                day_segments.append((open_time, close_time))

            if not day_segments:
                logger.debug("Unable to parse operating hours", extra={'day': raw_day, 'schedule': schedule})
                continue

            for slot_index, (open_time, close_time) in enumerate(day_segments, start=1):
                hours_records.append({
                    'facility_id': facility_id,
                    'weekday': weekday,
                    'weekday_label': weekday_label,
                    'open_time': open_time,
                    'close_time': close_time,
                    'notes': None,
                    'slot': slot_index
                })

        return hours_records

    @staticmethod
    def _normalize_schedule_text(text: str) -> str:
        """Normalize common punctuation and whitespace in schedule text"""
        replacements = {
            '\u2013': '-',
            '\u2014': '-',
            '\u2212': '-',
            '\u2009': ' ',
            '\u200a': ' ',
            '\u200b': ' ',
            '\u00a0': ' ',
        }

        normalized = text
        for target, repl in replacements.items():
            normalized = normalized.replace(target, repl)

        normalized = normalized.replace(' to ', ' - ')
        normalized = re.sub(r'\s+', ' ', normalized)
        return normalized.strip()

    @staticmethod
    def _parse_hour_segment(segment: str) -> Optional[tuple[str, str]]:
        """Parse a single time range segment into 24-hour open/close times"""
        cleaned_segment = segment.strip()
        if not cleaned_segment:
            return None

        cleaned_segment = re.sub(r'\s*-\s*', '-', cleaned_segment)
        match = re.match(
            r'(?P<start>\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm)?)\-(?P<end>\d{1,2}(?::\d{2})?\s*(?:AM|PM|am|pm)?)',
            cleaned_segment
        )

        if not match:
            return None

        start_raw = match.group('start').strip()
        end_raw = match.group('end').strip()

        end_meridiem = CorticoTransformer._extract_meridiem(end_raw)
        start_meridiem = CorticoTransformer._extract_meridiem(start_raw) or end_meridiem

        open_time = CorticoTransformer._to_24h_time(start_raw, start_meridiem)
        close_time = CorticoTransformer._to_24h_time(end_raw, end_meridiem)

        if not open_time or not close_time:
            return None

        return open_time, close_time

    @staticmethod
    def _extract_meridiem(time_str: str) -> Optional[str]:
        """Return AM/PM marker if present in the time string"""
        if not time_str:
            return None

        match = re.search(r'(AM|PM)', time_str, re.IGNORECASE)
        if not match:
            return None

        return match.group(1).upper()

    @staticmethod
    def _to_24h_time(time_str: str, fallback_meridiem: Optional[str]) -> Optional[str]:
        """Convert a time string with optional meridiem to 24-hour HH:MM"""
        if not time_str:
            return None

        cleaned = time_str.strip().upper()
        cleaned = cleaned.replace('.', '')

        meridiem = CorticoTransformer._extract_meridiem(cleaned)
        if meridiem:
            cleaned = re.sub(r'(AM|PM)', '', cleaned)
        else:
            meridiem = fallback_meridiem

        cleaned = cleaned.strip()
        if not cleaned:
            return None

        if ':' in cleaned:
            hour_str, minute_str = cleaned.split(':', 1)
        else:
            hour_str, minute_str = cleaned, '00'

        try:
            hour = int(hour_str)
            minute = int(minute_str)
        except ValueError:
            return None

        if minute < 0 or minute > 59:
            return None

        if meridiem:
            if meridiem == 'AM':
                if hour == 12:
                    hour = 0
            elif meridiem == 'PM':
                if hour != 12:
                    hour += 12

        if hour < 0 or hour > 23:
            return None

        return f"{hour:02d}:{minute:02d}"

    @staticmethod
    def _determine_facility_type(specialties: List[str], workflows: List[Dict]) -> str:
        # """Determine facility type based on specialties and workflows"""
        # # Defensive: Ensure inputs are iterables
        # specialties = specialties or []
        # workflows = workflows or []
        
        # specialties_str = ' '.join(str(s).lower() for s in specialties if s is not None)
        
        # # Check for specific facility types
        # if 'emergency' in specialties_str:
        #     return 'emergency_room'
        # elif 'urgent' in specialties_str:
        #     return 'urgent_care'
        # elif 'walk-in' in specialties_str or 'walk in' in specialties_str:
        #     return 'walk_in_clinic'
        # elif 'dental' in specialties_str:
        #     return 'dental_clinic'
        # elif 'mental health' in specialties_str or 'psychology' in specialties_str:
        #     return 'mental_health_center'
        # elif 'rehabilitation' in specialties_str or 'physiotherapy' in specialties_str:
        #     return 'rehabilitation_center'
        # elif 'pharmacy' in specialties_str:
        #     return 'pharmacy'
        # elif 'vision' in specialties_str or 'eye' in specialties_str:
        #     return 'vision_center'
        
        # # Check workflows for additional clues
        # workflow_types = [w.get('workflow_type', '') for w in workflows if isinstance(w, dict)]
        # workflow_slugs = [w.get('slug', '') for w in workflows if isinstance(w, dict)]
        
        # if 'terminal-walk-in' in workflow_types or 'terminal' in workflow_slugs:
        #     return 'walk_in_clinic'
        # elif 'urgent-care' in workflow_slugs:
        #     return 'urgent_care'
        
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
