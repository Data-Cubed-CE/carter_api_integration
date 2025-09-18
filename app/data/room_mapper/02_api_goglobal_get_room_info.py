import pandas as pd
import requests
import json
import time
import os
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Dict, Optional
import logging
import re

# Configure logging without unicode characters
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('goglobal_rooms_normalized.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class GoGlobalRoomsNormalizer:
    """GoGlobal Rooms Normalizer - mapping to RateHawk-like structure"""
    
    def __init__(self):
        # Initialize API client with environment variables support for Azure
        self.credentials = {
            'agency_id': os.getenv('GOGLOBAL_AGENCY_ID', '164044'),
            'username': os.getenv('GOGLOBAL_USERNAME', 'CARTERXMLTEST'), 
            'password': os.getenv('GOGLOBAL_PASSWORD', 'Q2E4969KJ72')
        }
        self.base_url = 'https://carter.xml.goglobal.travel/xmlwebservice.asmx'
        self.session = requests.Session()
        
        # Set default timeout and retry configuration
        self.session.timeout = 30
        
        print("GoGlobal Rooms Normalizer initialized")
        print(f"Using agency: {self.credentials['agency_id']}, user: {self.credentials['username']}")
    
    def create_hotel_search_request(self, hotel_id: str, arrival_date: str = None, nights: int = 1) -> str:
        """Create XML request with proper authentication"""
        if not arrival_date:
            # Use a date that's always in the future
            future_date = datetime.now() + timedelta(days=30)
            arrival_date = future_date.strftime("%Y-%m-%d")
        
        xml_request = f'''<Root>
<Header>
<Agency>{self.credentials['agency_id']}</Agency>
<User>{self.credentials['username']}</User>
<Password>{self.credentials['password']}</Password>
<Operation>HOTEL_SEARCH_REQUEST</Operation>
<OperationType>Request</OperationType>
</Header>
<Main Version="2.4" ResponseFormat="JSON" IncludeGeo="false" HotelFacilities="true" RoomFacilities="true" Currency="EUR">
<SortOrder>1</SortOrder>
<FilterPriceMin>0</FilterPriceMin>
<FilterPriceMax>100000</FilterPriceMax>
<MaximumWaitTime>15</MaximumWaitTime>
<MaxResponses>1000</MaxResponses>
<Nationality>PL</Nationality>
<Hotels>
<HotelId>{hotel_id}</HotelId>
</Hotels>
<ArrivalDate>{arrival_date}</ArrivalDate>
<Nights>{nights}</Nights>
<Rooms>
<Room Adults="2" RoomCount="1" ChildCount="0">
</Room>
</Rooms>
</Main>
</Root>'''
        return xml_request
    
    def create_soap_envelope(self, xml_request: str) -> str:
        """Create SOAP envelope"""
        soap_envelope = f'''<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" 
                 xmlns:xsd="http://www.w3.org/2001/XMLSchema" 
                 xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
    <soap12:Body>
        <MakeRequest xmlns="http://www.goglobal.travel/">
            <requestType>11</requestType>
            <xmlRequest><![CDATA[{xml_request}]]></xmlRequest>
        </MakeRequest>
    </soap12:Body>
</soap12:Envelope>'''
        return soap_envelope
    
    def make_request(self, xml_request: str) -> Optional[Dict]:
        """Make request to GoGlobal API with enhanced error handling"""
        soap_envelope = self.create_soap_envelope(xml_request)
        
        headers = {
            'Content-Type': 'application/soap+xml; charset=utf-8',
            'User-Agent': 'Azure-Function-GoGlobal-Client/1.0',
            'Accept': '*/*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive'
        }
        
        try:
            logger.info("Making HOTEL_SEARCH_REQUEST to GoGlobal API")
            
            response = self.session.post(
                self.base_url,
                data=soap_envelope,
                headers=headers,
                timeout=30
            )
            
            logger.info(f"Response status: {response.status_code}")
            logger.debug(f"Response headers: {dict(response.headers)}")
            
            if response.status_code == 200:
                return self.parse_soap_response(response.text)
            else:
                logger.error(f"HTTP {response.status_code}: {response.text[:500]}")
                return None
                
        except requests.exceptions.Timeout:
            logger.error("Request timeout - GoGlobal API took too long to respond")
            return None
        except requests.exceptions.ConnectionError:
            logger.error("Connection error - unable to reach GoGlobal API")
            return None
        except Exception as e:
            logger.error(f"Request error: {e}")
            return None
    
    def parse_soap_response(self, response_text: str) -> Optional[Dict]:
        """Parse SOAP response and extract JSON data - ENHANCED VERSION"""
        try:
            # Parse XML
            root = ET.fromstring(response_text)
            
            # Debug: Log the response structure
            logger.debug(f"Response root tag: {root.tag}")
            
            # Find MakeRequestResult element
            result_elem = None
            
            # Method 1: Search by tag ending
            for elem in root.iter():
                if elem.tag.endswith('MakeRequestResult'):
                    result_elem = elem
                    break
            
            # Method 2: Search for element containing JSON (starts with {)
            if result_elem is None:
                for elem in root.iter():
                    if elem.text and elem.text.strip().startswith('{'):
                        result_elem = elem
                        break
            
            # Method 3: Check if the response itself is an error response
            if result_elem is None:
                # Check for error elements in the response
                error_elements = []
                for elem in root.iter():
                    if elem.text and any(keyword in elem.text.lower() for keyword in ['error', 'fault', 'exception']):
                        error_elements.append(f"{elem.tag}: {elem.text}")
                
                if error_elements:
                    logger.error(f"API returned error: {'; '.join(error_elements)}")
                    return None
                
                # Log the structure for debugging
                sample_elements = []
                for elem in list(root.iter())[:10]:
                    sample_elements.append(f"{elem.tag}: {elem.text[:50] if elem.text else 'None'}")
                logger.debug(f"Sample response elements: {sample_elements}")
                
                # Check if this is a Root response (which suggests an error)
                if root.tag.endswith('Root') or 'Root' in root.tag:
                    logger.warning("Received Root response instead of JSON - likely authentication/request error")
                    
                    # Try to extract error information from Root response
                    header_elem = root.find('.//Header') or root.find('.//*[local-name()="Header"]')
                    if header_elem is not None:
                        user_elem = header_elem.find('.//User') or header_elem.find('.//*[local-name()="User"]')
                        if user_elem is not None and not user_elem.text:
                            logger.error("Authentication failed - User field is empty in response")
                    
                    return None
                
                logger.error("MakeRequestResult not found in response")
                return None
            
            json_text = result_elem.text
            if not json_text or not json_text.strip():
                logger.warning("Empty JSON content in MakeRequestResult")
                return {"Hotels": []}
            
            # Clean and validate JSON text
            json_text = json_text.strip()
            
            # Check if it looks like valid JSON
            if not json_text.startswith('{'):
                logger.warning(f"Response doesn't start with '{{': {json_text[:50]}")
                return {"Hotels": []}
            
            # Try to parse JSON
            try:
                json_data = json.loads(json_text)
                logger.info(f"✓ JSON parsed successfully, keys: {list(json_data.keys())}")
                
                # Validate structure
                if 'Hotels' not in json_data:
                    logger.warning("No 'Hotels' key in response")
                    return {"Hotels": []}
                
                hotels = json_data.get('Hotels', [])
                if not hotels:
                    logger.warning("No hotels in response")
                    return {"Hotels": []}
                
                logger.info(f"Found {len(hotels)} hotel(s) in response")
                return json_data
                
            except json.JSONDecodeError as e:
                logger.warning(f"JSON decode error: {e}")
                logger.debug(f"Failed JSON text: {json_text[:200]}...")
                
                # Sometimes the response contains escaped or malformed JSON
                # Try some basic fixes
                try:
                    # Replace HTML entities
                    cleaned_json = json_text.replace('&lt;', '<').replace('&gt;', '>').replace('&amp;', '&')
                    json_data = json.loads(cleaned_json)
                    logger.info("✓ JSON parsed after cleaning HTML entities")
                    return json_data
                except:
                    logger.warning("Failed to parse JSON even after cleaning")
                    return {"Hotels": []}
                
        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            logger.debug(f"Failed XML: {response_text[:500]}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error in response parsing: {e}")
            return None
    
    def extract_bedding_config_id(self, room_name: str) -> int:
        """Extract bedding config ID from room name"""
        room_name_lower = room_name.lower()
        
        if "bunk" in room_name_lower:
            return 1  # bunk bed
        elif "single" in room_name_lower or "1 twin" in room_name_lower:
            return 2  # single bed
        elif "double" in room_name_lower or "king" in room_name_lower or "queen" in room_name_lower:
            return 3  # double
        elif "twin" in room_name_lower and ("2 twin" in room_name_lower or "twin bed" in room_name_lower):
            return 4  # twin
        elif "multiple" in room_name_lower or "3 twin" in room_name_lower:
            return 7  # multiple
        else:
            return 0  # undefined
    
    # Add RateHawk mapping functions
    def map_room_class(self, class_id: int) -> str:
        """Map room class ID to descriptive name"""
        class_mapping = {
            0: "run of house",
            1: "dorm", 
            2: "capsule",
            3: "room",
            4: "junior suite",
            5: "suite",
            6: "apartment",
            7: "studio",
            8: "villa",
            9: "cottage",
            17: "bungalow",
            18: "chalet",
            19: "camping",
            20: "tent"
        }
        return class_mapping.get(class_id, f"unknown_{class_id}")
    
    def map_room_quality(self, quality_id: int) -> str:
        """Map room quality ID to descriptive name"""
        quality_mapping = {
            0: "undefined",
            1: "economy",
            2: "standard", 
            3: "comfort",
            4: "business",
            5: "superior",
            6: "deluxe",
            7: "premier",
            8: "executive",
            9: "presidential",
            17: "premium",
            18: "classic",
            19: "ambassador",
            20: "grand",
            21: "luxury",
            22: "platinum",
            23: "prestige",
            24: "privilege",
            25: "royal"
        }
        return quality_mapping.get(quality_id, f"unknown_{quality_id}")
    
    def map_capacity(self, capacity_id: int) -> str:
        """Map capacity ID to descriptive name"""
        capacity_mapping = {
            0: "undefined",
            1: "single",
            2: "double",
            3: "triple", 
            4: "quadruple",
            5: "quintuple",
            6: "sextuple"
        }
        return capacity_mapping.get(capacity_id, f"unknown_{capacity_id}")
    
    def map_bathroom_type(self, bathroom_id: int) -> str:
        """Map bathroom type ID to descriptive name"""
        bathroom_mapping = {
            0: "undefined",
            1: "shared bathroom",
            2: "private bathroom",
            3: "external private bathroom"
        }
        return bathroom_mapping.get(bathroom_id, f"unknown_{bathroom_id}")
    
    def map_bedding_config(self, bedding_id: int) -> str:
        """Map bedding config ID to descriptive name"""
        bedding_mapping = {
            0: "undefined",
            1: "bunk bed",
            2: "single bed",
            3: "double",
            4: "twin",
            7: "multiple"
        }
        return bedding_mapping.get(bedding_id, f"unknown_{bedding_id}")
    
    def map_sex_restriction(self, sex_id: int) -> str:
        """Map sex restriction ID to descriptive name"""
        sex_mapping = {
            0: "undefined",
            1: "male",
            2: "female",
            3: "mixed"
        }
        return sex_mapping.get(sex_id, f"unknown_{sex_id}")
    
    def map_bedrooms_count(self, bedrooms_id: int) -> str:
        """Map bedrooms count ID to descriptive name"""
        bedrooms_mapping = {
            0: "undefined",
            1: "1 bedroom",
            2: "2 bedrooms",
            3: "3 bedrooms",
            4: "4 bedrooms",
            5: "5 bedrooms",
            6: "6 bedrooms"
        }
        return bedrooms_mapping.get(bedrooms_id, f"unknown_{bedrooms_id}")
    
    def map_room_view(self, view_id: int) -> str:
        """Map room view ID to descriptive name"""
        view_mapping = {
            0: "undefined", 1: "bay view", 2: "bosphorus view", 3: "burj-khalifa view",
            4: "canal view", 5: "city view", 6: "courtyard view", 7: "dubai-marina view",
            8: "garden view", 9: "golf view", 17: "harbour view", 18: "inland view",
            19: "kremlin view", 20: "lake view", 21: "land view", 22: "mountain view",
            23: "ocean view", 24: "panoramic view", 25: "park view", 26: "partial-ocean view",
            27: "partial-sea view", 28: "partial view", 29: "pool view", 30: "river view",
            31: "sea view", 32: "sheikh-zayed view", 33: "street view", 34: "sunrise view",
            35: "sunset view", 36: "water view", 37: "with view", 38: "beachfront",
            39: "ocean front", 40: "sea front"
        }
        return view_mapping.get(view_id, f"unknown_{view_id}")
    
    def map_room_floor(self, floor_id: int) -> str:
        """Map room floor ID to descriptive name"""
        floor_mapping = {
            0: "undefined",
            1: "penthouse floor",
            2: "duplex floor", 
            3: "basement floor",
            4: "attic floor"
        }
        return floor_mapping.get(floor_id, f"unknown_{floor_id}")

    def extract_bedding_info(self, room_name: str) -> Dict[str, str]:
        """Extract bedding information from room name - ENHANCED"""
        room_name_lower = room_name.lower()
        
        # Enhanced bedding type detection
        if "1 king" in room_name_lower or "king bed" in room_name_lower:
            return {"bedding_type": "1 King Bed", "main_name": "King Room"}
        elif "2 king" in room_name_lower:
            return {"bedding_type": "2 King Beds", "main_name": "Twin King Room"}
        elif "1 queen" in room_name_lower or "queen bed" in room_name_lower:
            return {"bedding_type": "1 Queen Bed", "main_name": "Queen Room"}
        elif "2 queen" in room_name_lower or "2queens" in room_name_lower:
            return {"bedding_type": "2 Queen Beds", "main_name": "Twin Queen Room"}
        elif "1 double" in room_name_lower or "double bed" in room_name_lower:
            return {"bedding_type": "1 Double Bed", "main_name": "Double Room"}
        elif "2 double" in room_name_lower or "2doubles" in room_name_lower:
            return {"bedding_type": "2 Double Beds", "main_name": "Twin Double Room"}
        elif "twin" in room_name_lower:
            if "2 twin" in room_name_lower:
                return {"bedding_type": "2 Twin Beds", "main_name": "Twin Room"}
            elif "3 twin" in room_name_lower:
                return {"bedding_type": "3 Twin Beds", "main_name": "Triple Room"}
            else:
                return {"bedding_type": "Twin Beds", "main_name": "Twin Room"}
        elif "suite" in room_name_lower:
            return {"bedding_type": "Suite Configuration", "main_name": "Suite"}
        elif "villa" in room_name_lower:
            return {"bedding_type": "Villa Configuration", "main_name": "Villa"}
        elif "bungalow" in room_name_lower:
            return {"bedding_type": "Bungalow Configuration", "main_name": "Bungalow"}
        else:
            return {"bedding_type": "Standard Configuration", "main_name": "Standard Room"}
    
    def extract_view_code(self, room_name: str) -> int:
        """Extract view code matching RateHawk style"""
        room_name_lower = room_name.lower()
        
        if "ocean view" in room_name_lower or "oceanvw" in room_name_lower or "sea view" in room_name_lower:
            return 23  # Ocean view
        elif "partial ocean" in room_name_lower or "part ov" in room_name_lower:
            return 26  # Partial ocean view
        elif "garden view" in room_name_lower or "garden vw" in room_name_lower:
            return 8   # Garden view
        elif "limited view" in room_name_lower:
            return 28  # Limited view
        elif "seafront" in room_name_lower:
            return 23  # Seafront = ocean view
        elif "city view" in room_name_lower:
            return 5   # City view
        elif "mountain view" in room_name_lower:
            return 15  # Mountain view
        else:
            return 0   # No specific view
    
    def extract_room_amenities(self, room_facilities: List[str], room_name: str) -> str:
        """Extract and normalize room amenities matching RateHawk style"""
        amenities = []
        
        # Facility mapping to match RateHawk format
        facility_mapping = {
            "Bathroom": "private-bathroom",
            "Air conditioning": "air-conditioning", 
            "TV": "tv",
            "Satellite/cable TV": "satellite-tv",
            "Minibar": "mini-bar",
            "Safe": "safe",
            "Balcony/Terrace": "balcony",
            "Hairdryer": "hairdryer",
            "Internet access": "wi-fi",
            "WLAN access": "wi-fi",
            "Tea/coffee maker": "tea-or-coffee",
            "Phone": "phone",
            "Radio": "radio",
            "Wake up service": "wake-up-service",
            "Iron/ironing board": "iron"
        }
        
        # Map facilities
        for facility in room_facilities:
            if facility in facility_mapping:
                amenities.append(facility_mapping[facility])
        
        # Add amenities based on room name
        room_name_lower = room_name.lower()
        if "private pool" in room_name_lower:
            amenities.append("private-pool")
        if "balcony" in room_name_lower or "terrace" in room_name_lower:
            amenities.append("balcony")
        if "pool access" in room_name_lower:
            amenities.append("pool-access")
        if "spa" in room_name_lower:
            amenities.append("spa-access")
        if "butler" in room_name_lower:
            amenities.append("butler-service")
            
        # Always add private bathroom if not already included
        if "private-bathroom" not in amenities:
            amenities.append("private-bathroom")
            
        return ", ".join(sorted(set(amenities)))
    
    def extract_capacity_bedrooms_bathrooms(self, room_name: str, room_facilities: List[str]) -> tuple:
        """Extract capacity, bedrooms, and bathrooms as IDs for mapping"""
        room_name_lower = room_name.lower()
        
        # Default values - 0 for undefined when cannot be determined
        capacity_id = 0
        bedrooms_id = 0
        bathrooms_id = 0
        
        # Capacity detection - return ID for mapping
        if "triple" in room_name_lower or "3 twin" in room_name_lower:
            capacity_id = 3  # triple
        elif "2 queen" in room_name_lower or "2 double" in room_name_lower:
            capacity_id = 4  # quadruple
        elif "1 king" in room_name_lower or "1 queen" in room_name_lower or "1 double" in room_name_lower:
            capacity_id = 2  # double
        elif "family" in room_name_lower:
            if "6" in room_name_lower:
                capacity_id = 6  # sextuple
            elif "4" in room_name_lower:
                capacity_id = 4  # quadruple
            else:
                capacity_id = 0  # undefined
        elif "villa" in room_name_lower:
            capacity_id = 4  # quadruple (villas typically accommodate more people)
        elif "suite" in room_name_lower:
            capacity_id = 3  # triple (suites typically accommodate more people)
        else:
            capacity_id = 0  # undefined
        
        # Bedrooms detection - return ID for mapping
        if "1 bedroom" in room_name_lower or "1-bedroom" in room_name_lower:
            bedrooms_id = 1
        elif "2 bedroom" in room_name_lower or "2-bedroom" in room_name_lower:
            bedrooms_id = 2
        elif "3 bedroom" in room_name_lower or "3-bedroom" in room_name_lower:
            bedrooms_id = 3
        elif "4 bedroom" in room_name_lower or "4-bedroom" in room_name_lower:
            bedrooms_id = 4
        elif "villa" in room_name_lower:
            bedrooms_id = 2  # Villas typically have multiple bedrooms
        elif "suite" in room_name_lower:
            if "junior" in room_name_lower:
                bedrooms_id = 1
            else:
                bedrooms_id = 1  # Regular suites typically have separate bedroom
        else:
            bedrooms_id = 0  # undefined
        
        # Bathrooms detection - not typically available in GoGlobal room names
        # Set to 0 (undefined) as this info is rarely available
        bathrooms_id = 0
        
        return capacity_id, bedrooms_id, bathrooms_id
    
    def extract_room_class_quality(self, room_name: str) -> tuple:
        """Extract room class and quality IDs matching RateHawk codes"""
        room_name_lower = room_name.lower()
        
        if "villa" in room_name_lower:
            return (8, 8)  # Villa - highest quality
        elif "presidential suite" in room_name_lower:
            return (5, 9)  # Presidential suite
        elif "suite" in room_name_lower:
            if "junior" in room_name_lower:
                return (4, 4)  # Junior suite
            elif "family" in room_name_lower:
                return (5, 6)  # Family suite
            elif "luxury" in room_name_lower or "royal" in room_name_lower:
                return (5, 21)  # Luxury suite
            else:
                return (5, 5)  # Regular suite
        elif "bungalow" in room_name_lower:
            if "ocean" in room_name_lower or "seafront" in room_name_lower:
                return (17, 6)  # Premium bungalow
            else:
                return (17, 4)  # Standard bungalow
        elif "club" in room_name_lower:
            return (3, 6)  # Club room
        elif "deluxe" in room_name_lower:
            return (3, 6)  # Deluxe
        elif "superior" in room_name_lower:
            return (3, 5)  # Superior
        elif "premium" in room_name_lower:
            return (3, 17)  # Premium
        elif "family" in room_name_lower:
            return (3, 4)  # Family room
        elif "standard" in room_name_lower:
            return (3, 2)  # Standard
        else:
            return (3, 2)  # Default to standard room
    
    def extract_special_features(self, room_name: str) -> Dict[str, int]:
        """Extract special features as binary flags"""
        room_name_lower = room_name.lower()
        
        return {
            'balcony': 1 if ('balcony' in room_name_lower or 'terrace' in room_name_lower) else 0,
            'floor': 0,  # Not typically available in GoGlobal
            'view': self.extract_view_code(room_name),
            'club': 1 if 'club' in room_name_lower else 0,
            'family': 1 if 'family' in room_name_lower else 0,
            'sex': 0  # Not applicable for GoGlobal
        }
    
    def normalize_room_data(self, hotel_data: Dict, hotel_code: str, hotel_name: str, reference_id: str, ref_hotel_name: str) -> List[Dict]:
        """Normalize GoGlobal data to match RateHawk structure exactly"""
        rooms_data = []
        extracted_at = datetime.now().isoformat()
        
        offers = hotel_data.get('Offers', [])
        if not offers:
            logger.warning(f"No offers for hotel {hotel_code}")
            return rooms_data
        
        # Group offers by room name to create room_groups similar to RateHawk
        room_groups = {}
        for offer in offers:
            room_names = offer.get('Rooms', ['Standard Room'])
            for room_name in room_names:
                if room_name not in room_groups:
                    room_groups[room_name] = []
                room_groups[room_name].append(offer)
        
        logger.info(f"Found {len(room_groups)} unique room types")
        
        # Create records for each room type matching EXACT RateHawk CSV structure
        for room_group_id, (room_name, room_offers) in enumerate(room_groups.items(), 1):
            
            # Extract room information
            bedding_info = self.extract_bedding_info(room_name)
            capacity_id, bedrooms_id, bathrooms_id = self.extract_capacity_bedrooms_bathrooms(room_name, hotel_data.get('RoomFacilities', []))
            room_class_id, room_quality_id = self.extract_room_class_quality(room_name)
            special_features = self.extract_special_features(room_name)
            room_amenities = self.extract_room_amenities(hotel_data.get('RoomFacilities', []), room_name)
            
            # Build room record matching EXACT RateHawk CSV structure
            room_data = {
                # Reference hotel data
                'reference_id': reference_id,
                'ref_hotel_name': ref_hotel_name,
                
                # API hotel data
                'hotel_id': hotel_code,
                'hotel_name': hotel_name,
                
                # Room basic data
                'room_group_id': room_group_id,
                'room_name': room_name,
                'main_name': bedding_info['main_name'],
                'bedding_type': bedding_info['bedding_type'],
                'bathroom_info': 'Private Bathroom',  # GoGlobal default
                
                # Room classification with both ID and mapped values
                'room_class_id': room_class_id,
                'room_class': self.map_room_class(room_class_id),
                'room_quality_id': room_quality_id,
                'room_quality': self.map_room_quality(room_quality_id),
                'room_capacity_id': capacity_id,
                'room_capacity': self.map_capacity(capacity_id),
                'bedrooms_count_id': bedrooms_id,
                'bedrooms_count': self.map_bedrooms_count(bedrooms_id),
                'bathroom_type_id': 2,  # Private bathroom for GoGlobal
                'bathroom_type': self.map_bathroom_type(2),
                'bedding_config_id': self.extract_bedding_config_id(room_name),
                'bedding_config': self.map_bedding_config(self.extract_bedding_config_id(room_name)),
                'sex_restriction_id': special_features['sex'],
                'sex_restriction': self.map_sex_restriction(special_features['sex']),
                'family_room_id': special_features['family'],
                'family_room': 'family' if special_features['family'] == 1 else 'not family',
                'club_room_id': special_features['club'],
                'club_room': 'club' if special_features['club'] == 1 else 'not club',
                'balcony_id': special_features['balcony'],
                'balcony': 'balcony' if special_features['balcony'] == 1 else 'no balcony',
                'room_view_id': special_features['view'],
                'room_view': self.map_room_view(special_features['view']),
                'room_floor_id': special_features['floor'],
                'room_floor': self.map_room_floor(special_features['floor']),
                
                # Room amenities
                'room_amenities': room_amenities,
                'room_amenities_count': len(room_amenities.split(', ')) if room_amenities else 0,
                
                # Metadata
                'api_source': 'goglobal',
                'extracted_at': extracted_at
            }
            
            rooms_data.append(room_data)
        
        logger.info(f"Normalized {len(rooms_data)} room records for hotel {reference_id}")
        return rooms_data
    
    def search_hotel(self, hotel_id: str) -> Optional[Dict]:
        """Search specific hotel with retry logic"""
        max_retries = 3
        for attempt in range(max_retries):
            try:
                xml_request = self.create_hotel_search_request(hotel_id)
                data = self.make_request(xml_request)
                
                if data and isinstance(data, dict):
                    hotels = data.get('Hotels', [])
                    if hotels:
                        return hotels[0]
                
                # If no data and not the last attempt, wait and retry
                if attempt < max_retries - 1:
                    logger.info(f"Attempt {attempt + 1} failed for hotel {hotel_id}, retrying...")
                    time.sleep(5)
                
            except Exception as e:
                logger.error(f"Error in attempt {attempt + 1} for hotel {hotel_id}: {e}")
                if attempt < max_retries - 1:
                    time.sleep(5)
        
        return None
    
    def load_hotel_mappings(self, csv_path: str) -> pd.DataFrame:
        """Load hotel mappings CSV"""
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} hotel mappings")
            return df
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            raise
    
    def filter_goglobal_hotels(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter hotels with GoGlobal matches"""
        filtered = df[
            (df['goglobal_matched'] == True) & 
            (df['goglobal_hotel_id'].notna()) & 
            (df['goglobal_hotel_id'] != '')
        ].copy()
        
        logger.info(f"Found {len(filtered)} GoGlobal hotels to process")
        return filtered
    
    def process_all_hotels(self, hotels_df: pd.DataFrame, batch_size: int = 3, delay: float = 5.0) -> List[Dict]:
        """Process ALL hotels with enhanced error handling and slower pace"""
        all_rooms_data = []
        total_hotels = len(hotels_df)
        processed_count = 0
        success_count = 0
        error_count = 0
        
        print(f"\n Starting extraction for {total_hotels} hotels...")
        print(f" Batch size: {batch_size}, Delay between batches: {delay}s")
        print("=" * 60)
        
        for i in range(0, total_hotels, batch_size):
            batch_end = min(i + batch_size, total_hotels)
            batch = hotels_df.iloc[i:batch_end]
            batch_num = i // batch_size + 1
            total_batches = (total_hotels - 1) // batch_size + 1
            
            print(f"\n Batch {batch_num}/{total_batches} (Hotels {i+1}-{batch_end})")
            logger.info(f"Processing batch {batch_num}/{total_batches} (hotels {i+1}-{batch_end} of {total_hotels})")
            
            batch_success = 0
            batch_errors = 0
            
            for idx, row in batch.iterrows():
                hotel_id = str(int(row['goglobal_hotel_id']))
                hotel_name = row['goglobal_hotel_name']
                reference_id = row['reference_id']
                ref_hotel_name = row['ref_hotel_name']
                processed_count += 1
                
                print(f"   [{processed_count}/{total_hotels}] {reference_id} | {hotel_id} - {hotel_name[:50]}...")
                logger.info(f"Fetching rooms for hotel {hotel_id} ({hotel_name}) [Reference: {reference_id}]")
                
                try:
                    hotel_data = self.search_hotel(hotel_id)
                    if hotel_data:
                        rooms_data = self.normalize_room_data(hotel_data, hotel_id, hotel_name, reference_id, ref_hotel_name)
                        all_rooms_data.extend(rooms_data)
                        batch_success += 1
                        success_count += 1
                        print(f"     Extracted {len(rooms_data)} rooms")
                        logger.info(f"Extracted {len(rooms_data)} room types for hotel {hotel_id} [Reference: {reference_id}]")
                    else:
                        batch_errors += 1
                        error_count += 1
                        print(f"     No data retrieved")
                        logger.warning(f"No data retrieved for hotel {hotel_id} [Reference: {reference_id}]")
                
                except Exception as e:
                    batch_errors += 1
                    error_count += 1
                    print(f"     Error: {str(e)[:50]}...")
                    logger.error(f"Error processing hotel {hotel_id} [Reference: {reference_id}]: {e}")
                
                # Longer delay between individual requests to avoid rate limiting
                time.sleep(15)
            
            # Batch summary
            print(f"   Batch {batch_num} complete:  {batch_success} success,  {batch_errors} errors")
            
            # Progress summary
            remaining = total_hotels - processed_count
            success_rate = (success_count / processed_count) * 100
            print(f"   Overall progress: {processed_count}/{total_hotels} ({success_rate:.1f}% success rate)")
            print(f"   Estimated remaining: {remaining} hotels")
            
            # Longer delay between batches
            if batch_end < total_hotels:
                print(f"   Waiting {delay} seconds before next batch...")
                logger.info(f"Batch completed. Waiting {delay} seconds before next batch...")
                time.sleep(delay)
        
        # Final summary
        print("\n" + "=" * 60)
        print(f" EXTRACTION COMPLETE!")
        print(f" Final Statistics:")
        print(f"   • Total hotels processed: {processed_count}")
        print(f"   • Successful extractions: {success_count}")
        print(f"   • Failed extractions: {error_count}")
        print(f"   • Success rate: {(success_count/processed_count)*100:.1f}%")
        print(f"   • Total rooms extracted: {len(all_rooms_data)}")
        print("=" * 60)
        
        return all_rooms_data
    
    def save_to_csv(self, rooms_data: List[Dict], output_path: str):
        """Save extracted room data to CSV with enhanced summary"""
        if not rooms_data:
            logger.warning("No room data to save")
            return
        
        df = pd.DataFrame(rooms_data)
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Saved {len(rooms_data)} room records to {output_path}")
        
        # Enhanced summary statistics
        summary = {
            'total_rooms': len(rooms_data),
            'unique_hotels': df['hotel_id'].nunique(),
            'unique_reference_hotels': df['reference_id'].nunique(),
            'extraction_date': datetime.now().isoformat(),
            'api_source': 'goglobal',
            'rooms_per_hotel': {
                'min': int(df.groupby('hotel_id').size().min()),
                'max': int(df.groupby('hotel_id').size().max()),
                'avg': float(df.groupby('hotel_id').size().mean()),
                'median': float(df.groupby('hotel_id').size().median())
            },
            'hotel_details': df.groupby('reference_id').agg({
                'ref_hotel_name': 'first',
                'hotel_id': 'first',
                'hotel_name': 'first',
                'room_group_id': 'count'
            }).rename(columns={'room_group_id': 'room_count'}).to_dict('index')
        }
        
        summary_path = output_path.replace('.csv', '_summary.json')
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Summary: {summary}")
        
        # Print file locations
        print(f"\n Files saved:")
        print(f"   • Room data CSV: {os.path.abspath(output_path)}")
        print(f"   • Summary JSON: {os.path.abspath(summary_path)}")
    
    def test_connection(self) -> bool:
        """Test connection with sample hotel"""
        print("\n🔧 Testing GoGlobal API connection...")
        
        # Test with a different hotel ID first
        test_hotel_ids = ["897336", "825082", "57302"]  # From your mappings CSV
        
        for hotel_id in test_hotel_ids:
            print(f"   Testing hotel ID: {hotel_id}")
            xml_request = self.create_hotel_search_request(hotel_id)
            data = self.make_request(xml_request)
            
            if data and data.get('Hotels'):
                print(f"   ✓ Connection successful with hotel {hotel_id}!")
                return True
            else:
                print(f"   ✗ Failed with hotel {hotel_id}")
        
        print("   ✗ All test hotels failed - check credentials/configuration")
        return False


def main():
    """Main execution function"""
    # Configuration
    CSV_INPUT_PATH = r'.\app\data\hotel_mappings.csv'
    CSV_OUTPUT_PATH = r'.\app\data\02_api_goglobal_rooms.csv'
    
    # Slower processing to avoid rate limiting
    BATCH_SIZE = 3  # Smaller batches
    BATCH_DELAY = 5.0  # Longer delays
    API_SOURCE = 'goglobal'
    
    # Debug: Check environment and paths
    print("=== INITIALIZATION ===")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Input file exists: {os.path.exists(CSV_INPUT_PATH)}")
    print(f"Input file path: {os.path.abspath(CSV_INPUT_PATH)}")
    print("=====================")
    
    try:
        # Initialize normalizer
        normalizer = GoGlobalRoomsNormalizer()
        print("✅ Normalizer initialized successfully")
        
        # Test connection
        if not normalizer.test_connection():
            print(" API connection failed - check credentials/network")
            print("\n Troubleshooting suggestions:")
            print("   1. Verify GOGLOBAL_USERNAME and GOGLOBAL_PASSWORD environment variables")
            print("   2. Check if your IP is whitelisted by GoGlobal")
            print("   3. Verify the API credentials are still valid")
            return
        
        # Load and filter hotel mappings
        print("\n Loading hotel mappings...")
        logger.info("Starting GoGlobal rooms extraction process...")
        mappings_df = normalizer.load_hotel_mappings(CSV_INPUT_PATH)
        filtered_hotels = normalizer.filter_goglobal_hotels(mappings_df)
        
        if filtered_hotels.empty:
            print(f" No hotels found for API source: {API_SOURCE}")
            logger.warning(f"No hotels found for API source: {API_SOURCE}")
            return
        
        print(f" Found {len(filtered_hotels)} hotels to process")
        
        # Ask for confirmation for large datasets
        if len(filtered_hotels) > 10:
            response = input(f"\n This will process {len(filtered_hotels)} hotels. Continue? (y/N): ")
            if response.lower() != 'y':
                print(" Process cancelled by user")
                return
        
        # Process ALL hotels
        logger.info(f"Starting extraction for {len(filtered_hotels)} hotels...")
        rooms_data = normalizer.process_all_hotels(
            filtered_hotels, 
            batch_size=BATCH_SIZE, 
            delay=BATCH_DELAY
        )
        
        # Save results
        print(f"\n Saving results...")
        normalizer.save_to_csv(rooms_data, CSV_OUTPUT_PATH)
        
        print("\n PROCESS COMPLETED SUCCESSFULLY!")
        logger.info("GoGlobal extraction process completed successfully!")
        
    except KeyboardInterrupt:
        print("\n Process interrupted by user")
        logger.info("Process interrupted by user")
    except Exception as e:
        print(f"\n FATAL ERROR: {e}")
        logger.error(f"Fatal error during extraction: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()