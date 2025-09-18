import json
import xml.etree.ElementTree as ET
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging
import time
import aiohttp

from app.services.universal_provider import ProviderAdapter
from app.config import Config

logger = logging.getLogger(__name__)


class GoGlobalProvider(ProviderAdapter):
    """Simplified GoGlobal Provider - Clean, elegant, minimal"""
    
    def __init__(self, provider_name: str = "goglobal"):
        super().__init__(provider_name)  # Use parent's initialization
        self._load_config()
        
    def _load_config(self):
        """Load credentials from config"""
        from app.config import config
        
        goglobal_config = config.get_provider_config('goglobal')
        if not goglobal_config:
            raise ValueError("GoGlobal configuration not found")
            
        self.credentials = {
            'agency_id': goglobal_config.get('agency_id'),
            'username': goglobal_config.get('username'), 
            'password': goglobal_config.get('password')
        }
        
        # Validate required credentials
        missing = [k for k, v in self.credentials.items() if not v]
        if missing:
            raise ValueError(f"Missing GoGlobal credentials: {missing}")
            
        self.base_url = goglobal_config.get('base_url', 'https://carter.xml.goglobal.travel/xmlwebservice.asmx')
        
        logger.debug(f"[PROVIDERS] GoGlobal Provider initialized - Agency: {self.credentials['agency_id']}")

    async def search(self, criteria: dict) -> dict:
        """
        Main search method - unified interface
        
        Input: Standard criteria dict (with hotel_names list)
        Output: Standard response dict with status/data/error
        """
        start_time = time.time()
        
        try:
            # Simple logging
            hotel_names = criteria.get("hotel_names", [])
            
            # Debug logging
            logger.debug(f"GoGlobal search called with criteria type: {type(criteria)}, value: {criteria}")
            
            # Validate input
            if not criteria or not isinstance(criteria, dict):
                return {"status": "error", "error": "Invalid criteria provided"}
            
            # 1. Map hotel names to GoGlobal IDs
            hotel_names = criteria.get("hotel_names", [])
            if not hotel_names:
                return {"status": "error", "error": "No hotel_names in criteria"}
            
            # Collect all valid hotel IDs
            hotel_ids = []
            hotel_name_to_id_map = {}
            
            for hotel_name in hotel_names:
                hotel_id = self._get_hotel_id(hotel_name)
                if hotel_id:
                    hotel_ids.append(hotel_id)
                    hotel_name_to_id_map[hotel_id] = hotel_name
                    # Remove duplicate log - hotel_mapping_service already logs this
                else:
                    logger.warning(f"GoGlobal: Hotel '{hotel_name}' not found in mappings - skipping")
            
            if not hotel_ids:
                return {"status": "error", "error": "No hotels found in GoGlobal mappings"}
            
            # 2. Prepare search parameters with all hotel IDs
            search_params = self._prepare_search_params(criteria, hotel_ids=hotel_ids)
            search_params['hotel_name_to_id_map'] = hotel_name_to_id_map
            logger.debug(f"GoGlobal multi-hotel search_params: {search_params}")
            
            # 3. Make single API call with all hotels
            result = await self._make_api_call(search_params)
            
            logger.debug(f"GoGlobal API result: {type(result)}")
            
            if result and isinstance(result, dict):
                # Simple results summary  
                search_time = (time.time() - start_time) * 1000
                hotels_found = len(result.get('hotels', []))
                
                logger.debug(f"[PROVIDERS] GOGLOBAL: Raw response received in {search_time:.0f}ms")
                
                # Add hotel name mapping to result for normalize() to use
                result['hotel_name_to_id_map'] = hotel_name_to_id_map
                return {"status": "success", "data": result, "provider": self.provider_name}
            else:
                return {"status": "error", "error": "No data from GoGlobal API"}
                
        except Exception as e:
            logger.error(f"GoGlobal search error: {e}")
            import traceback
            logger.error(f"GoGlobal search traceback: {traceback.format_exc()}")
            return {"status": "error", "error": str(e)}

    def normalize(self, raw: dict, criteria: dict = None) -> List[dict]:
        """
        Normalize GoGlobal response to standard Offer format
        
        Input: Raw API response + original criteria
        Output: List of standardized offer dicts
        """
        offers = []
        
        # Debug logging for troubleshooting
        logger.debug(f"GoGlobal normalize called with raw type: {type(raw)}, value: {raw}")
        
        # Validate input - handle None case explicitly
        if raw is None:
            logger.warning("GoGlobal normalize called with None raw response")
            return offers
            
        if not isinstance(raw, dict):
            logger.warning(f"GoGlobal normalize called with non-dict raw response: {type(raw)}")
            return offers
            
        if raw.get("status") != "success":
            logger.warning(f"GoGlobal normalize called with unsuccessful response: {raw.get('status')}")
            return offers
            
        # Safe data extraction
        data = raw.get("data")
        if not data:
            logger.warning("GoGlobal normalize: no 'data' field in response")
            return offers
            
        if not isinstance(data, dict):
            logger.warning(f"GoGlobal normalize: 'data' is not dict: {type(data)}")
            return offers
            
        hotels = data.get("Hotels", [])
        if not hotels:
            logger.warning("GoGlobal normalize: no 'Hotels' in data")
            return offers
        
        # Get hotel name mapping if available
        hotel_name_to_id_map = data.get('hotel_name_to_id_map', {})
        
        # Process each hotel's offers
        for hotel in hotels:
            # Pass the mapping in criteria-like dict for _process_hotel_offers
            criteria_with_mapping = (criteria or {}).copy()
            criteria_with_mapping['hotel_name_to_id_map'] = hotel_name_to_id_map
            
            hotel_offers = self._process_hotel_offers(hotel, criteria_with_mapping)
            offers.extend(hotel_offers)
            
        logger.info(f"[NORMALIZATION] Normalized {len(offers)} offers from GoGlobal")
        return offers

    # Private helper methods
    
    def _get_hotel_id(self, hotel_name: str) -> Optional[str]:
        """Get GoGlobal hotel ID from mapping service"""
        if not hotel_name:
            return None
            
        from app.services.hotel_mapping import hotel_mapping_service
        return hotel_mapping_service.get_hotel_id(hotel_name, "goglobal")
    
    def _prepare_search_params(self, criteria: dict, hotel_ids: List[str] = None) -> dict:
        """Prepare search parameters from criteria"""
        # Parse dates and calculate nights
        check_in = criteria.get("check_in", "2025-09-12")
        check_out = criteria.get("check_out", "2025-09-15")
        
        check_in_date = datetime.strptime(check_in, "%Y-%m-%d")
        check_out_date = datetime.strptime(check_out, "%Y-%m-%d")
        nights = (check_out_date - check_in_date).days
        
        params = {
            "arrival_date": check_in,
            "nights": nights,
            "adults": criteria.get("adults", 2),
            "children": criteria.get("children", 0),
            "children_ages": criteria.get("children_ages", []),
            "meal_type": criteria.get("meal_type")
        }
        
        # Add hotel IDs if provided
        if hotel_ids:
            params["hotel_ids"] = hotel_ids
        
        return params
    
    async def _make_api_call(self, params: dict) -> Optional[dict]:
        """Make SOAP API call to GoGlobal"""
        try:
            # Build XML request
            xml_request = self._build_xml_request(params)
            
            # Wrap in SOAP envelope
            soap_envelope = self._build_soap_envelope(xml_request)

            # Get aiohttp session
            session = await self.get_session()
            
            # Make HTTP request
            async with session.post(
                self.base_url,
                data=soap_envelope,
                headers={
                    'Content-Type': 'application/soap+xml; charset=utf-8',
                    'API-Operation': 'HOTEL_SEARCH_REQUEST',
                    'API-AgencyID': str(self.credentials['agency_id'])
                }
            ) as response:
                
                if response.status == 200:
                    response_text = await response.text()
                    parsed_response = self._parse_response(response_text)
                    return parsed_response
                else:
                    response_text = await response.text()
                    logger.error(f"GOGLOBAL: HTTP {response.status}: {response_text[:200]}")
                    return None
                
        except Exception as e:
            logger.error(f"API call error: {e}")
            return None
    
    def _build_xml_request(self, params: dict) -> str:
        """Build XML request for GoGlobal API"""
        # Build children ages XML
        children_xml = ""
        if params["children"] > 0 and params["children_ages"]:
            for age in params["children_ages"][:params["children"]]:
                children_xml += f"<ChildAge>{age}</ChildAge>"
        
        # Build meal filter XML (if specified)
        meal_filter_xml = ""
        if params["meal_type"]:
            meal_filter_xml = f"""<FilterRoomBasises>
<FilterRoomBasis>{params['meal_type']}</FilterRoomBasis>
</FilterRoomBasises>"""
        
        # Build hotels XML - support multiple hotel IDs
        hotels_xml = "<Hotels>"
        hotel_ids = params.get('hotel_ids', [params.get('hotel_id')])  # Support both single and multiple
        for hotel_id in hotel_ids:
            if hotel_id:  # Skip None/empty values
                hotels_xml += f"<HotelId>{hotel_id}</HotelId>"
        hotels_xml += "</Hotels>"
        
        return f'''<Root>
<Header>
<Agency>{self.credentials['agency_id']}</Agency>
<User>{self.credentials['username']}</User>
<Password>{self.credentials['password']}</Password>
<Operation>HOTEL_SEARCH_REQUEST</Operation>
<OperationType>Request</OperationType>
</Header>
<Main Version="2.3" ResponseFormat="JSON" Currency="EUR">
{meal_filter_xml}
{hotels_xml}
<ArrivalDate>{params['arrival_date']}</ArrivalDate>
<Nights>{params['nights']}</Nights>
<Rooms>
<Room Adults="{params['adults']}" RoomCount="1" ChildCount="{params['children']}">
{children_xml}
</Room>
</Rooms>
</Main>
</Root>'''
    
    def _build_soap_envelope(self, xml_request: str) -> str:
        """Wrap XML in SOAP envelope"""
        return f'''<?xml version="1.0" encoding="utf-8"?>
<soap12:Envelope xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
<soap12:Body>
<MakeRequest xmlns="http://www.goglobal.travel/">
<requestType>11</requestType>
<xmlRequest><![CDATA[{xml_request}]]></xmlRequest>
</MakeRequest>
</soap12:Body>
</soap12:Envelope>'''
    
    def _parse_response(self, response_text: str) -> Optional[dict]:
        """Parse SOAP response and extract JSON"""
        try:
            # Debug: log raw response
            logger.debug(f"GoGlobal raw response length: {len(response_text)}")
            logger.debug(f"GoGlobal raw response first 500 chars: {response_text[:500]}")
            
            # Parse XML and find JSON data
            root = ET.fromstring(response_text)
            
            # Find MakeRequestResult element
            for elem in root.iter():
                if 'MakeRequestResult' in elem.tag:
                    logger.debug(f"Found MakeRequestResult: {elem.tag}, text: {elem.text}")
                    if elem.text:
                        logger.debug(f"GoGlobal DEBUG: Parsing JSON response length: {len(elem.text)}")
                        logger.debug(f"Parsing JSON from MakeRequestResult: {elem.text[:200]}...")
                        parsed_response = json.loads(elem.text)
                        logger.debug(f"GoGlobal DEBUG: Parsed response has Hotels: {len(parsed_response.get('Hotels', []))}")
                        return parsed_response
                    else:
                        logger.warning("MakeRequestResult found but has no text content")
            
            logger.warning("No MakeRequestResult found in response")
            return {"Hotels": []}
            
        except (ET.ParseError, json.JSONDecodeError) as e:
            logger.error(f"Response parsing error: {e}")
            logger.error(f"Problematic response text: {response_text[:1000]}")
            return None
    
    def _process_hotel_offers(self, hotel: dict, criteria: dict) -> List[dict]:
        """Process single hotel's offers into standardized format"""
        offers = []
        
        hotel_id = str(hotel.get("HotelCode", ""))
        
        # Map hotel ID back to hotel name using the mapping from search_params
        hotel_name = ""
        if criteria and 'hotel_name_to_id_map' in criteria:
            hotel_name_to_id_map = criteria['hotel_name_to_id_map']
            # Find the hotel name by ID
            for hid, hname in hotel_name_to_id_map.items():
                if str(hid) == hotel_id:
                    hotel_name = hname
                    break
        
        if not hotel_name:
            # Fallback to hotel name from response or criteria
            hotel_name = hotel.get("HotelName", criteria.get("hotel_name", "")) if criteria else ""
            
        logger.debug(f"GoGlobal: Processing hotel {hotel_id} -> '{hotel_name}'")
        
        # Get allowed fields once per hotel (not per offer - optimization)
        allowed_fields = Config.get_allowed_fields()
        
        for offer in hotel.get("Offers", []):
            try:
                # Buduj ofertę selektywnie
                standardized_offer = {}
                
                # Extract data ONLY if field is needed
                if 'room_name' in allowed_fields:
                    room_names = offer.get("Rooms", [])
                    room_name = room_names[0] if room_names else offer.get("RoomName", "Sztuczna nazwa pokoju")
                    standardized_offer['room_name'] = room_name
                    
                if 'room_features' in allowed_fields:
                    special = offer.get("Special", "")
                    standardized_offer['room_features'] = [special.strip()] if special else []
                
                # Map other allowed fields
                if 'supplier_hotel_id' in allowed_fields:
                    standardized_offer['supplier_hotel_id'] = hotel_id
                if 'hotel_name' in allowed_fields:
                    standardized_offer['hotel_name'] = hotel_name
                if 'supplier_room_code' in allowed_fields:
                    standardized_offer['supplier_room_code'] = offer.get("HotelSearchCode")
                if 'room_category' in allowed_fields:
                    standardized_offer['room_category'] = None  # Will be set by universal provider
                if 'room_mapping_id' in allowed_fields:
                    standardized_offer['room_mapping_id'] = None  # Will be set by universal provider
                if 'meal_plan' in allowed_fields:
                    standardized_offer['meal_plan'] = offer.get("RoomBasis", "room_only")
                if 'total_price' in allowed_fields:
                    standardized_offer['total_price'] = str(offer.get("TotalPrice", "0"))
                if 'currency' in allowed_fields:
                    standardized_offer['currency'] = offer.get("Currency", "EUR")
                if 'amenities' in allowed_fields:
                    standardized_offer['amenities'] = []  # GoGlobal doesn't provide detailed amenities
                if 'free_cancellation_until' in allowed_fields:
                    # Simple extraction - GoGlobal usually doesn't have complex cancellation data
                    standardized_offer['free_cancellation_until'] = offer.get("CancellationDeadline")
                
                # Add required system fields
                standardized_offer['provider'] = 'goglobal'
                
                offers.append(standardized_offer)
                
            except Exception as e:
                logger.warning(f"Error processing offer: {e}")
                continue
                
        return offers
    
    def prepare_meal_type_criteria(self, criteria: Dict[str, Any]) -> Dict[str, Any]:
        """
        GoGlobal supports request-level filtering via FilterRoomBasis XML element
        
        Args:
            criteria: Standard search criteria dict containing meal_types
            
        Returns:
            Modified criteria dict with GoGlobal-specific meal_type mapping
        """
        # Handle both old single meal_type and new meal_types list for backward compatibility
        meal_types = criteria.get("meal_types") or (
            [criteria["meal_type"]] if criteria.get("meal_type") else []
        )
        
        if not meal_types:
            logger.debug("GoGlobal: No meal_types provided")
            return criteria
            
        # Import meal service for proper mapping
        from app.services.meal_mapping import meal_mapping_service as meal_type_service
        
        # Use first meal type and map to GoGlobal native code
        meal_type = meal_types[0]  # Take first meal type for request filtering
        
        # Check if we should filter at request level
        if meal_type_service.should_filter_at_request_level("goglobal", meal_type):
            native_code = meal_type_service.get_native_meal_code("goglobal", meal_type)
            if native_code:
                logger.debug(f"[PROVIDERS] GoGlobal: Using request-level filtering - {meal_type} -> {native_code}")
                # Set the mapped meal_type for XML request building
                criteria = criteria.copy()  # Don't modify original
                criteria["meal_type"] = native_code
                return criteria
            else:
                logger.warning(f"GoGlobal: No native mapping found for meal_type '{meal_type}'")
        else:
            logger.debug(f"GoGlobal: Request-level filtering not supported for '{meal_type}'")
            
        return criteria