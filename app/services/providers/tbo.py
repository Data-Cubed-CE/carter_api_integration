
"""
TBO API Provider Implementation
Provider for TBO Hotel Search API integration
"""

import json
import logging
from datetime import datetime
from typing import Dict, List, Any, Optional
import aiohttp
import asyncio
import time

from app.services.universal_provider import ProviderAdapter
from app.services.hotel_mapping import hotel_mapping_service
from app.config import Config

logger = logging.getLogger(__name__)

class TBOProvider(ProviderAdapter):
    """TBO API Provider for hotel search"""
    
    def __init__(self, provider_name: str = "tbo"):
        """Initialize TBO provider."""
        super().__init__(provider_name)
        
        # Ensure config is properly initialized
        if not hasattr(self, 'config') or self.config is None:
            self.config = {
                'base_url': 'http://api.tbotechnology.in/TBOHolidays_HotelAPI/search',
                'timeout': 25
            }

    def _get_config_value(self, key: str, default: Any = None) -> Any:
        """Safely get configuration value with fallback"""
        if not hasattr(self, 'config') or self.config is None:
            logger.warning(f"Config not initialized, using default for {key}: {default}")
            return default
        return self.config.get(key, default)

    async def search(self, search_params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute hotel search against TBO API
        
        Args:
            search_params: Standard search parameters (including hotel_names list)
            
        Returns:
            Raw TBO API response
        """
        start_time = time.time()
        
        try:
            # Simple logging
            hotel_names = search_params.get("hotel_names", [])
            
            # Map hotel_names to TBO hotel_codes
            if not hotel_names:
                raise ValueError("hotel_names list is required for TBO search")
            
            hotel_codes = []
            hotel_id_to_name_map = {}  # Mapowanie TBO hotel_code -> original hotel_name
            
            for hotel_name in hotel_names:
                tbo_hotel_code = hotel_mapping_service.get_hotel_id(hotel_name, "tbo")
                if tbo_hotel_code:
                    hotel_codes.append(tbo_hotel_code)
                    hotel_id_to_name_map[tbo_hotel_code] = hotel_name
                else:
                    logger.warning(f"TBO: Hotel '{hotel_name}' not found in mappings - skipping")
            
            if not hotel_codes:
                raise ValueError(f"None of the hotels {hotel_names} found in TBO mappings")
            
            # Add hotel_codes to search_params for _build_tbo_request
            search_params_with_codes = search_params.copy()
            search_params_with_codes['hotel_codes'] = hotel_codes
            
            # Build TBO API request
            tbo_request = self._build_tbo_request(search_params_with_codes)
            
            # Get config values safely
            base_url = self._get_config_value('base_url', 'http://api.tbotechnology.in/TBOHolidays_HotelAPI/search')
            timeout = self._get_config_value('timeout', 25)
            
            # Log request
            logger.debug(f"TBO request data: {tbo_request}")
            
            # Execute API call using shared session with Basic Auth
            session = await self.get_session()
            
            # Prepare Basic Auth
            username = self.config.get('username')
            password = self.config.get('password')
            auth = aiohttp.BasicAuth(username, password) if username and password else None
            
            async with session.post(
                base_url,
                json=tbo_request,
                auth=auth,  # Add Basic Auth
                headers={
                    'Content-Type': 'application/json',
                    'Accept': 'application/json'
                },
                timeout=aiohttp.ClientTimeout(total=timeout)
            ) as response:
                
                response_text = await response.text()
                
                # Handle HTTP errors
                if response.status != 200:
                    error_msg = f"TBO API HTTP Error {response.status}: {response_text}"
                    logger.error(error_msg)
                    return {
                        'success': False,
                        'error': error_msg,
                        'status_code': response.status
                    }
                
                # Parse JSON response
                try:
                    tbo_response = json.loads(response_text)
                except json.JSONDecodeError as e:
                    error_msg = f"TBO API Invalid JSON response: {str(e)}"
                    logger.error(error_msg)
                    return {
                        'success': False,
                        'error': error_msg,
                        'raw_response': response_text
                    }
                
                # Check TBO API status
                api_status = tbo_response.get('Status', {})
                status_code = api_status.get('Code', 0)
                
                logger.info(f"[PROVIDERS] TBO API Status Code: {status_code}")
                
                if status_code == 200:  # SUCCESS
                    # Structured logging - search results summary
                    search_time = (time.time() - start_time) * 1000
                    hotels_found = len(tbo_response.get('HotelResult', []))
                    total_offers = sum(len(hotel.get('Rooms', [])) for hotel in tbo_response.get('HotelResult', []))
                    
                    logger.info(f"[PROVIDERS] TBO: {total_offers} offers in {search_time:.0f}ms")
                    
                    return {
                        'success': True,
                        'data': tbo_response,
                        'provider': 'tbo',
                        'hotel_id_to_name_map': hotel_id_to_name_map  # Dodaj mapowanie
                    }
                elif status_code == 201:  # NO_AVAILABILITY
                    logger.info(f"[PROVIDERS] TBO search completed - No availability for given criteria")
                    return {
                        'success': True,
                        'data': {
                            'Status': api_status,
                            'HotelResult': []
                        },
                        'provider': 'tbo'
                    }
                else:
                    error_msg = f"TBO API Error - Status: {status_code}, Description: {api_status.get('Description', 'Unknown error')}"
                    logger.error(error_msg)
                    return {
                        'success': False,
                        'error': error_msg,
                        'tbo_status': api_status
                    }
                        
        except asyncio.TimeoutError:
            timeout = self._get_config_value('timeout', 25)
            error_msg = f"TBO API timeout after {timeout} seconds"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'timeout': True
            }
            
        except Exception as e:
            error_msg = f"TBO API unexpected error: {str(e)}"
            logger.error(error_msg)
            return {
                'success': False,
                'error': error_msg,
                'exception': str(e)
            }

    def normalize(self, raw_response: Dict[str, Any], criteria: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Normalize TBO API response to standard flat offers format (matching Rate Hawk/GoGlobal)
        
        Args:
            raw_response: Raw TBO API response
            criteria: Original search criteria (optional, for enhanced processing)
            
        Returns:
            List of normalized offers (flat structure like Rate Hawk/GoGlobal)
        """
        try:
            if not raw_response.get('success', False):
                logger.warning("TBO normalize called with unsuccessful response")
                return []
            
            tbo_data = raw_response.get('data', {})
            hotel_results = tbo_data.get('HotelResult', [])
            
            if not hotel_results:
                logger.info(f"[NORMALIZATION] TBO normalize - No hotels in response")
                return []
            
            offers = []
            
            # Process each hotel and flatten rooms into individual offers
            for hotel_data in hotel_results:
                try:
                    # Extract basic hotel info
                    hotel_code = str(hotel_data.get('HotelCode', ''))
                    hotel_name = hotel_data.get('HotelName', '')  # Often empty in TBO
                    
                    if not hotel_code:
                        logger.warning(f"TBO hotel missing HotelCode")
                        continue
                    
                    # Map TBO hotel code back to original hotel name from request
                    hotel_id_to_name_map = raw_response.get('hotel_id_to_name_map', {})
                    original_hotel_name = hotel_id_to_name_map.get(hotel_code)
                    
                    if original_hotel_name:
                        hotel_name = original_hotel_name  # Use original name from request
                        logger.debug(f"TBO: Mapped hotel code {hotel_code} to original name '{original_hotel_name}'")
                    else:
                        # Fallback to ref_hotel_name from mapping service
                        ref_hotel_name = self._get_ref_hotel_name_from_tbo_id(hotel_code)
                        if not ref_hotel_name:
                            logger.debug(f"TBO hotel {hotel_code} not found in mappings")
                            continue
                        
                        # Use ref_hotel_name as hotel_name if TBO doesn't provide it
                        if not hotel_name:
                            hotel_name = ref_hotel_name
                    
                    # Process each room as separate offer (like Rate Hawk does)
                    room_results = hotel_data.get('Rooms', [])  # FIX: TBO uses 'Rooms' not 'RoomResult'
                    for room_data in room_results:
                        try:
                            # Extract room details - TBO format
                            room_names = room_data.get('Name', ['Standard Room'])
                            room_type = room_names[0] if room_names else 'Standard Room'
                            meal_type_raw = room_data.get('MealType', 'Room_Only') 
                            booking_code = room_data.get('BookingCode', '')
                            
                            # Keep original meal type for response-level filtering
                            # Mapping to standard codes will be done later in universal_provider
                            
                            # Extract pricing - TBO format
                            total_price = float(room_data.get('TotalFare', 0))
                            currency = hotel_data.get('Currency', 'USD')  # Currency is at hotel level
                            
                            # Validate
                            allowed_fields = Config.get_allowed_fields()
                            
                            # Rozpocznij z pustą ofertą i dodawaj tylko potrzebne pola
                            offer = {}
                            
                            # Mapuj tylko jeśli pole jest dozwolone
                            if 'supplier_hotel_id' in allowed_fields:
                                offer['supplier_hotel_id'] = hotel_code
                            if 'hotel_name' in allowed_fields:
                                offer['hotel_name'] = hotel_name
                            if 'supplier_room_code' in allowed_fields:
                                offer['supplier_room_code'] = booking_code
                            if 'room_name' in allowed_fields:
                                offer['room_name'] = room_type
                            if 'room_category' in allowed_fields:
                                offer['room_category'] = None  # Will be set by universal provider
                            if 'room_mapping_id' in allowed_fields:
                                offer['room_mapping_id'] = None  # Will be set by universal provider
                            if 'meal_plan' in allowed_fields:
                                offer['meal_plan'] = meal_type_raw
                            if 'total_price' in allowed_fields:
                                offer['total_price'] = total_price
                            if 'currency' in allowed_fields:
                                offer['currency'] = currency
                            if 'room_features' in allowed_fields:
                                offer['room_features'] = []  # TBO doesn't provide detailed room features
                            if 'amenities' in allowed_fields:
                                offer['amenities'] = []  # TBO doesn't provide detailed amenities
                            if 'free_cancellation_until' in allowed_fields:
                                # Extract free cancellation date from CancelPolicies
                                free_cancellation_date = self._extract_free_cancellation_date(room_data)
                                offer['free_cancellation_until'] = free_cancellation_date
                                logger.debug(f"TBO: Added free_cancellation_until={free_cancellation_date} to offer")
                            
                            # Dodaj pola systemowe (zawsze potrzebne)
                            offer['provider'] = "tbo"
                            offer['offer_id'] = booking_code
                            
                            offers.append(offer)
                            
                        except Exception as e:
                            logger.error(f"TBO room normalization error: {str(e)}")
                            continue
                            
                except Exception as e:
                    logger.error(f"TBO hotel normalization error: {str(e)}")
                    continue
            
            logger.info(f"[NORMALIZATION] TBO normalized {len(offers)} offers successfully")
            return offers
            
        except Exception as e:
            error_msg = f"TBO normalization error: {str(e)}"
            logger.error(error_msg)
            return []

    def prepare_meal_type_criteria(self, criteria: Dict[str, Any]) -> Dict[str, Any]:
        """
        TBO uses response-level filtering, so no request preparation needed
        
        Args:
            criteria: Original search criteria containing hotel_codes and other params
            
        Returns:
            Original criteria unchanged (TBO doesn't support request-level meal filtering)
        """
        # TBO filtering happens at response level, just pass criteria through
        meal_types = criteria.get("meal_types") or (
            [criteria.get("meal_type")] if criteria.get("meal_type") else []
        )
        
        if meal_types:
            logger.debug(f"TBO meal filtering - will be applied at response level for: {meal_types}")
            
        return criteria  # Return original criteria with hotel_codes intact

    def _build_tbo_request(self, search_params: Dict[str, Any]) -> Dict[str, Any]:
        """Build TBO API request from standard search parameters"""
        
        # Extract dates - try both possible field names
        check_in = search_params.get('check_in_date') or search_params.get('check_in')
        check_out = search_params.get('check_out_date') or search_params.get('check_out')
        
        if not check_in or not check_out:
            raise ValueError(f"Missing required dates: check_in={check_in}, check_out={check_out}")
        
        # Format dates for TBO (YYYY-MM-DD format)
        if isinstance(check_in, str):
            check_in_dt = datetime.fromisoformat(check_in.replace('Z', '+00:00'))
        else:
            check_in_dt = check_in
            
        if isinstance(check_out, str):
            check_out_dt = datetime.fromisoformat(check_out.replace('Z', '+00:00'))
        else:
            check_out_dt = check_out
            
        if check_in_dt is None or check_out_dt is None:
            raise ValueError(f"Failed to parse dates: check_in_dt={check_in_dt}, check_out_dt={check_out_dt}")
            
        formatted_check_in = check_in_dt.strftime("%Y-%m-%d")
        formatted_check_out = check_out_dt.strftime("%Y-%m-%d")
        
        # Build PaxRooms array (TBO format)
        pax_rooms = []
        for room in search_params.get('rooms', [{'adults': 2, 'children': 0}]):
            room_data = {
                "Adults": room.get('adults', 2),
                "Children": room.get('children', 0)
            }
            
            # Add children ages if provided
            if room.get('children', 0) > 0 and room.get('children_ages'):
                room_data["ChildrenAges"] = room['children_ages']
            else:
                room_data["ChildrenAges"] = []
            
            pax_rooms.append(room_data)
        
        # Build TBO request with correct format
        hotel_codes = search_params.get('hotel_codes', [])
        # Convert array to comma-separated string if needed
        if isinstance(hotel_codes, list):
            hotel_codes_str = ','.join(str(code) for code in hotel_codes)
        else:
            hotel_codes_str = str(hotel_codes)
        
        tbo_request = {
            "CheckIn": formatted_check_in,
            "CheckOut": formatted_check_out,
            "HotelCodes": hotel_codes_str,  # String, not array
            "GuestNationality": search_params.get('guest_nationality', 'PL'),
            "PaxRooms": pax_rooms,
            "ResponseTime": 30,
            "IsDetailedResponse": True,
            "Filters": {
                "Refundable": False,
                "NoOfRooms": 0,  # Use 0 like in working example
                "MealType": "All"  # Will be filtered at response level
            }
        }
        
        logger.debug(f"TBO request built: {tbo_request}")
        return tbo_request

    def _get_ref_hotel_name_from_tbo_id(self, tbo_hotel_id: str) -> Optional[str]:
        """
        Reverse lookup: find ref_hotel_name from TBO hotel_id using Azure SQL Database
        
        Args:
            tbo_hotel_id: TBO provider hotel ID (as string)
            
        Returns:
            Reference hotel name if found, None otherwise
        """
        try:
            # Validate input
            if not tbo_hotel_id:
                logger.warning("Empty TBO hotel ID provided")
                return None
                
            # Normalize TBO hotel ID - keep as string
            normalized_id = str(tbo_hotel_id).strip()
            
            # Use hotel_mapping_service reverse lookup
            ref_hotel_name = hotel_mapping_service.get_ref_hotel_name_by_provider_id(normalized_id, "tbo")
            
            if ref_hotel_name:
                logger.debug(f"TBO hotel ID {tbo_hotel_id} mapped to: {ref_hotel_name}")
                return ref_hotel_name
            else:
                logger.debug(f"TBO hotel ID {tbo_hotel_id} not found in mappings")
                return None
                
        except Exception as e:
            logger.error(f"Error in TBO hotel ID lookup: {e}")
            return None

    def __str__(self) -> str:
        """String representation of TBO provider"""
        base_url = self._get_config_value('base_url', 'not_configured')
        timeout = self._get_config_value('timeout', 25)
        return f"TBOProvider(base_url={base_url}, timeout={timeout})"

    def _extract_free_cancellation_date(self, room_data: Dict[str, Any]) -> Optional[str]:
        """
        Extract free cancellation date from TBO CancelPolicies
        
        Logic: Find the policy where CancellationCharge = 0.0 and return its FromDate
        
        Args:
            room_data: Room data from TBO API containing CancelPolicies
            
        Returns:
            ISO datetime string of free cancellation deadline or None
        """
        try:
            cancel_policies = room_data.get('CancelPolicies', [])
            
            if not cancel_policies:
                logger.debug("TBO: No CancelPolicies found")
                return None
            
            # Find policy with CancellationCharge = 0.0 (free cancellation)
            free_cancellation_policy = None
            for policy in cancel_policies:
                charge = policy.get('CancellationCharge', 100.0)
                
                # Look for free cancellation - only charge = 0.0 matters
                if charge == 0.0:
                    free_cancellation_policy = policy
                    break
            
            if not free_cancellation_policy:
                logger.debug("TBO: No free cancellation policy found")
                return None
            
            # Extract and convert date format
            from_date_str = free_cancellation_policy.get('FromDate')
            if not from_date_str:
                logger.debug("TBO: No FromDate in free cancellation policy")
                return None
            
            # Convert TBO date format "17-08-2025 00:00:00" to ISO format
            
            # Parse TBO date format
            tbo_date = datetime.strptime(from_date_str, "%d-%m-%Y %H:%M:%S")
            
            # Convert to ISO format for standardization
            iso_date = tbo_date.isoformat() + "Z"
            
            logger.debug(f"TBO: Converted free cancellation date {from_date_str} -> {iso_date}")
            return iso_date
            
        except Exception as e:
            logger.error(f"TBO: Error extracting free cancellation date: {e}")
            return None
