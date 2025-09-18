# app/services/providers/rate_hawk.py

import aiohttp
import asyncio
import logging
import time
import json
from typing import Dict, Any
from app.services.universal_provider import ProviderAdapter
from app.services.hotel_mapping import hotel_mapping_service
from app.utils.logger import hotel_logger
from app.config import Config

logger = logging.getLogger(__name__)

class RateHawkProvider(ProviderAdapter):
    """
    Rate Hawk API provider adapter.
    
    Implements hotel search and rate normalization for the Rate Hawk API (worldota.net).
    Uses BasicAuth authentication and supports ETG API V3 specifications.
    """

    def __init__(self, provider_name: str = "rate_hawk"):
        """Initialize Rate Hawk provider."""
        super().__init__(provider_name)
        # Get base URL from configuration
        self.base_url = self.config.get('base_url', "https://api.worldota.net/api/b2b/v3/search/serp/hotels/")
    
    def prepare_meal_type_criteria(self, criteria: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare meal type criteria for Rate Hawk API request.
        
        Rate Hawk uses response-level filtering for meal types rather than 
        request-level filtering. This method stores meal type preferences
        for later use during response normalization.
        
        Args:
            criteria: Search criteria dictionary
            
        Returns:
            Modified criteria with Rate Hawk specific meal type handling
        """
        # Support both legacy and new meal_types formats
        meal_types = criteria.get("meal_types") or (
            [criteria.get("meal_type")] if criteria.get("meal_type") else []
        )
        
        if meal_types:
            logger.info(f"[PROVIDERS] RateHawk: Will use response-level filtering for meal_types {meal_types}")
            # Store for response-level filtering
            criteria['rate_hawk_meal_types'] = meal_types
            
        return criteria

    async def search(self, criteria: dict) -> dict:
        """
        Search for hotel offers using the Rate Hawk API.
        
        Implements ETG API V3 specifications with comprehensive validation
        and error handling.
        
        Args:
            criteria: Search criteria containing:
                - hotel_names: List of hotel names to search
                - check_in: Check-in date (YYYY-MM-DD)
                - check_out: Check-out date (YYYY-MM-DD)
                - adults: Number of adults (1-6)
                - children: Number of children (0-4)
                - children_ages: List of children ages (0-17 each)
                - rooms: Optional number of rooms (1-9)
                - currency: Currency code (ISO 4217)
                - residency: Country code for residency (ISO 3166-1 alpha-2)
                
        Returns:
            Raw API response with hotel_id_to_name_map added
            
        Raises:
            ValueError: If validation fails or required fields are missing
            aiohttp.ClientResponseError: If API request fails
            asyncio.TimeoutError: If request times out
        """
        from datetime import datetime, timedelta
        start_time = time.time()
        
    # Log hotel names for debugging
        hotel_names = criteria.get("hotel_names", [])
        
        # Validate dates according to ETG API V3 limits
        check_in_str = criteria.get("check_in")
        check_out_str = criteria.get("check_out")
        
        if not check_in_str or not check_out_str:
            raise ValueError("check_in and check_out dates are required")
        
        try:
            check_in_date = datetime.strptime(check_in_str, "%Y-%m-%d")
            check_out_date = datetime.strptime(check_out_str, "%Y-%m-%d")
            today = datetime.now()
            
            # ETG API V3 validation rules
            if check_in_date < today.replace(hour=0, minute=0, second=0, microsecond=0):
                raise ValueError("check_in date must be current or future date")
            
            if check_in_date > today + timedelta(days=730):
                raise ValueError("check_in date must be not later than 730 days from today")
            
            if check_out_date <= check_in_date:
                raise ValueError("check_out date must be after check_in date")
            
            if check_out_date > check_in_date + timedelta(days=30):
                raise ValueError("check_out date must be not later than 30 days from check_in")
                
        except ValueError as e:
            if "time data" in str(e):
                raise ValueError("Date format must be YYYY-MM-DD (ISO8601)")
            raise
        
    # Map hotel names to Rate Hawk hotel IDs
        hotel_names = criteria.get("hotel_names", [])
        if not hotel_names:
            raise ValueError("hotel_names list is required in criteria")
            
        rate_hawk_hotel_ids = []
        hotel_id_to_name_map = {}  # Map Rate Hawk ID to original hotel name
        
        for hotel_name in hotel_names:
            rate_hawk_hotel_id = hotel_mapping_service.get_hotel_id(hotel_name, "rate_hawk")
            if rate_hawk_hotel_id:
                rate_hawk_hotel_ids.append(rate_hawk_hotel_id)
                hotel_id_to_name_map[rate_hawk_hotel_id] = hotel_name
                # hotel_mapping_service already logs missing hotels
            else:
                logger.warning(f"Hotel '{hotel_name}' not found in Rate Hawk mappings - skipping")
        
        if not rate_hawk_hotel_ids:
            raise ValueError(f"None of the hotels {hotel_names} found in Rate Hawk mappings")
        
        # Structured logs already show hotel details
        
        # Prepare guest data for Rate Hawk API
        adults = criteria.get("adults", 2)
        children_count = criteria.get("children", 0)
        
        # Validate children ages (max 4 children, age 0-17)
        children_ages = []
        if children_count > 0:
            # Rate Hawk: max 4 children per room, age 0-17
            if children_count > 4:
                error_msg = f"Too many children ({children_count}), Rate Hawk supports max 4 per room"
                logger.error(error_msg)
                raise ValueError(error_msg)
            # Validate children ages
            provided_ages = criteria.get("children_ages", [])
            if provided_ages:
                children_ages = [min(max(age, 0), 17) for age in provided_ages[:children_count]]
            else:
                # Default to age 10 if not provided
                children_ages = [10] * children_count
                
        # Validate total guests per room (max 6)
        total_guests = adults + len(children_ages)
        if total_guests > 6:
            error_msg = f"Too many guests ({total_guests}), Rate Hawk supports max 6 per room"
            logger.error(error_msg)
            raise ValueError(error_msg)
        
        # Build payload according to ETG API V3 documentation
        residency = criteria.get("nationality", criteria.get("residency", "PL"))
        payload = {
            "checkin":    criteria.get("check_in"),   # Format: YYYY-MM-DD
            "checkout":   criteria.get("check_out"),  # Format: YYYY-MM-DD
            "residency":  residency,
            "guests":     [{"adults": adults, "children": children_ages}],
            "ids":        rate_hawk_hotel_ids,
            "currency":   criteria.get("currency", "EUR")
        }
        
        # Optional fields according to HotelSearchRequest model
        if criteria.get("rooms") is not None:
            payload["rooms"] = criteria.get("rooms")

        try:
            # Use shared session from UniversalProvider
            session = await self.get_session()
            
            async with session.post(
                self.base_url, 
                json=payload,
                timeout=aiohttp.ClientTimeout(total=self.config.get('timeout', 30))
            ) as resp:
                logger.info(f"[PROVIDERS] RATE_HAWK API Status Code: {resp.status}")
                response_text = await resp.text()
                logger.debug(f"Rate Hawk Response body: {response_text[:1000]}...")  # Log first 1000 chars
                # Log parsed JSON response
                try:
                    response_json = await resp.json()
                    logger.debug(f"Rate Hawk Response JSON: {json.dumps(response_json, indent=2)}")
                except Exception as e:
                    logger.warning(f"Could not parse Rate Hawk response as JSON: {e}")
                
                if resp.status != 200:
                    error_data = None
                    try:
                        error_data = await resp.json()
                    except Exception as parse_error:
                        logger.warning(f"Could not parse error response as JSON: {parse_error}")
                        pass
                    
                    # ETG API V3 error handling
                    if resp.status == 400:
                        error_msg = "Invalid parameters"
                        if error_data and error_data.get("debug", {}).get("validation_error"):
                            error_msg += f": {error_data['debug']['validation_error']}"
                        logger.error(f"Rate Hawk validation error: {error_msg}")
                    elif resp.status == 402:
                        logger.error("Rate Hawk: Overdue debt - contact account manager")
                    elif resp.status == 403:
                        logger.error("Rate Hawk: Authorization error - check API credentials")
                    elif resp.status == 429:
                        logger.error("Rate Hawk: Rate limit exceeded")
                    else:
                        logger.error(f"Rate Hawk HTTP Error: {resp.status} - {response_text}")
                    
                    resp.raise_for_status()
                
                data = await resp.json()
                
                # Check for ETG API specific error responses
                if data.get("error"):
                    error_type = data.get("error")
                    if error_type == "core_search_error":
                        logger.warning("Rate Hawk: Internal search error - limiting retries")
                        raise Exception("Rate Hawk internal search error")
                    elif error_type == "invalid_params":
                        validation_error = data.get("debug", {}).get("validation_error", "Unknown validation error")
                        raise ValueError(f"Rate Hawk validation error: {validation_error}")
                    else:
                        raise Exception(f"Rate Hawk API error: {error_type}")
                
                # Log results summary
                search_time = (time.time() - start_time) * 1000
                total_offers = sum(len(hotel.get('rates', [])) for hotel in data.get('data', {}).get('hotels', []))
                
                logger.info(f"[PROVIDERS] RATE_HAWK: {total_offers} offers in {search_time:.0f}ms")
                
                # Add hotel_id_to_name_map for normalization
                data['hotel_id_to_name_map'] = hotel_id_to_name_map
                return data
                
        except aiohttp.ClientResponseError as e:
            logger.error(f"Rate Hawk HTTP Error: {e.status} - {e.message}")
            if hasattr(e, 'response') and e.response:
                error_text = await e.response.text()
                logger.error(f"Error response: {error_text}")
            raise
        except asyncio.TimeoutError:
            logger.error("Rate Hawk request timeout")
            raise
        except Exception as e:
            logger.error(f"Rate Hawk request error: {type(e).__name__}: {e}")
            raise

    def normalize(self, raw: dict, criteria: dict = None) -> list:
        """
        Normalize Rate Hawk API response to standard offer format.
        
        Converts Rate Hawk specific response structure to standardized offers
        with comprehensive validation and error handling.
        
        Args:
            raw: Raw API response from Rate Hawk
            criteria: Optional search criteria for filtering
            
        Returns:
            List of normalized hotel offers
        """
        start_time = time.time()
        offers = []
        skipped_rates = 0

        # Raw response analysis disabled for cleaner logs
        
        try:
            # Check if response contains data
            if not raw or not raw.get("data"):
                logger.warning("Rate Hawk response contains no data")
                hotel_logger.data_loss_logger.warning("Rate Hawk: No data in response")
                return offers
            
            data = raw.get("data", {})
            
            # Handle only new multi-hotel structure with hotels array
            hotels = data.get("hotels", [])
            if not isinstance(hotels, list):
                logger.error(f"Invalid response structure - hotels must be a list, got: {type(hotels)}")
                logger.error(f"Available data keys: {list(data.keys())}")
                return offers
            
            if not hotels:
                logger.warning("No hotels found in Rate Hawk response")
                return offers
            
            # Get allowed fields from Offer model
            from app.models.response import Offer
            allowed_fields = set(Offer.__fields__.keys())
            
            logger.debug(f"[PROVIDERS] Found {len(hotels)} hotels in Rate Hawk response")
            
            # Parse hotels and their rates
            for hotel in hotels:
                # Rate Hawk API structure: 'id' (internal), 'hid' (identifier)
                hotel_id = hotel.get("id")  # Rate Hawk internal ID
                hotel_hid = hotel.get("hid", hotel_id)  # Hotel identifier (fallback to id)
                
                # Map hotel ID to original hotel name from request
                hotel_id_to_name_map = raw.get('hotel_id_to_name_map', {})
                hotel_name = None
                
                # Try to find original hotel name using both id and hid
                for mapped_id, original_name in hotel_id_to_name_map.items():
                    if str(mapped_id) == str(hotel_id) or str(mapped_id) == str(hotel_hid):
                        hotel_name = original_name
                        break
                
                # Fallback to ID if mapping not found
                if not hotel_name:
                    hotel_name = str(hotel_hid) if hotel_hid else str(hotel_id)
                    logger.warning(f"Rate Hawk: No hotel name mapping found for ID {hotel_id}/{hotel_hid}, using ID as name")
                
                if not hotel_id:
                    logger.warning("Hotel missing id, skipping")
                    hotel_logger.log_skipped_item("rate_hawk", 0, "Hotel missing id")
                    continue
                
                rates = hotel.get("rates", [])
                logger.debug(f"[PROVIDERS] Hotel {hotel_name} (id: {hotel_id}, hid: {hotel_hid}) has {len(rates)} rates")
                
                for rate_index, rate in enumerate(rates):
                    # Log every rate for transparency
                    hotel_logger.debug_logger.debug(f"Processing rate {rate_index + 1}/{len(rates)}: keys={list(rate.keys())}")
                    
                    try:
                        # Check for completely empty rates
                        if not rate or len(rate) == 0:
                            hotel_logger.log_skipped_item("rate_hawk", rate_index, 
                                                        "Completely empty rate", 
                                                        rate, f"rate_{rate_index}")
                            skipped_rates += 1
                            continue
                            
                        # Check for rates with only legal_info field
                        if len(rate.keys()) <= 1 and "legal_info" in rate:
                            hotel_logger.log_skipped_item("rate_hawk", rate_index, 
                                                        "Rate contains only legal_info field", 
                                                        rate, f"rate_{rate_index}")
                            skipped_rates += 1
                            continue
                        
                        # Check for rates with only metadata fields
                        if len(rate.keys()) <= 2 and all(key in ["legal_info", "id", "rate_id"] for key in rate.keys()):
                            hotel_logger.log_skipped_item("rate_hawk", rate_index, 
                                                        "Rate contains only metadata fields", 
                                                        rate, f"rate_{rate_index}")
                            skipped_rates += 1
                            continue
                        
                        # Check for rates missing all essential fields
                        essential_fields = ['match_hash', 'room_name', 'payment_options', 'daily_prices']
                        present_essential = [field for field in essential_fields if rate.get(field)]
                        
                        if len(present_essential) == 0:
                            hotel_logger.log_skipped_item("rate_hawk", rate_index, 
                                                        f"Rate missing all essential fields {essential_fields}", 
                                                        rate, f"rate_{rate_index}")
                            skipped_rates += 1
                            continue
                        
                        # Validate essential fields
                        match_hash = rate.get("match_hash")
                        if not match_hash:
                            hotel_logger.log_skipped_item("rate_hawk", rate_index, 
                                                        "Missing match_hash", 
                                                        rate, f"rate_{rate_index}")
                            skipped_rates += 1
                            continue
                        
                        room_name = rate.get("room_name")
                        if not room_name:
                            hotel_logger.log_skipped_item("rate_hawk", rate_index, 
                                                        "Missing room_name", 
                                                        rate, match_hash)
                            skipped_rates += 1
                            continue
                        
                        payment_options = rate.get("payment_options", {})
                        payment_types = payment_options.get("payment_types", [])
                        
                        # Validate payment_options
                        if not payment_options or not isinstance(payment_options, dict):
                            hotel_logger.log_skipped_item("rate_hawk", rate_index, 
                                                        "Missing or invalid payment_options", 
                                                        rate, match_hash)
                            skipped_rates += 1
                            continue
                            
                        # Validate payment_types array
                        if not payment_types or len(payment_types) == 0:
                            hotel_logger.log_skipped_item("rate_hawk", rate_index, 
                                                        "Empty payment_types array", 
                                                        rate, match_hash)
                            skipped_rates += 1
                            continue
                        
                        daily_prices = rate.get("daily_prices", [])
                        if not daily_prices or len(daily_prices) == 0:
                            hotel_logger.log_skipped_item("rate_hawk", rate_index, 
                                                        "Missing or empty daily_prices", 
                                                        rate, match_hash)
                            skipped_rates += 1
                            continue
                        
                        # Extract price and currency from payment_types
                        payment_options = rate.get("payment_options", {})
                        payment_types = payment_options.get("payment_types", [])
                        
                        total_amount = 0
                        currency = "EUR"
                        if payment_types:
                            first_payment = payment_types[0]
                            total_amount = float(first_payment.get("amount", 0))
                            currency = first_payment.get("currency_code", "EUR")
                        
                        # Extract room name
                        room_name = rate.get("room_name", "")
                        
                        # Extract free cancellation date
                        free_cancellation_until = None
                        if payment_types:
                            first_payment = payment_types[0]
                            cancellation_penalties = first_payment.get("cancellation_penalties")
                            if cancellation_penalties:
                                free_cancellation_until = cancellation_penalties.get("free_cancellation_before")
                        
                        # Extract room features
                        room_features = []
                        
                        # Extract basic room features from serp_filters
                        serp_filters = rate.get("serp_filters", [])
                        for feature in serp_filters:
                            if feature == "has_bathroom":
                                room_features.append("bathroom")
                            elif feature == "has_internet":
                                room_features.append("internet")
                            elif feature == "has_wifi" or feature == "wifi":
                                room_features.append("wifi")
                            else:
                                clean_feature = feature.replace("has_", "").replace("_", " ")
                                room_features.append(clean_feature)
                        
                        # Extract detailed room characteristics from rg_ext
                        rg_ext = rate.get("rg_ext", {})
                        if rg_ext:
                            # Bathroom types
                            bathroom = rg_ext.get("bathroom", 0)
                            if bathroom == 2:
                                room_features.append("private bathroom")
                            elif bathroom == 1:
                                room_features.append("shared bathroom")
                            
                            # View information
                            view_code = rg_ext.get("view", 0)
                            if view_code > 0:
                                room_features.append("room with view")
                            
                            # Balcony
                            if rg_ext.get("balcony", 0) > 0:
                                room_features.append("balcony")
                            
                            # Club access
                            if rg_ext.get("club", 0) > 0:
                                room_features.append("club access")
                            
                            # Family friendly
                            if rg_ext.get("family", 0) > 0:
                                room_features.append("family friendly")
                        
                        # Extract specific amenities from amenities_data
                        amenities_data = rate.get("amenities_data", [])
                        for amenity in amenities_data:
                            # Clean amenity names
                            clean_amenity = amenity.replace("-", " ").replace("_", " ")
                            if clean_amenity not in room_features:
                                room_features.append(clean_amenity)
                        
                        # Validate room_features and remove duplicates
                        if not isinstance(room_features, list):
                            room_features = []
                        else:
                            # Remove duplicates while preserving order
                            room_features = list(dict.fromkeys(room_features))
                        
                        # Get meal plan
                        offer_meal_plan = rate.get("meal")
                        
                        # Build offer dict with allowed fields only
                        offer = {}
                        
                        # Map only allowed fields
                        if 'supplier_hotel_id' in allowed_fields:
                            offer['supplier_hotel_id'] = hotel_id
                        if 'hotel_id' in allowed_fields:
                            # Include both id and hid for mapping purposes
                            offer['hotel_id'] = str(hotel_hid) if hotel_hid else str(hotel_id)
                        if 'hotel_name' in allowed_fields:
                            # Use the actual hotel identifier from response (hid or id)
                            # This will be mapped to friendly name later by universal_provider
                            offer['hotel_name'] = hotel_name
                        if 'supplier_room_code' in allowed_fields:
                            offer['supplier_room_code'] = rate.get('match_hash', '')
                        if 'room_name' in allowed_fields:
                            offer['room_name'] = room_name
                        if 'room_category' in allowed_fields:
                            offer['room_category'] = None  # Will be set by universal provider
                        if 'room_mapping_id' in allowed_fields:
                            offer['room_mapping_id'] = None  # Will be set by universal provider
                        if 'meal_plan' in allowed_fields:
                            offer['meal_plan'] = offer_meal_plan
                        if 'total_price' in allowed_fields:
                            # Convert to Decimal as required by Offer model
                            from decimal import Decimal
                            offer['total_price'] = Decimal(str(total_amount))
                        if 'currency' in allowed_fields:
                            offer['currency'] = currency
                        if 'room_features' in allowed_fields and room_features:
                            offer['room_features'] = room_features
                        if 'amenities' in allowed_fields:
                            offer['amenities'] = rate.get("amenities_data", []) if isinstance(rate.get("amenities_data"), list) else []
                        if 'free_cancellation_until' in allowed_fields:
                            offer['free_cancellation_until'] = free_cancellation_until
                        
                        # Add required system fields
                        offer['provider'] = self.provider_name  # System needs this
                        
                        logger.debug(f"RateHawk: Built offer with only {len(offer)} required fields (skipped {len(allowed_fields) - len(offer)} unnecessary mappings)")
                        
                        # Try to append the offer
                        try:
                            offers.append(offer)
                            logger.debug(f"RateHawk: Added optimized offer - total: {len(offers)}")
                            hotel_logger.log_offer_creation_attempt("rate_hawk", offer, True)
                        except Exception as creation_error:
                            logger.warning(f"RateHawk: Failed to add offer - error: {creation_error}")
                            hotel_logger.log_offer_creation_attempt("rate_hawk", offer, False, str(creation_error))
                            skipped_rates += 1
                        
                    except Exception as e:
                        hotel_logger.log_validation_error("rate_hawk", rate_index, [str(e)], 
                                                        rate, rate.get('match_hash'))
                        skipped_rates += 1
                        continue
        
        except Exception as e:
            logger.error(f"Error normalizing Rate Hawk response: {e}")
            hotel_logger.general_logger.error(f"Rate Hawk normalization failed: {e}")
        
        # Log normalization summary
        processing_time = (time.time() - start_time) * 1000
        logger.info(f"[NORMALIZATION] Successfully normalized {len(offers)} offers from Rate Hawk in {processing_time:.0f}ms")
        return offers