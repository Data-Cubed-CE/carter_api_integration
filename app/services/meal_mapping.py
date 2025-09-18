# app/services/meal_mapping.py

import logging
import asyncio
from typing import Dict, List, Optional, Any
from enum import Enum
from app.config import config
from app.services.azure_sql_connector import create_azure_sql_connector_from_env

logger = logging.getLogger(__name__)

class FilteringStrategy(str, Enum):
    """Provider filtering capabilities"""
    REQUEST_LEVEL = "request_level"     # Provider supports native filtering
    RESPONSE_LEVEL = "response_level"   # Requires post-processing filtering
    NOT_SUPPORTED = "not_supported"     # Provider doesn't support meal type filtering

class MealMapping:
    """
    Azure SQL Database-based meal mapping service for provider-specific meal type conversions.
    
    Loads meal mappings from Azure SQL Database and provides methods to:
    - Get provider-specific meal codes
    - Convert between standard and provider codes
    - Validate meal type availability
    """
    
    def __init__(self):
        """
        Initialize meal mapping service with Azure SQL Database.
        """        
        self._mappings = {}
        self.provider_capabilities = {}
        self._load_mappings()
        self._initialize_provider_capabilities()
    
    def _load_mappings(self) -> None:
        """Load meal mappings from Azure SQL Database"""
        try:
            # Check if Azure SQL is configured
            sql_config = config.validate_azure_sql_config()
            if not sql_config['is_configured']:
                logger.error("Azure SQL not configured, cannot load meal mappings")
                self._mappings = {}
                return
            
            # Use thread pool to run async code
            import concurrent.futures
            import threading
            
            def run_async_in_thread():
                """Run async method in a separate thread to avoid event loop conflicts"""
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(self._async_load_mappings())
                finally:
                    loop.close()
            
            # Run in thread pool to avoid "event loop already running" error
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_async_in_thread)
                success = future.result()
            
            if not success:
                logger.error("Failed to load meal mappings from database")
                self._mappings = {}
                
        except Exception as e:
            logger.error(f"Failed to load meal mappings: {e}")
            self._mappings = {}
    
    
    async def _async_load_mappings(self) -> bool:
        """Async method to load data from Azure SQL Database"""
        try:
            connector = create_azure_sql_connector_from_env()
            
            # Pobierz całą tabelę meal_mappings
            query = "SELECT * FROM [dbo].[meal_mappings]"
            
            results = await connector.execute_query(query)
            
            if not results:
                logger.warning("No meal mappings found in database")
                return False
            
            # Convert to pandas DataFrame for easier processing
            import pandas as pd
            df = pd.DataFrame(results)
            
            # Get provider names from config and their sql_column mappings
            provider_names = config.get_all_provider_names()
            column_mapping = {}
            
            for provider_name in provider_names:
                meal_config = config.get_meal_filtering_config(provider_name)
                if meal_config and meal_config.get("sql_column"):
                    sql_column = meal_config["sql_column"]
                    # Check if this column exists in DataFrame
                    if sql_column in df.columns:
                        column_mapping[provider_name] = sql_column
                    else:
                        logger.warning(f"Column '{sql_column}' for provider '{provider_name}' not found in database")
            
            # Convert DataFrame to mappings dictionary
            self._mappings = {}
            for _, row in df.iterrows():
                code = row['Kod']
                mapping = {}
                
                # Add provider-specific mappings using sql_column from config
                for provider_name, db_column in column_mapping.items():
                    value = row.get(db_column)
                    if pd.notna(value) and str(value).strip():
                        mapping[provider_name] = str(value).strip()
                
                self._mappings[code] = mapping
            
            logger.info(f"Loaded {len(self._mappings)} meal mappings from Azure SQL Database")
            logger.debug(f"Available meal codes: {list(self._mappings.keys())}")
            logger.debug(f"Provider columns used: {column_mapping}")
            return True
            
        except Exception as e:
            logger.error(f"Database query failed: {e}")
            return False

    def _initialize_provider_capabilities(self) -> None:
        """Initialize provider capabilities dynamically from Azure SQL data and Config"""
        
        capabilities = {}
        
        # Initialize each provider using Config
        for provider_name in config.get_all_provider_names():
            # Get meal filtering config from main config
            meal_config = config.get_meal_filtering_config(provider_name)
            
            if not meal_config:
                # Skip providers without meal filtering configuration
                logger.debug(f"Provider {provider_name} has no meal filtering configuration, skipping")
                continue
                
            # Map strategy string to enum
            strategy_str = meal_config.get("strategy", "not_supported")
            if strategy_str == "request_level":
                strategy = FilteringStrategy.REQUEST_LEVEL
            elif strategy_str == "response_level":
                strategy = FilteringStrategy.RESPONSE_LEVEL
            else:
                strategy = FilteringStrategy.NOT_SUPPORTED
            
            capabilities[provider_name] = {
                "strategy": strategy,
                "supported_meal_types": [],
                "native_mapping": {} if strategy == FilteringStrategy.REQUEST_LEVEL else None,
                "response_field_mapping": {} if strategy == FilteringStrategy.RESPONSE_LEVEL else None,
                "reverse_mapping": {} if strategy == FilteringStrategy.RESPONSE_LEVEL else None
            }
        
        # Build capabilities from SQL mappings - use EXACT values from database
        for meal_code, mapping in self._mappings.items():
            for provider_name in capabilities.keys():
                provider_value = mapping.get(provider_name)
                if not provider_value:
                    continue
                    
                capabilities[provider_name]["supported_meal_types"].append(meal_code)
                
                if capabilities[provider_name]["strategy"] == FilteringStrategy.REQUEST_LEVEL:
                    # GoGlobal: Direct mapping for requests
                    capabilities[provider_name]["native_mapping"][meal_code] = provider_value
                else:
                    # RateHawk/TBO: Store exact SQL values for response filtering
                    capabilities[provider_name]["response_field_mapping"][provider_value] = meal_code
                    
                    # Store reverse mapping with ONLY the exact SQL value (case-insensitive matching)
                    if meal_code not in capabilities[provider_name]["reverse_mapping"]:
                        capabilities[provider_name]["reverse_mapping"][meal_code] = []
                    
                    capabilities[provider_name]["reverse_mapping"][meal_code].append(provider_value)
        
        self.provider_capabilities = capabilities
    
    def get_provider_value(self, meal_code: str, provider: str) -> Optional[str]:
        """
        Get provider-specific meal code for standard meal code.
        
        Args:
            meal_code: Standard meal code (e.g., 'BB', 'HB')
            provider: Provider name (e.g., 'goglobal', 'rate_hawk')
            
        Returns:
            Provider-specific meal code or None if not found
        """
        mapping = self._mappings.get(meal_code, {})
        return mapping.get(provider)
    
    def get_standard_code(self, provider_value: str, provider: str) -> Optional[str]:
        """
        Get standard meal code from provider-specific value.
        
        Args:
            provider_value: Provider-specific meal code
            provider: Provider name
            
        Returns:
            Standard meal code or None if not found
        """
        for code, mapping in self._mappings.items():
            if mapping.get(provider) == provider_value:
                return code
        return None
    
    def get_all_mappings(self) -> Dict[str, Dict[str, Any]]:
        """Get all meal mappings"""
        return self._mappings.copy()
    
    def get_supported_meal_types(self, provider: str = 'all') -> List[str]:
        """
        Get list of supported meal types.
        
        Args:
            provider: Provider name ('all' for all supported meal types)
            
        Returns:
            List of supported meal type codes
        """
        if provider == 'all':
            # Return all meal codes from mappings
            return list(self._mappings.keys())
        else:
            # Return meal types supported by specific provider
            supported_types = []
            for meal_code, mapping in self._mappings.items():
                if provider in mapping and mapping[provider]:
                    supported_types.append(meal_code)
            return supported_types
    
    def normalize_offers_meal_plans(self, offers: List[Dict[str, Any]], provider: str) -> List[Dict[str, Any]]:
        """
        Normalize meal_plan values in offers from provider-specific to standard codes.
        
        Args:
            offers: List of offer dictionaries containing meal_plan field
            provider: Provider name (e.g., 'tbo', 'rate_hawk')
            
        Returns:
            List of offers with normalized meal_plan values
        """
        if not offers:
            return offers
        
        normalized_offers = []
        for offer in offers:
            # Create a copy of the offer to avoid modifying original
            normalized_offer = offer.copy()
            
            # Get the current meal_plan value
            current_meal_plan = offer.get('meal_plan')
            if current_meal_plan:
                # Convert provider-specific value to standard code
                standard_code = self.get_standard_code(current_meal_plan, provider)
                if standard_code:
                    normalized_offer['meal_plan'] = standard_code
                    logger.debug(f"Normalized meal_plan: {current_meal_plan} -> {standard_code}")
                else:
                    logger.warning(f"No mapping found for {provider} meal_plan: {current_meal_plan}")
            
            normalized_offers.append(normalized_offer)
        
        return normalized_offers
    
    # ============ Advanced Filtering Methods ============
    
    def get_provider_strategy(self, provider_name: str) -> FilteringStrategy:
        """Get filtering strategy for specific provider"""
        config = self.provider_capabilities.get(provider_name, {})
        return config.get("strategy", FilteringStrategy.NOT_SUPPORTED)
    
    def is_meal_type_supported(self, provider_name: str, meal_type: str) -> bool:
        """Check if provider supports specific meal type"""
        config = self.provider_capabilities.get(provider_name, {})
        supported = config.get("supported_meal_types", [])
        return meal_type in supported
    
    def get_native_meal_code(self, provider_name: str, meal_type: str) -> Optional[str]:
        """Get provider-specific meal code for request-level filtering"""
        config = self.provider_capabilities.get(provider_name, {})
        mapping = config.get("native_mapping", {})
        return mapping.get(meal_type)
    
    def get_response_filter_values(self, provider_name: str, meal_type: str) -> List[str]:
        """Get list of response field values to match for response-level filtering"""
        config = self.provider_capabilities.get(provider_name, {})
        reverse_mapping = config.get("reverse_mapping", {})
        return reverse_mapping.get(meal_type, [])
    
    def should_filter_at_request_level(self, provider_name: str, meal_type: str) -> bool:
        """Determine if meal_type should be filtered at request level"""
        strategy = self.get_provider_strategy(provider_name)
        is_supported = self.is_meal_type_supported(provider_name, meal_type)
        
        result = strategy == FilteringStrategy.REQUEST_LEVEL and is_supported
        logger.debug(f"Provider {provider_name}: meal_type '{meal_type}' - "
                    f"Strategy: {strategy}, Supported: {is_supported}, "
                    f"Filter at request: {result}")
        return result
    
    def should_filter_at_response_level(self, provider_name: str, meal_type: str) -> bool:
        """Determine if meal_type should be filtered at response level"""
        strategy = self.get_provider_strategy(provider_name)
        is_supported = self.is_meal_type_supported(provider_name, meal_type)
        
        result = strategy == FilteringStrategy.RESPONSE_LEVEL and is_supported
        logger.debug(f"Provider {provider_name}: meal_type '{meal_type}' - "
                    f"Strategy: {strategy}, Supported: {is_supported}, "
                    f"Filter at response: {result}")
        return result
    
    def _extract_meal_from_offer(self, provider_name: str, offer: Dict) -> Optional[str]:
        """Extract meal information from provider-specific offer format"""
        # Get meal filtering config for this provider
        meal_config = config.get_meal_filtering_config(provider_name)
        if meal_config and meal_config.get("response_field"):
            response_field = meal_config["response_field"]
            return offer.get(response_field)
        
        # Fallback to common field names
        return offer.get("meal_plan") or offer.get("meal_type") or offer.get("meal")
    
    def filter_offers_by_any_meal_type(self, provider_name: str, offers: List[Dict], 
                                      meal_types: List[str]) -> List[Dict]:
        """
        Filter offers by multiple meal types using OR logic.
        Returns ALL offers from provider that match the meal types - no deduplication.
        
        Args:
            provider_name: Provider name (e.g., 'rate_hawk', 'goglobal')
            offers: List of offers to filter
            meal_types: List of meal type codes to match
        """
        if not meal_types or not offers:
            return offers
            
        valid_meal_types = [mt.strip() for mt in meal_types if mt and mt.strip()]
        if not valid_meal_types:
            return offers
        
        logger.info(f"Filtering {len(offers)} offers for {provider_name} with meal_types: {valid_meal_types}")
        
        # Check if we need response-level filtering
        needs_response_filtering = any(
            self.should_filter_at_response_level(provider_name, meal_type) 
            for meal_type in valid_meal_types
        )
        
        if not needs_response_filtering:
            logger.debug(f"No response-level filtering needed for {provider_name}")
            return offers
        
        # Build combined filter values
        all_filter_values = set()
        for meal_type in valid_meal_types:
            if self.should_filter_at_response_level(provider_name, meal_type):
                filter_values = self.get_response_filter_values(provider_name, meal_type)
                for value in filter_values:
                    all_filter_values.add(value.lower().strip())
        
        if not all_filter_values:
            logger.warning(f"No filter values found for {provider_name} meal_types {valid_meal_types}")
            return []
        
        # Filter offers - show ALL matching offers
        matching_offers = []
        
        for offer in offers:
            offer_meal = self._extract_meal_from_offer(provider_name, offer)
            
            if offer_meal:
                offer_meal_lower = offer_meal.lower().strip()
                
                if offer_meal_lower in all_filter_values:
                    matching_offers.append(offer)
        
        logger.info(f"{provider_name}: Filtered {len(offers)} to {len(matching_offers)} offers")
        
        # Log sample offers for debugging (first 3 offers)
        if matching_offers:
            logger.debug(f"{provider_name}: Sample filtered offers:")
            for i, offer in enumerate(matching_offers[:3]):
                meal_plan = offer.get('meal_plan', 'N/A')
                hotel_name = offer.get('hotel_name', 'N/A')
                room_name = offer.get('room_name', 'N/A') 
                total_price = offer.get('total_price', 'N/A')
                logger.debug(f"  {i+1}. {hotel_name} | {room_name} | {meal_plan} | {total_price}")
        
        return matching_offers

    def validate_meal_type(self, meal_code: str) -> bool:
        """Validate if meal type code is supported by any provider"""
        return meal_code in self._mappings


# Global instance (singleton pattern)
_meal_mapping_instance = None

def get_meal_mapping() -> MealMapping:
    """Get singleton instance of MealMapping service"""
    global _meal_mapping_instance
    if _meal_mapping_instance is None:
        _meal_mapping_instance = MealMapping()
    return _meal_mapping_instance

# Backward compatibility aliases
def get_meal_type_service() -> MealMapping:
    """Backward compatibility alias for get_meal_mapping"""
    return get_meal_mapping()

# Global service instances
meal_mapping_service = get_meal_mapping()
meal_type_service = get_meal_mapping()  # Backward compatibility
