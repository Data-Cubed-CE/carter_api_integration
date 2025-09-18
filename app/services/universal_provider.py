import aiohttp
import asyncio
import time
import logging
import importlib
from typing import Dict, Any, List, Optional
from abc import ABC, abstractmethod
from app.config import config
from app.services.circuit_breaker import CircuitBreaker, CircuitBreakerOpenError, CircuitState
from app.services.session_manager import session_manager

# Initialize logger
logger = logging.getLogger(__name__)

class ProviderAdapter(ABC):
    """Abstract base for all provider adapters"""
    
    def __init__(self, provider_name: str):
        """
        Initialize ProviderAdapter with provider name and config.
        Loads provider-specific configuration for authentication and endpoints.
        """
        self.provider_name = provider_name
        self.config = config.get_provider_config(provider_name)
        if not self.config:
            raise ValueError(f"No configuration found for provider: {provider_name}")
        # Session management is now handled by SessionManager
        # Remove individual session management
    
    @abstractmethod
    async def search(self, criteria: Dict[str, Any]) -> Dict[str, Any]:
        """
        Execute search request to provider API.
        Args:
            criteria (Dict[str, Any]): Search parameters
        Returns:
            Dict[str, Any]: Raw provider response
        """
        pass
    
    @abstractmethod
    def normalize(self, raw_response: Dict[str, Any], criteria: Dict[str, Any] = None) -> List[Dict[str, Any]]:
        """
        Normalize provider response to standard format.
        Args:
            raw_response (Dict[str, Any]): Raw provider response
        Returns:
            List[Dict[str, Any]]: List of normalized offers
        """
        pass
    
    def prepare_meal_type_criteria(self, criteria: Dict[str, Any]) -> Dict[str, Any]:
        """
        Prepare criteria with meal_type for provider-specific request.
        Override in child classes for providers supporting request-level filtering.
        
        Args:
            criteria: Original search criteria
        Returns:
            Dict: Modified criteria for provider API call
        """
        # Default implementation - no modification (response-level filtering)
        return criteria
    
    async def get_session(self) -> aiohttp.ClientSession:
        """
        Get HTTP session using centralized SessionManager.
        Returns optimized session with connection pooling.
        
        Returns:
            aiohttp.ClientSession: Optimized HTTP session
        """
        return await session_manager.get_session(self.provider_name)
    
    async def close(self):
        """Close the session - now handled by SessionManager"""
        # Session lifecycle is managed by SessionManager
        # Individual providers don't need to manage sessions
        pass

class UniversalProvider:
    """Universal provider that manages all hotel search providers"""
    
    def __init__(self):
        """
        Initialize UniversalProvider and load all configured provider adapters.
        Sets up circuit breakers.
        """
        self.adapters: Dict[str, ProviderAdapter] = {}
        self._circuit_breakers: Dict[str, CircuitBreaker] = {}
        self._load_adapters()
    
    def get_circuit_breaker(self, provider_name: str):
        """Public accessor for a provider's circuit breaker."""
        return self._circuit_breakers.get(provider_name)

    def reset_circuit_breaker(self, provider_name: str):
        """Public method to reset a provider's circuit breaker."""
        cb = self._circuit_breakers.get(provider_name)
        if cb:
            cb.reset()
    
    def _load_adapters(self):
        """
        Load all configured provider adapters dynamically from config.
        Sets up circuit breakers for each provider.
        """
        failed_providers = []
        logger = logging.getLogger(__name__)
        for provider_name in config.get_all_provider_names():
            try:
                # Dynamic import based on config
                provider_config = config.get_provider_config(provider_name)
                if not provider_config.get('active', True):
                    logger.debug(f"Provider {provider_name} is disabled, skipping")
                    continue
                module_path = provider_config['module']
                class_name = provider_config['class']
                # Import the module and get the class
                module = importlib.import_module(module_path)
                adapter_class = getattr(module, class_name)
                # Create adapter instance
                self.adapters[provider_name] = adapter_class(provider_name)
                # Create circuit breaker for this provider
                from app.config import Config
                self._circuit_breakers[provider_name] = CircuitBreaker(
                    failure_threshold=Config.CIRCUIT_BREAKER_FAILURE_THRESHOLD,
                    timeout=Config.CIRCUIT_BREAKER_TIMEOUT,
                    reset_timeout=Config.CIRCUIT_BREAKER_RESET_TIMEOUT,
                    name=f"cb_{provider_name}"
                )
                logger.debug(f"Successfully loaded provider: {provider_name}")
            except Exception as e:
                failed_providers.append((provider_name, str(e)))
                logger.error(f"Failed to load adapter for {provider_name}: {e}")
                logger.error(f"Provider config: {provider_config}")
                # Log more details for debugging
                import traceback
                logger.error(f"Full traceback: {traceback.format_exc()}")
        
        if failed_providers:
            logger.warning(f"Warning: {len(failed_providers)} providers failed to load")
            for provider, error in failed_providers:
                logger.warning(f"   - {provider}: {error}")
        
        if not self.adapters:
            raise RuntimeError("No providers could be loaded! Check your configuration.")
    
    async def search_single(self, provider_name: str, criteria: Dict[str, Any]) -> Dict[str, Any]:
        """
        Search using a single provider with circuit breaker and retry logic.
        Args:
            provider_name (str): Name of the provider
            criteria (Dict[str, Any]): Search parameters
        Returns:
            Dict[str, Any]: Search result including offers and status
        """
        if provider_name not in self.adapters:
            return {
                "status": "error",
                "error": f"The {provider_name} booking service is currently not available. Please try using other providers or contact support.",
                "offers": []
            }
        
        adapter = self.adapters[provider_name]
        circuit_breaker = self._circuit_breakers.get(provider_name)
        start_time = time.time()
        
        # Check circuit breaker first
        if circuit_breaker and circuit_breaker.state == CircuitState.OPEN:
            return {
                "status": "error",
                "provider": provider_name,
                "error": f"Service protection active: The {provider_name} booking service has been temporarily disabled after experiencing repeated connection failures. This is a safety measure to prevent further issues. The service will automatically retry in a few minutes.",
                "offers": [],
                "processing_time_ms": int((time.time() - start_time) * 1000),
                "circuit_breaker_state": circuit_breaker.state.value
            }
        
        # Retry logic with exponential backoff
        max_retries = config.MAX_RETRIES
        base_delay = config.RETRY_BASE_DELAY
        
        for attempt in range(max_retries):
            try:
                # Prepare criteria with meal_type handling for this provider
                provider_criteria = adapter.prepare_meal_type_criteria(criteria)
                
                # Note: Provider-specific hotel mapping is now handled directly in each provider's search() method
                
                if circuit_breaker:
                    # Use circuit breaker for the call
                    async def provider_call():
                        return await adapter.search(provider_criteria)
                    
                    raw_response = await circuit_breaker.call(provider_call)
                else:
                    # Direct call without circuit breaker
                    raw_response = await adapter.search(provider_criteria)
                
                normalized_offers = adapter.normalize(raw_response, criteria)
                
                # Apply meal_types filtering if specified
                meal_types = criteria.get("meal_types")
                logger.debug(f"DEBUG: meal_types from criteria = '{meal_types}' (type: {type(meal_types)}, bool: {bool(meal_types)})")
                
                if meal_types:
                    # Import here to avoid circular imports
                    from app.services.meal_mapping import meal_mapping_service as meal_type_service
                    
                    logger.debug(f"DEBUG: Checking should_filter_at_response_level for {provider_name}")
                    
                    # Apply response-level filtering if required by provider
                    # Check if any of the meal types requires response-level filtering
                    needs_response_filtering = any(
                        meal_type_service.should_filter_at_response_level(provider_name, meal_type) 
                        for meal_type in meal_types if meal_type
                    )
                    
                    if needs_response_filtering:
                        logger.debug(f"DEBUG: APPLYING response-level filtering for {provider_name}")
                        normalized_offers = meal_type_service.filter_offers_by_any_meal_type(
                            provider_name, normalized_offers, meal_types
                        )
                        logger.debug(f"{provider_name}: Applied response-level meal_types filtering for {meal_types}")
                    else:
                        logger.debug(f"DEBUG: should_filter_at_response_level returned False for {provider_name}")
                else:
                    logger.debug(f"DEBUG: No meal_types in criteria - skipping filtering")
                
                # Apply room_category filtering if specified
                room_category = criteria.get("room_category")
                logger.debug(f"DEBUG: room_category from criteria = '{room_category}' (type: {type(room_category)}, bool: {bool(room_category)})")
                
                if room_category:
                    initial_count = len(normalized_offers)
                    # Filter offers by room_category (case-insensitive matching)
                    filtered_offers = []
                    for offer in normalized_offers:
                        offer_category = offer.get('room_category', '').strip()
                        if offer_category.lower() == room_category.lower():
                            filtered_offers.append(offer)
                    
                    normalized_offers = filtered_offers
                    logger.debug(f"{provider_name}: Applied room_category filtering for '{room_category}' - {initial_count} -> {len(normalized_offers)} offers")
                else:
                    logger.debug(f"DEBUG: No room_category in criteria - skipping filtering")
                
                # Normalize meal_plan values to standard codes for final response
                from app.services.meal_mapping import meal_mapping_service as meal_type_service
                normalized_offers = meal_type_service.normalize_offers_meal_plans(normalized_offers, provider_name)
                
                return {
                    "status": "success",
                    "provider": provider_name,
                    "offers": normalized_offers,
                    "offer_count": len(normalized_offers),
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "attempts": attempt + 1,
                    "circuit_breaker_state": circuit_breaker.state.value if circuit_breaker else "disabled"
                }
                
            except CircuitBreakerOpenError as e:
                return {
                    "status": "error",
                    "provider": provider_name,
                    "error": f"Connection protection triggered: The {provider_name} booking service has been automatically disabled due to repeated connection issues (circuit breaker activated). We're monitoring the situation and will restore service automatically. Please try again in a few minutes or contact support if issues persist.",
                    "offers": [],
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "circuit_breaker_state": "open"
                }
                
            except asyncio.TimeoutError:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Timeout on {provider_name}, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                return {
                    "status": "error",
                    "provider": provider_name,
                    "error": f"Response timeout occurred: The {provider_name} service did not respond within the expected time limit after {max_retries} attempts. This may indicate high server load or network issues. Please try again later or contact support if the problem persists.",
                    "offers": [],
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "circuit_breaker_state": circuit_breaker.state.value if circuit_breaker else "disabled"
                }
            except Exception as e:
                if attempt < max_retries - 1:
                    delay = base_delay * (2 ** attempt)
                    logger.warning(f"Error on {provider_name}: {e}, retrying in {delay}s (attempt {attempt + 1}/{max_retries})")
                    await asyncio.sleep(delay)
                    continue
                return {
                    "status": "error",
                    "provider": provider_name,
                    "error": f"Service error encountered: We experienced technical difficulties while connecting to the {provider_name} booking service after {max_retries} retry attempts. This could be due to temporary server issues or network problems. Please try again later or contact support for assistance.",
                    "offers": [],
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "circuit_breaker_state": circuit_breaker.state.value if circuit_breaker else "disabled"
                }
    
    async def search_all(self, criteria: Dict[str, Any]) -> Dict[str, Any]:
        """
        Search using all available providers with optimized parallel processing.
        
        Features:
        - Concurrent execution of all providers
        - Timeout handling with partial results
        - Error isolation (one provider failure doesn't block others)
        - Progressive results (return available results even if some providers timeout)
        
        Args:
            criteria (Dict[str, Any]): Search parameters (including optional 'providers' list and 'hotel_names' list)
        Returns:
            Dict[str, Any]: Aggregated results and statistics
        """
        start_time = time.time()
        
        # Extract hotel names from criteria
        hotel_names = criteria.get("hotel_names", [])
        if not hotel_names:
            logger.error("No hotel names provided in criteria")
            return {
                "providers": {},
                "summary": {
                    "total_offers": 0,
                    "successful_providers": 0,
                    "processing_time_ms": int((time.time() - start_time) * 1000),
                    "hotel_count": 0
                }
            }
        
        # Filter providers based on criteria if specified
        selected_providers = criteria.get("providers", [])
        if selected_providers:
            # Validate and filter providers
            available_providers = list(self.adapters.keys())
            valid_providers = [p for p in selected_providers if p in available_providers]
            
            # Log provider filtering
            if len(valid_providers) != len(selected_providers):
                invalid_providers = [p for p in selected_providers if p not in available_providers]
                logger.warning(f"Ignoring invalid providers: {invalid_providers}")
                logger.warning(f"Available providers: {available_providers}")
            
            if not valid_providers:
                logger.warning(f"No valid providers found from request: {selected_providers}")
                logger.info(f"Falling back to all available providers: {available_providers}")
                providers_to_search = available_providers
            else:
                providers_to_search = valid_providers
        else:
            providers_to_search = list(self.adapters.keys())
        
        logger.info(f"Starting parallel search across {len(providers_to_search)} providers")
        
        # Create tasks for concurrent execution
        provider_tasks = {}
        for provider_name in providers_to_search:
            task = asyncio.create_task(
                self.search_single(provider_name, criteria),
                name=f"search_{provider_name}"
            )
            provider_tasks[provider_name] = task
        
        # Enhanced parallel processing with timeout and partial results
        provider_results = {}
        all_offers = []
        
        # Set timeout for provider searches (configurable)
        search_timeout = criteria.get('search_timeout', 10.0)  # 10 seconds default
        
        try:
            # Wait for all tasks with timeout
            completed_tasks = await asyncio.wait_for(
                asyncio.gather(*provider_tasks.values(), return_exceptions=True),
                timeout=search_timeout
            )
            
            # Process completed results
            for i, result in enumerate(completed_tasks):
                provider_name = providers_to_search[i]
                
                if isinstance(result, Exception):
                    logger.error(f"Provider {provider_name} failed with exception: {result}")
                    provider_results[provider_name] = {
                        "status": "error",
                        "error": str(result),
                        "offers": [],
                        "processing_time_ms": int((time.time() - start_time) * 1000)
                    }
                else:
                    provider_results[provider_name] = result
                    if result["status"] == "success":
                        all_offers.extend(result["offers"])
        
        except asyncio.TimeoutError:
            # Handle timeout - collect partial results from completed providers
            logger.warning(f"Search timeout after {search_timeout}s, collecting partial results")
            
            completed_providers = []
            timeout_providers = []
            
            for provider_name, task in provider_tasks.items():
                if task.done():
                    # Provider completed (success or error)
                    try:
                        result = task.result()  # Use result() instead of await to avoid CancelledError
                        provider_results[provider_name] = result
                        if result["status"] == "success":
                            all_offers.extend(result["offers"])
                        completed_providers.append(provider_name)
                    except asyncio.CancelledError:
                        # Task was cancelled during timeout
                        provider_results[provider_name] = {
                            "status": "timeout",
                            "error": f"Provider search was cancelled during timeout handling",
                            "offers": []
                        }
                        timeout_providers.append(provider_name)
                    except Exception as e:
                        logger.error(f"Provider {provider_name} failed: {e}")
                        provider_results[provider_name] = {
                            "status": "error", 
                            "error": str(e),
                            "offers": []
                        }
                        completed_providers.append(provider_name)
                else:
                    # Provider still running - cancel and mark as timeout
                    if not task.cancelled():
                        task.cancel()
                    provider_results[provider_name] = {
                        "status": "timeout",
                        "error": f"Provider search timed out after {search_timeout}s",
                        "offers": [],
                        "processing_time_ms": int(search_timeout * 1000)
                    }
                    timeout_providers.append(provider_name)
            
            logger.info(f"Partial results: {len(completed_providers)} completed, {len(timeout_providers)} timed out")
            if completed_providers:
                logger.info(f"Completed providers: {completed_providers}")
            if timeout_providers:
                logger.warning(f"Timed out providers: {timeout_providers}")
                
        except Exception as e:
            # Handle any other exceptions during parallel processing
            logger.error(f"Unexpected error during parallel search: {e}")
            for provider_name in providers_to_search:
                if provider_name not in provider_results:
                    provider_results[provider_name] = {
                        "status": "error",
                        "error": f"Unexpected error: {str(e)}",
                        "offers": []
                    }
        
        # Create comprehensive summary
        hotel_count = len(hotel_names)
        successful_providers_count = sum(1 for result in provider_results.values() if result["status"] == "success")
        error_providers_count = sum(1 for result in provider_results.values() if result["status"] == "error")
        timeout_providers_count = sum(1 for result in provider_results.values() if result["status"] == "timeout")
        
        processing_time_ms = int((time.time() - start_time) * 1000)
        
        logger.info(f"Search completed in {processing_time_ms}ms: {len(all_offers)} total offers")
        
        # Positive messaging for successful operations
        if error_providers_count == 0 and timeout_providers_count == 0:
            logger.info(f"All {successful_providers_count} providers responded successfully")
        else:
            logger.warning(f"Provider status: {successful_providers_count} successful, {error_providers_count} errors, {timeout_providers_count} timeouts")
        
        return {
            "providers": provider_results,
            "summary": {
                "total_offers": len(all_offers),
                "successful_providers": successful_providers_count,
                "error_providers": error_providers_count,
                "timeout_providers": timeout_providers_count,
                "processing_time_ms": processing_time_ms,
                "hotel_count": hotel_count,
                "hotels_searched": hotel_names,
                "search_timeout_used": search_timeout
            }
        }
    
    def get_available_providers(self) -> List[str]:
        """
        Get list of available provider names.
        Returns:
            List[str]: Provider names
        """
        return list(self.adapters.keys())
    
    async def close(self):
        """
        Close all sessions via SessionManager.
        Should be called on application shutdown.
        """
        logger.info("Closing UniversalProvider - delegating to SessionManager")
        await session_manager.close_all_sessions()
    

# Global instance
universal_provider = UniversalProvider()
