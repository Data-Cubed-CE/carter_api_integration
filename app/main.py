import os
import logging
import uuid
import asyncio
from datetime import datetime
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware

from app.models.request import HotelSearchRequest
from app.models.response import HotelSearchResponse, ProviderResult, MetaInfo
from app.services.universal_provider import universal_provider
from app.services.blob_storage import blob_storage_service
from app.utils.logger import get_logger
from app.config import Config

# Load environment variables
load_dotenv(dotenv_path=Path(__file__).parent.parent / ".env")

# Configure unified logging format
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [APP] %(message)s',
    datefmt='%H:%M:%S',
    force=True
)
logger = logging.getLogger(__name__)

# Initialize session logger for comprehensive logging
session_logger = get_logger()

app = FastAPI(
    title="Hotel Aggregator API",
    description="""
## Hotel Price Comparison & Aggregation API

### Error Handling
All endpoints return standardized error responses with detailed error codes and descriptions.
""",
    version="1.0.0",
    contact={
        "name": "Hotel Aggregator API Team",
        "email": "api-support@example.com",
    },
    license_info={
        "name": "MIT License",
        "url": "https://opensource.org/licenses/MIT",
    },
    servers=[
        {
            "url": "https://your-function-app.azurewebsites.net/api",
            "description": "Production server"
        },
        {
            "url": "http://localhost:7071/api",
            "description": "Local development server"
        }
    ],
    openapi_tags=[
        {
            "name": "Health",
            "description": "Health check and system status endpoints"
        },
        {
            "name": "Hotels",
            "description": "Hotel search and price comparison operations"
        },
        {
            "name": "Providers",
            "description": "Hotel supplier management and monitoring"
        },
        {
            "name": "Mappings",
            "description": "Hotel and room type mapping utilities"
        }
    ]
)


def parse_origins(origins_str):
    if not origins_str:
        return None
    return [o.strip() for o in origins_str.split(",") if o.strip()]


env_origins = os.getenv("CORS_ALLOWED_ORIGINS")
allowed_origins = parse_origins(env_origins) or [
    "http://localhost:3000",
    "http://localhost:8080",
    "http://127.0.0.1:3000",
    "http://127.0.0.1:8080",
]

app.add_middleware(
    CORSMiddleware,
    allow_origins=allowed_origins,
    allow_credentials=True,
    allow_methods=["GET", "POST", "PUT", "DELETE"],
    allow_headers=["Authorization", "Content-Type", "Accept"],
)


@app.get("/",
         tags=["Health"],
         summary="API Root - Health Check",
         description="Basic health check endpoint that returns API status and available providers",
         response_description="API status information with provider availability")
async def root():
    """
    **API Root Endpoint**

    Returns basic health information and system status.

    **Response includes:**
    - API status and timestamp
    - List of available hotel providers
    - System version information
    """
    return {
        "message": "Hotel Aggregator API",
        "status": "running",
        "timestamp": datetime.utcnow().isoformat(),
        "available_providers": universal_provider.get_available_providers()
    }


@app.get("/health",
         tags=["Health"],
         summary="Health Check",
         description="Detailed health check for monitoring and load balancers",
         response_description="Health status with timestamp and version")
async def health_check():
    """
    **System Health Check**

    Provides health status for monitoring systems, load balancers, and uptime checks.

    **Use cases:**
    - Load balancer health checks
    - Application monitoring
    - CI/CD deployment verification
    """
    return {
        "status": "healthy",
        "timestamp": datetime.utcnow().isoformat(),
        "version": "1.0.0"
    }


@app.get("/providers/status",
         tags=["Providers"],
         summary="Provider Status Check",
         description="Get real-time status of all configured hotel providers",
         response_description="Status information for each provider including health and configuration")
async def providers_status():
    """
    **Provider Health Monitoring**

    Returns detailed status information for all configured hotel suppliers.

    **Response includes:**
    - Provider availability status
    - Configuration validation
    - Circuit breaker states
    - Last known error information

    **Use cases:**
    - System monitoring and alerting
    - Troubleshooting provider issues
    - Capacity planning and load distribution
    """
    provider_statuses = {}

    for provider_name in universal_provider.get_available_providers():
        try:
            # Basic availability check
            if provider_name in universal_provider.adapters:
                adapter = universal_provider.adapters[provider_name]
                provider_config = adapter.config
                # Check if provider is active
                is_active = provider_config.get('active', True)
                # Basic connectivity check (simplified)
                status = {
                    "available": True,
                    "active": is_active,
                    "base_url": provider_config.get('base_url', 'N/A'),
                    "timeout": provider_config.get('timeout', 30),
                    "last_check": datetime.utcnow().isoformat()
                }
                if not is_active:
                    status["status"] = "disabled"
                else:
                    status["status"] = "healthy"
            else:
                status = {
                    "available": False,
                    "status": "unavailable",
                    "error": "Provider not loaded"
                }
        except Exception as e:
            logger.error(f"Error checking provider status for {provider_name}: {e}", exc_info=True)
            status = {
                "available": False,
                "status": "error",
                "error": "Internal error",
                "last_check": datetime.utcnow().isoformat()
            }
        provider_statuses[provider_name] = status

    return {
        "providers": provider_statuses,
        "total_providers": len(provider_statuses),
        "healthy_providers": sum(1 for p in provider_statuses.values() if p.get("status") == "healthy"),
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/rooms/mappings",
         tags=["Mappings"],
         summary="Get Room Type Mappings",
         description="Returns available room type mappings and categories",
         response_description="Room type mapping information")
async def get_room_mappings():
    """
    **Room Type Discovery**

    Returns available room type mappings for search filtering.

    **Note:** Currently returns empty as room mappings are not yet implemented.
    This endpoint is reserved for future room type filtering functionality.
    """
    # For now, return empty as we don't have room mappings implemented
    return {
        "rooms": [],
        "count": 0,
        "note": "Room mappings not yet implemented"
    }


def _prepare_search_criteria(request: HotelSearchRequest) -> dict:
    """Prepare search criteria from request"""
    children_count = len(request.children_ages) if request.children_ages else 0
    criteria = {
        "hotel_names": request.hotel_names,
        "check_in": request.check_in.isoformat(),
        "check_out": request.check_out.isoformat(),
        "adults": request.adults,
        "children": children_count,
        "children_ages": request.children_ages,
        "currency": request.currency or "EUR",
        "nationality": request.nationality or "PL"
    }

    if request.rooms:
        criteria["rooms"] = request.rooms
    if request.room_category:
        criteria["room_category"] = request.room_category
    if request.providers:
        criteria["providers"] = request.providers
    if request.user:
        criteria["user"] = request.user

    return criteria


def _validate_meal_types(request: HotelSearchRequest, criteria: dict, session_logger, request_id: str):
    """Validate meal types and add to criteria"""
    if request.meal_types:
        from app.services.meal_mapping import get_meal_type_service
        meal_service = get_meal_type_service()
        
        invalid_meal_types = []
        for meal_type in request.meal_types:
            if not meal_service.validate_meal_type(meal_type):
                invalid_meal_types.append(meal_type)

        if invalid_meal_types:
            session_logger.error(f"Invalid meal types: {invalid_meal_types}")
            raise HTTPException(
                status_code=400,
                detail=f"Invalid meal_types {invalid_meal_types}. Supported types: {meal_service.get_supported_meal_types('all')}"
            )

        criteria["meal_types"] = request.meal_types


def _process_provider_results(aggregated_results: dict, session_logger) -> dict:
    """Process and validate provider results"""
    results_by_provider = {}
    for provider_name, provider_result in aggregated_results["providers"].items():
        if provider_result["status"] == "success":
            # Validate offers data before processing
            offers_data = provider_result.get("offers", [])
            if not isinstance(offers_data, list):
                session_logger.warning(f"Provider {provider_name} returned non-list offers: {type(offers_data)}")
                offers_data = []

            # Validate each offer has required fields
            validated_offers = []
            for i, offer in enumerate(offers_data):
                if not isinstance(offer, dict):
                    session_logger.warning(f"Provider {provider_name} offer {i} is not dict: {type(offer)}")
                    continue

                # Check for required fields
                required_fields = ["total_price", "currency", "room_name"]
                if all(field in offer for field in required_fields):
                    validated_offers.append(offer)
                else:
                    missing = [f for f in required_fields if f not in offer]
                    session_logger.warning(f"Provider {provider_name} offer {i} missing fields: {missing}")

            results_by_provider[provider_name] = ProviderResult(
                status="success",
                data=validated_offers,
                error=None
            )
        else:
            results_by_provider[provider_name] = ProviderResult(
                status="error",
                data=None,
                error=provider_result.get("error", "Unknown error")
            )

    return results_by_provider


@app.post("/hotels/search",
          tags=["Hotels"],
          summary="Hotel Price Search & Comparison",
          description="Search and compare hotel prices from multiple suppliers",
          response_model=HotelSearchResponse,
          response_description="Aggregated search results with price comparison from all providers")
async def search_hotels(request: HotelSearchRequest):
    """
    **Multi-Provider Hotel Search**

    Searches all configured hotel suppliers in parallel and returns aggregated price comparison results.

    **Request Requirements:**
    - `hotel_names`: List of exact hotel names from `/hotels/mappings` endpoint. Use single item for one hotel: ["Hotel Name"], or multiple items for multi-hotel search: ["Hotel 1", "Hotel 2"]
    - `check_in` / `check_out`: Must be future dates with minimum 1 night stay
    - `adults`: Number of adult guests (minimum 1)
    - `children_ages`: Optional list of child ages (0-17 years)
    - `providers`: Optional list of specific providers to search (e.g., ["rate_hawk", "tbo"]). If empty or not provided, all available providers will be used.

    **Search Process:**
    1. **Provider Selection**: Filters providers based on request (if specified)
    2. **Hotel Mapping**: Validates hotel availability across selected providers
    3. **Parallel Search**: Simultaneous API calls to selected suppliers
    4. **Data Normalization**: Converts responses to unified format
    5. **Price Aggregation**: Combines offers with provider metadata
    6. **Result Ranking**: Sorts by price, availability, and quality

    **Response Structure:**
    - `meta`: Request metadata and processing statistics
    - `search_criteria`: Echo of submitted search parameters
    - `results_by_provider`: Individual provider results and status
    - `summary`: Aggregated insights and price ranges

    **Error Handling:**
    - Provider failures don't block other providers
    - Partial results returned even if some providers fail
    - Detailed error information for troubleshooting

    **Performance:**
    - Typical response time: 2-5 seconds
    - Circuit breaker prevents cascading failures
    - Concurrent API calls for optimal speed
    """
    start_time = datetime.utcnow()
    timestamp = int(start_time.timestamp())
    uuid_short = uuid.uuid4().hex[:8]
    request_id = f"req_{timestamp}_{uuid_short}"

    try:
        # Start search session logging
        logger.info(f"[SEARCH] Started session: {request_id}")
        
        # Log user information if provided
        if request.user:
            logger.info(f"[SEARCH] User: {request.user}")
        
        # Simple request logging
        nights = (request.check_out - request.check_in).days
        logger.info(f"[SEARCH] Hotels: {len(request.hotel_names)}, Nights: {nights}, Adults: {request.adults}")
        if request.providers:
            logger.info(f"[SEARCH] Providers: {request.providers}")
        if request.meal_types:
            logger.info(f"[SEARCH] Meals: {request.meal_types}")
            
        # Keep session logger for file dump functionality
        search_params = {
            "hotel_names": request.hotel_names,
            "check_in": str(request.check_in),
            "check_out": str(request.check_out),
            "adults": request.adults,
            "children_ages": request.children_ages,
            "currency": request.currency or "EUR",
            "nationality": request.nationality or "PL",
            "rooms": request.rooms,
            "room_category": request.room_category,
            "meal_types": request.meal_types,
            "user": request.user
        }
        # session_logger.start_search_session(request_id, search_params)  # Replaced with new logging

        # Prepare search criteria
        criteria = _prepare_search_criteria(request)

        # Validate meal types if provided
        _validate_meal_types(request, criteria, logger, request_id)

        # Hotel mapping section
        logger.info("[MAPPING] Starting hotel mapping")

        # Search using universal provider
        aggregated_results = await universal_provider.search_all(criteria)
        
        # Provider responses section
        logger.info("[PROVIDERS] Processing provider responses")
        
        # Simple API response summary
        total_api_offers = 0
        successful_providers = 0
        for provider_name, provider_result in aggregated_results["providers"].items():
            if provider_result["status"] == "success":
                successful_providers += 1
                total_api_offers += len(provider_result.get("offers", []))

        logger.info(f"[PROVIDERS] Results: {total_api_offers} offers from {successful_providers}/3 providers")

        # Flatten results directly - combine all offers from all providers
        all_offers = []
        successful_providers = 0
        provider_breakdown = {}
        
        for provider_name, provider_result in aggregated_results["providers"].items():
            if provider_result["status"] == "success":
                successful_providers += 1
                # Validate offers data before processing
                offers_data = provider_result.get("offers", [])
                if not isinstance(offers_data, list):
                    session_logger.warning(f"Provider {provider_name} returned non-list offers: {type(offers_data)}")
                    offers_data = []

                # Process and add each offer with provider field
                validated_offers = []
                for i, offer in enumerate(offers_data):
                    if not isinstance(offer, dict):
                        session_logger.warning(f"Provider {provider_name} offer {i} is not dict: {type(offer)}")
                        continue

                    # Check for required fields
                    required_fields = ["total_price", "currency", "room_name"]
                    if all(field in offer for field in required_fields):
                        # Create new offer with provider field first for proper ordering
                        ordered_offer = {"provider": provider_name}
                        ordered_offer.update(offer)
                        validated_offers.append(ordered_offer)
                        all_offers.append(ordered_offer)
                    else:
                        missing = [f for f in required_fields if f not in offer]
                        session_logger.warning(f"Provider {provider_name} offer {i} missing fields: {missing}")

                provider_breakdown[provider_name] = {
                    "status": "success",
                    "offers_count": len(validated_offers),
                    "processing_time_ms": provider_result.get("processing_time_ms")
                }
            else:
                provider_breakdown[provider_name] = {
                    "status": "error",
                    "offers_count": 0,
                    "processing_time_ms": provider_result.get("processing_time_ms"),
                    "error": provider_result.get("error", "Unknown error")
                }

        logger.info(f"[NORMALIZATION] Processed {len(all_offers)} offers from {successful_providers} providers")

        # Filter fields
        filtered_offers = Config.filter_response_data(all_offers)

        # Apply room categorization to all offers centrally (final step)
        try:
            from app.services.room_mapping import get_room_mapping_service
            room_service = get_room_mapping_service()
            
            categorized_count = 0
            for offer in filtered_offers:
                room_name = offer.get('room_name', '')
                if room_name:
                    room_category = room_service.get_room_class(room_name)
                    if room_category:
                        # Format room_class to be more readable (e.g. junior_suite -> Junior Suite)
                        formatted_category = room_category.replace('_', ' ').title()
                        offer['room_category'] = formatted_category
                        categorized_count += 1
                    else:
                        offer['room_category'] = 'Other'
                else:
                    offer['room_category'] = 'Other'
            
            logger.info(f"[NORMALIZATION] Room categorization completed successfully")
            
        except Exception as e:
            logger.error(f"[NORMALIZATION] Room categorization failed: {e}")
            # Continue processing - set all to 'Other' if categorization fails
            for offer in filtered_offers:
                offer.setdefault('room_category', 'Other')

        # Simple final results
        processing_time = (datetime.utcnow() - start_time).total_seconds() * 1000
        logger.info(f"[RESULTS] Search completed: {len(filtered_offers)} offers in {processing_time:.0f}ms")

        # Auto-save complete session dump
        session_results = {
            "total_offers": len(all_offers),
            "successful_providers": successful_providers,
            "total_providers": len(provider_breakdown),
            "processing_time_ms": processing_time,
            "provider_breakdown": provider_breakdown
        }
        dump_file = session_logger.end_search_session(session_results)
        if dump_file:
            logger.debug(f"[RESULTS] Complete session saved to: {dump_file}")

        # Create response in new flat format
        response_data = {
            "meta": {
                "request_id": request_id,
                "timestamp": datetime.utcnow().isoformat(),
                "total_providers": len(provider_breakdown),
                "successful_providers": successful_providers,
                "total_results": len(all_offers),
                "processing_time_ms": processing_time,
                "provider_breakdown": provider_breakdown
            },
            "search_criteria": {
                "hotel_names": request.hotel_names,
                "city": request.city,
                "check_in": criteria.get("check_in"),
                "check_out": criteria.get("check_out"),
                "adults": criteria.get("adults"),
                "children_ages": criteria.get("children_ages"),
                "rooms": criteria.get("rooms"),
                "room_category": criteria.get("room_category"),
                "nationality": criteria.get("nationality"),
                "currency": criteria.get("currency"),
                "meal_types": request.meal_types or [],
                "children": criteria.get("children", 0),
                "user": criteria.get("user")
            },
            "data": filtered_offers
        }

        # Schedule background save to blob storage (non-blocking)
        async def save_to_blob():
            try:
                blob_info = await blob_storage_service.save_response_async(
                    request_id=request_id,
                    response_data=response_data,
                    user=request.user
                )
                if blob_info:
                    logger.info(f"[RESULTS] Saved to blob storage successfully")
            except Exception as e:
                logger.warning(f"[RESULTS] Blob storage failed: {e}")

        # Start background task (non-blocking)
        asyncio.create_task(save_to_blob())

        return response_data

    except Exception as e:
        # Save session even if error occurred
        if session_logger and hasattr(session_logger, 'end_search_session'):
            error_summary = {
                "error": str(e),
                "error_type": type(e).__name__,
                "successful": False
            }
            dump_file = session_logger.end_search_session(error_summary)
            if dump_file:
                logger.debug(f"[RESULTS] Error session saved to: {dump_file}")

        logger.error(f"[SEARCH] Request {request_id}: Search error: {e}", exc_info=True)
        raise HTTPException(status_code=500, detail="Internal server error")


@app.get("/meal-types")
async def get_supported_meal_types():
    """Get list of supported meal types with descriptions."""
    from app.services.meal_mapping import get_meal_type_service

    meal_service = get_meal_type_service()
    all_mappings = meal_service.meal_mapping.get_all_mappings()

    meal_types = []
    for code, mapping in all_mappings.items():
        meal_types.append({
            "code": code,
            "description": mapping.get("description", ""),
            "providers": {
                provider: mapping.get(provider)
                for provider in ["goglobal", "rate_hawk"]
                if mapping.get(provider)
            }
        })

    return {
        "supported_meal_types": meal_types,
        "total_count": len(meal_types)
    }


@app.get("/providers/circuit-breakers")
async def get_circuit_breakers_status():
    """Get status of all circuit breakers."""
    circuit_breakers_status = {}
    for provider_name in universal_provider.get_available_providers():
        cb = universal_provider.get_circuit_breaker(provider_name)
        if cb:
            circuit_breakers_status[provider_name] = {
                "state": cb.state.value if hasattr(cb.state, 'value') else str(cb.state),
                "failure_count": cb.failure_count if hasattr(cb, 'failure_count') else None,
                "failure_threshold": getattr(cb, 'failure_threshold', None),
                "last_failure_time": cb.last_failure_time if hasattr(cb, 'last_failure_time') else None,
                "reset_timeout": getattr(cb, 'reset_timeout', None)
            }
        else:
            circuit_breakers_status[provider_name] = {"state": "not found"}
    return {
        "circuit_breakers": circuit_breakers_status,
        "timestamp": datetime.utcnow().isoformat()
    }


@app.post("/providers/{provider_name}/circuit-breaker/reset")
async def reset_circuit_breaker(provider_name: str):
    """Manually reset a circuit breaker for a specific provider."""
    cb = universal_provider.get_circuit_breaker(provider_name)
    if not cb:
        raise HTTPException(status_code=404, detail=f"Circuit breaker for provider '{provider_name}' not found")
    universal_provider.reset_circuit_breaker(provider_name)
    return {
        "message": f"Circuit breaker for '{provider_name}' has been reset",
        "provider": provider_name,
        "new_state": cb.state.value if hasattr(cb.state, 'value') else str(cb.state),
        "timestamp": datetime.utcnow().isoformat()
    }


@app.get("/providers/diagnostics",
         tags=["Providers"],
         summary="Provider Diagnostics",
         description="Detailed diagnostics for troubleshooting provider issues",
         response_description="Comprehensive provider diagnostic information")
async def providers_diagnostics():
    """
    **Provider Diagnostics**

    Returns detailed diagnostic information for all providers.
    Useful for troubleshooting why a provider might not be available.
    """
    from app.config import Config
    import traceback

    diagnostics = {}

    # Check all configured providers
    for provider_name in Config.get_all_provider_names():
        diag_info = {
            "provider_name": provider_name,
            "configuration": {},
            "credentials_check": {},
            "load_status": "unknown",
            "load_error": None,
            "adapter_available": False,
            "circuit_breaker_state": None
        }

        try:
            # Get configuration
            provider_config = Config.get_provider_config(provider_name)
            if provider_config:
                diag_info["configuration"] = {
                    "active": provider_config.get('active', True),
                    "module": provider_config.get('module'),
                    "class": provider_config.get('class'),
                    "base_url": provider_config.get('base_url'),
                    "timeout": provider_config.get('timeout')
                }

                # Check credentials
                credentials = Config.get_provider_credentials(provider_name)
                if credentials:
                    diag_info["credentials_check"]["status"] = "valid"
                    diag_info["credentials_check"]["fields"] = list(credentials.keys())
                    # Don't log actual values for security
                    diag_info["credentials_check"]["values_present"] = {
                        k: bool(v) for k, v in credentials.items()
                    }
                else:
                    diag_info["credentials_check"]["status"] = "invalid"
                    diag_info["credentials_check"]["error"] = "Missing or incomplete credentials"

                # Check if adapter is loaded
                if provider_name in universal_provider.adapters:
                    diag_info["adapter_available"] = True
                    diag_info["load_status"] = "success"

                    # Check circuit breaker
                    cb = universal_provider.get_circuit_breaker(provider_name)
                    if cb:
                        diag_info["circuit_breaker_state"] = {
                            "state": cb.state.value,
                            "failure_count": cb.failure_count,
                            "failure_threshold": cb.failure_threshold,
                            "last_failure_time": cb.last_failure_time.isoformat() if cb.last_failure_time else None
                        }
                else:
                    diag_info["load_status"] = "failed"
                    diag_info["load_error"] = "Provider not in adapters list"

                    # Try to load manually to get the actual error
                    try:
                        import importlib
                        module_path = provider_config['module']
                        class_name = provider_config['class']
                        module = importlib.import_module(module_path)
                        adapter_class = getattr(module, class_name)
                        adapter_class(provider_name)  # Test instantiation
                        diag_info["manual_load_test"] = "success"
                    except Exception as e:
                        diag_info["manual_load_test"] = "failed"
                        diag_info["manual_load_error"] = str(e)
                        diag_info["manual_load_traceback"] = traceback.format_exc()
            else:
                diag_info["configuration"]["error"] = "No configuration found"
                diag_info["load_status"] = "no_config"

        except Exception as e:
            diag_info["diagnostics_error"] = str(e)
            diag_info["diagnostics_traceback"] = traceback.format_exc()

        diagnostics[provider_name] = diag_info

    return {
        "diagnostics": diagnostics,
        "summary": {
            "total_configured": len(Config.get_all_provider_names()),
            "successfully_loaded": len(universal_provider.get_available_providers()),
            "failed_to_load": len(Config.get_all_provider_names()) - len(universal_provider.get_available_providers())
        },
        "timestamp": datetime.utcnow().isoformat()
    }


@app.on_event("startup")
async def startup_event():
    """Initialize services on startup"""
    logger.info("Starting Hotel Aggregator API")
    logger.info(f"Loaded {len(universal_provider.get_available_providers())} providers: {', '.join(universal_provider.get_available_providers())}")


@app.on_event("shutdown")
async def shutdown_event():
    """Cleanup on shutdown"""
    logger.info("Shutting down Hotel Aggregator API")
    await universal_provider.close()
    logger.info("Cleanup completed")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
