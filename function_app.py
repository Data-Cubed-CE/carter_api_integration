"""
Azure Functions v2 Programming Model - Main Function App
Hotel Aggregator Service Entry Point
"""
import logging
import json

# Standard Azure Functions v2 structure - no need for sys.path manipulation
# The root __init__.py file makes this a proper Python package

import azure.functions as func
from azure.functions import AuthLevel
from contextlib import asynccontextmanager
from datetime import datetime

# Configure unified logging format for entire application
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] [SYSTEM] %(message)s',
    datefmt='%H:%M:%S',
    force=True  # Override any existing configuration
)
logger = logging.getLogger(__name__)

# Import universal_provider at module level to avoid import issues during cleanup
try:
    from app.services.universal_provider import universal_provider
    from app.config import Config
except ImportError:
    universal_provider = None
    Config = None


@asynccontextmanager
async def managed_search():
    """Context manager for search operations with proper cleanup"""
    try:
        yield
    finally:
        # Always cleanup sessions
        if universal_provider:
            try:
                await universal_provider.close()
                logger.info("Session cleanup completed")
            except Exception as cleanup_error:
                logger.error(f"Session cleanup failed: {cleanup_error}", exc_info=True)

# Initialize the function app
app = func.FunctionApp()


@app.function_name(name="HealthCheck")
@app.route(route="health", methods=["GET"], auth_level=AuthLevel.FUNCTION)
async def health_check(req: func.HttpRequest) -> func.HttpResponse:
    """Enhanced health check with deployment validation"""
    logger.info('Health check requested')

    try:
        from app.config import Config

        # Get deployment health information
        health_data = Config.get_deployment_health()

        # Basic service health
        service_health = {
            "status": "healthy" if health_data["validation_status"] == "healthy" else "degraded",
            "service": "hotel-aggregator-functions",
            "version": "1.0.0",
            "timestamp": datetime.utcnow().isoformat(),
            "environment": health_data["environment"],
            "deployment": health_data,
            "features": {
                "best_offers_enabled": Config.is_best_offers_enabled()
            }
        }

        # Add provider status if available
        try:
            from app.services.universal_provider import universal_provider
            providers = universal_provider.get_available_providers()
            service_health["providers"] = {
                "available": len(providers),
                "names": providers
            }
        except Exception as e:
            logger.warning(f"Could not load providers for health check: {e}")
            service_health["providers"] = {"error": "Provider initialization failed"}

        status_code = 200 if service_health["status"] == "healthy" else 503

        return func.HttpResponse(
            json.dumps(service_health, indent=2),
            status_code=status_code,
            mimetype="application/json"
        )

    except Exception as e:
        logger.error(f"Health check failed: {e}", exc_info=True)
        error_response = {
            "status": "unhealthy",
            "service": "hotel-aggregator-functions",
            "error": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
        return func.HttpResponse(
            json.dumps(error_response),
            status_code=503,
            mimetype="application/json"
        )


def _validate_deployment_health():
    """Check if service is properly configured for deployment"""
    from app.config import Config
    deployment_health = Config.get_deployment_health()

    if deployment_health["validation_status"] != "healthy":
        logger.error(f"Deployment validation failed: {deployment_health}")
        return func.HttpResponse(
            json.dumps({
                "error": "Service configuration error",
                "details": "Service is not properly configured for deployment",
                "issues": deployment_health["issues"],
                "environment": deployment_health["environment"]
            }),
            status_code=503,
            mimetype="application/json"
        )
    return None


def _validate_request_body(req: func.HttpRequest):
    """Parse and validate the search request"""
    req_body = req.get_json()

    if not req_body:
        return None, func.HttpResponse(
            '{"error": "Invalid request", "details": "Request body is required"}',
            status_code=400,
            mimetype="application/json"
        )

    # Basic validation
    required_fields = ['hotel_names', 'check_in', 'check_out', 'adults']
    missing_fields = [field for field in required_fields if field not in req_body]

    if missing_fields:
        return None, func.HttpResponse(
            json.dumps({"error": "Missing required fields", "details": missing_fields}),
            status_code=400,
            mimetype="application/json"
        )

    return req_body, None


async def _execute_hotel_search(req_body, deployment_health):
    """Execute the actual hotel search logic"""
    from app.main import search_hotels
    from app.models.request import HotelSearchRequest

    try:
        # Create HotelSearchRequest object
        search_request = HotelSearchRequest(**req_body)
        # Call the main search function directly
        response_obj = await search_hotels(search_request)

        # Convert Pydantic response to dict for JSON serialization
        if hasattr(response_obj, 'model_dump'):
            response_dict = response_obj.model_dump()
        elif hasattr(response_obj, 'dict'):
            response_dict = response_obj.dict()
        else:
            response_dict = response_obj

        # Apply field filtering to final response
        from app.config import Config
        
        # Debug: Check one offer before filtering
        if response_dict.get('results_by_provider'):
            for provider_name, provider_result in response_dict['results_by_provider'].items():
                if provider_result.get('data') and len(provider_result['data']) > 0:
                    first_offer = provider_result['data'][0]
                    has_cancellation = 'free_cancellation_until' in first_offer
                    cancellation_value = first_offer.get('free_cancellation_until')
        
        response_dict = Config.filter_response_data(response_dict)

        return func.HttpResponse(
            json.dumps(response_dict, default=str),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logger.error(f"Error in search execution: {e}", exc_info=True)
        error_response = {
            "error": "Search execution failed",
            "details": str(e),
            "type": type(e).__name__,
            "environment": deployment_health["environment"]
        }
        return func.HttpResponse(
            json.dumps(error_response),
            status_code=500,
            mimetype="application/json"
        )


@app.function_name(name="HotelSearch")
@app.route(route="search", methods=["POST"], auth_level=AuthLevel.FUNCTION)
async def hotel_search(req: func.HttpRequest) -> func.HttpResponse:
    """Main hotel search aggregation endpoint"""
    logger.info('Hotel search request received')

    try:
        # Validate deployment health
        health_response = _validate_deployment_health()
        if health_response:
            return health_response

        # Parse and validate request
        req_body, validation_error = _validate_request_body(req)
        if validation_error:
            return validation_error

        # Get deployment health for error context
        from app.config import Config
        deployment_health = Config.get_deployment_health()

        # Execute hotel search
        async with managed_search():
            return await _execute_hotel_search(req_body, deployment_health)

    except ImportError as e:
        logger.error(f"Import error: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "error": "Service configuration error",
                "details": "Failed to initialize required services"
            }),
            status_code=500,
            mimetype="application/json"
        )
    except ValueError as e:
        logger.error(f"Validation error: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "error": "Validation error",
                "details": "Invalid request parameters"
            }),
            status_code=400,
            mimetype="application/json"
        )
    except Exception as e:
        logger.error(f"Error processing hotel search: {e}", exc_info=True)
        return func.HttpResponse(
            json.dumps({
                "error": "Internal server error",
                "details": "An unexpected error occurred"
            }),
            status_code=500,
            mimetype="application/json"
        )


@app.function_name(name="ProvidersStatus")
@app.route(route="providers/status", methods=["GET"], auth_level=AuthLevel.FUNCTION)
async def providers_status(req: func.HttpRequest) -> func.HttpResponse:
    """Get status of all hotel providers"""
    logger.info('Providers status check requested')

    try:
        from app.services.universal_provider import universal_provider
        from datetime import datetime

        providers = universal_provider.get_available_providers()
        status_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "total_providers": len(providers),
            "providers": providers,
            "status": "operational"
        }

        return func.HttpResponse(
            json.dumps(status_data),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logger.error(f"Error getting providers status: {e}", exc_info=True)
        # Dynamic fallback response
        try:
            from app.config import config
            available_providers = list(config.PROVIDERS.keys())
        except Exception:
            available_providers = ["rate_hawk", "goglobal"]  # Known defaults

        fallback_data = {
            "timestamp": datetime.utcnow().isoformat(),
            "total_providers": len(available_providers),
            "providers": available_providers,
            "status": "operational",
            "note": "Fallback response - providers not fully loaded"
        }

        return func.HttpResponse(
            json.dumps(fallback_data),
            status_code=200,
            mimetype="application/json"
        )


@app.function_name(name="MealTypes")
@app.route(route="meal-types", methods=["GET"], auth_level=AuthLevel.FUNCTION)
async def meal_types(req: func.HttpRequest) -> func.HttpResponse:
    """Get supported meal types with descriptions"""
    logger.info('Meal types request received')

    try:
        from app.services.meal_mapping import get_meal_mapping as get_meal_type_service

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

        response_data = {
            "supported_meal_types": meal_types,
            "total_count": len(meal_types),
            "timestamp": datetime.utcnow().isoformat()
        }

        return func.HttpResponse(
            json.dumps(response_data),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logger.error(f"Error getting meal types: {e}", exc_info=True)

        # Fallback response with basic meal types
        fallback_meal_types = [
            {"code": "BB", "description": "Bed and Breakfast", "providers": {"goglobal": "BB", "rate_hawk": "breakfast"}},
            {"code": "HB", "description": "Half Board", "providers": {"goglobal": "HB", "rate_hawk": "half-board"}},
            {"code": "HBD", "description": "Half Board Dinner", "providers": {"goglobal": "HB", "rate_hawk": "half-board-dinner"}},
            {"code": "AI", "description": "All-Inclusive", "providers": {"goglobal": "AI", "rate_hawk": "full-board"}},
            {"code": "RO", "description": "Room Only", "providers": {"goglobal": "RO", "rate_hawk": "nomeal"}}
        ]

        fallback_data = {
            "supported_meal_types": fallback_meal_types,
            "total_count": len(fallback_meal_types),
            "timestamp": datetime.utcnow().isoformat(),
            "note": "Fallback response - CSV service not fully loaded"
        }

        return func.HttpResponse(
            json.dumps(fallback_data),
            status_code=200,
            mimetype="application/json"
        )


@app.function_name(name="DiagnosticsCheck")
@app.route(route="diagnostics", methods=["GET"], auth_level=AuthLevel.FUNCTION)
async def diagnostics_check(req: func.HttpRequest) -> func.HttpResponse:
    """Comprehensive diagnostics endpoint for Azure deployment troubleshooting"""
    logger.info('Diagnostics check requested')

    try:
        from app.config import Config
        import os
        import sys
        from pathlib import Path

        # Collect comprehensive diagnostic information
        diagnostics = {
            "timestamp": datetime.utcnow().isoformat(),
            "environment": {
                "is_azure": (
                    os.getenv("WEBSITE_SITE_NAME") is not None or
                    os.getenv("FUNCTIONS_WORKER_RUNTIME") is not None or
                    os.getenv("WEBSITE_HOSTNAME") is not None or
                    os.getenv("FUNCTIONS_EXTENSION_VERSION") is not None
                ),
                "python_version": sys.version,
                "working_directory": str(Path.cwd()),
                "function_app_directory": str(Path(__file__).parent)
            },
            "configuration": Config.validate_azure_deployment(),
            "system_info": {
                "environment_variables": {
                    key: "***" if any(secret in key.lower() for secret in ['password', 'key', 'secret']) else value
                    for key, value in os.environ.items()
                    if key.startswith(('RATE_HAWK', 'GOGLOBAL', 'APPINSIGHTS', 'WEBSITE', 'FUNCTIONS'))
                }
            },
            "file_system": {},
            "providers": {}
        }

        # Check critical file paths
        critical_paths = [
            ("app_directory", Path(__file__).parent / "app"),
            ("config_file", Path(__file__).parent / "app" / "config.py"),
            ("data_directory", Path(__file__).parent / "app" / "data"),
            ("meal_mappings", Config.MEAL_MAPPINGS_PATH)
        ]

        for name, path in critical_paths:
            diagnostics["file_system"][name] = {
                "path": str(path),
                "exists": path.exists(),
                "is_file": path.is_file() if path.exists() else False,
                "is_directory": path.is_dir() if path.exists() else False
            }

        # Try to import and test providers
        try:
            from app.services.universal_provider import universal_provider
            available_providers = universal_provider.get_available_providers()
            diagnostics["providers"]["available"] = available_providers
            diagnostics["providers"]["count"] = len(available_providers)

            # Test provider configurations
            for provider_name in available_providers:
                try:
                    provider_config = Config.get_provider_config(provider_name)
                    diagnostics["providers"][provider_name] = {
                        "configured": provider_config is not None,
                        "has_credentials": all([
                            provider_config.get("username"),
                            provider_config.get("password")
                        ]) if provider_config else False
                    }
                except Exception as e:
                    diagnostics["providers"][provider_name] = {"error": str(e)}

        except Exception as e:
            diagnostics["providers"]["error"] = f"Failed to load universal_provider: {str(e)}"

        # Determine overall health
        is_healthy = (
            diagnostics["configuration"]["is_valid"] and
            diagnostics["file_system"]["app_directory"]["exists"] and
            diagnostics["file_system"]["config_file"]["exists"]
        )

        diagnostics["overall_status"] = "healthy" if is_healthy else "unhealthy"

        status_code = 200 if is_healthy else 503

        return func.HttpResponse(
            json.dumps(diagnostics, indent=2, default=str),
            status_code=status_code,
            mimetype="application/json"
        )

    except Exception as e:
        logger.error(f"Diagnostics check failed: {e}", exc_info=True)
        error_response = {
            "error": "Diagnostics failed",
            "details": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
        return func.HttpResponse(
            json.dumps(error_response),
            status_code=500,
            mimetype="application/json"
        )


def _check_provider_configuration(provider_name):
    """Check provider configuration and credentials"""
    from app.config import Config

    diag_info = {
        "provider_name": provider_name,
        "configuration": {},
        "credentials_check": {},
        "load_status": "unknown",
        "load_error": None,
        "adapter_available": False,
        "circuit_breaker_state": None
    }

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
    else:
        diag_info["configuration"]["error"] = "No configuration found"
        diag_info["load_status"] = "no_config"

    return diag_info, provider_config


def _check_provider_adapter_status(provider_name, diag_info, provider_config):
    """Check if provider adapter is loaded and working"""
    import traceback
    import importlib

    if not provider_config:
        return diag_info

    # Check if adapter is loaded
    try:
        from app.services.universal_provider import universal_provider
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
    except Exception as universal_provider_error:
        diag_info["load_error"] = f"Universal provider error: {str(universal_provider_error)}"
        diag_info["load_traceback"] = traceback.format_exc()

    return diag_info


def _generate_diagnostics_summary():
    """Generate summary of provider diagnostics"""
    from app.config import Config

    summary = {
        "total_configured": len(Config.get_all_provider_names()),
        "successfully_loaded": 0,
        "failed_to_load": 0
    }

    try:
        from app.services.universal_provider import universal_provider
        summary["successfully_loaded"] = len(universal_provider.get_available_providers())
        summary["failed_to_load"] = len(Config.get_all_provider_names()) - len(universal_provider.get_available_providers())
    except Exception:
        summary["successfully_loaded"] = 0
        summary["failed_to_load"] = len(Config.get_all_provider_names())

    return summary


@app.function_name(name="ProvidersDetailedDiagnostics")
@app.route(route="providers/diagnostics", methods=["GET"], auth_level=AuthLevel.FUNCTION)
async def providers_detailed_diagnostics(req: func.HttpRequest) -> func.HttpResponse:
    """Detailed diagnostics for troubleshooting provider issues"""
    logger.info('Providers detailed diagnostics requested')

    try:
        from app.config import Config
        import traceback

        diagnostics = {}

        # Check all configured providers
        for provider_name in Config.get_all_provider_names():
            try:
                # Get configuration and credentials
                diag_info, provider_config = _check_provider_configuration(provider_name)

                # Check adapter status
                diag_info = _check_provider_adapter_status(provider_name, diag_info, provider_config)

            except Exception as e:
                diag_info = {"diagnostics_error": str(e), "diagnostics_traceback": traceback.format_exc()}

            diagnostics[provider_name] = diag_info

        # Generate summary
        summary = _generate_diagnostics_summary()

        response_data = {
            "diagnostics": diagnostics,
            "summary": summary,
            "timestamp": datetime.utcnow().isoformat()
        }

        return func.HttpResponse(
            json.dumps(response_data, indent=2, default=str),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logger.error(f"Providers diagnostics failed: {e}", exc_info=True)
        error_response = {
            "error": "Providers diagnostics failed",
            "details": str(e),
            "timestamp": datetime.utcnow().isoformat()
        }
        return func.HttpResponse(
            json.dumps(error_response),
            status_code=500,
            mimetype="application/json"
        )
