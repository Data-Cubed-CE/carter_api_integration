import os
import json
import logging
from pathlib import Path
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Load local.settings.json if available
def _load_local_settings() -> Dict[str, str]:
    """Load settings from local.settings.json if available"""
    try:
        settings_file = Path(__file__).parent.parent / "local.settings.json"
        if settings_file.exists():
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                return settings.get("Values", {})
    except Exception as e:
        pass
    return {}

_local_settings = _load_local_settings()

# Initialize logger for configuration issues
config_logger = logging.getLogger(__name__)

# Configure logging levels to reduce noise
logging.getLogger('azure.storage').setLevel(logging.WARNING)
logging.getLogger('azure.core').setLevel(logging.WARNING) 
logging.getLogger('azure.identity').setLevel(logging.WARNING)
logging.getLogger('aiohttp').setLevel(logging.WARNING)
logging.getLogger('urllib3').setLevel(logging.WARNING)

# Import Key Vault service (required for production)
try:
    from app.services.azure_keyvault_service import get_secret_directly, get_keyvault_service
    KEYVAULT_AVAILABLE = True
    config_logger.info("Azure Key Vault service loaded successfully")
except ImportError as e:
    config_logger.error(f"Azure Key Vault service not available: {e}")
    KEYVAULT_AVAILABLE = False
    
    # Fallback function when Key Vault is not available - raises error
    def get_secret_directly(secret_name: str) -> Optional[str]:
        raise RuntimeError(f"Key Vault service not available, cannot retrieve secret: {secret_name}")
    
    def get_keyvault_service():
        raise RuntimeError("Key Vault service not available")

class Config:
    """Unified configuration for all providers and app settings"""
    
    # Azure Key Vault secret mappings - based on actual Key Vault contents
    KEYVAULT_SECRETS = {
        # Azure SQL Database secrets
        "azure-sql-server": "AZURE_SQL_SERVER",
        "azure-sql-database": "AZURE_SQL_DATABASE",
        "azure-sql-username": "AZURE_SQL_USERNAME",
        "azure-sql-password": "AZURE_SQL_PASSWORD", 
        "azure-sql-use-azure-ad": "USE_AZURE_AD",
        "azure-sql-connection-timeout": "SQL_CONNECTION_TIMEOUT",
        "azure-sql-command-timeout": "SQL_COMMAND_TIMEOUT",
        
        # Provider credentials
        "rate-hawk-username": "RATE_HAWK_USERNAME",
        "rate-hawk-password": "RATE_HAWK_PASSWORD",
        "rate-hawk-base-url": "RATE_HAWK_BASE_URL",
        "goglobal-username": "GOGLOBAL_USERNAME", 
        "goglobal-password": "GOGLOBAL_PASSWORD",
        "goglobal-agency-id": "GOGLOBAL_AGENCY_ID",
        "goglobal-base-url": "GOGLOBAL_BASE_URL",
        "tbo-username": "TBO_USERNAME",
        "tbo-password": "TBO_PASSWORD", 
        "tbo-base-url": "TBO_BASE_URL",
        
        # Azure Storage
        "azure-storage-connection-string": "AZURE_STORAGE_CONNECTION_STRING",
        "azure-blob-container-name": "AZURE_BLOB_CONTAINER_NAME",
    }
    
    @classmethod
    def get_secret(cls, secret_name: str) -> Optional[str]:
        """Get secret from Key Vault only"""
        if not KEYVAULT_AVAILABLE:
            raise RuntimeError(f"Key Vault service required but not available for secret: {secret_name}")
        return get_secret_directly(secret_name)
    
    # Field Filtering Configuration - Simple and Elegant
    ALLOWED_FIELDS = [
        "provider",
        "supplier_hotel_id",
        "hotel_id",
        "hotel_name", 
        "supplier_room_code",
        "room_name",
        "room_category",
        "room_mapping_id",
        "meal_plan",
        "total_price",
        "currency",
        "room_features",
        "amenities",
        "free_cancellation_until"
    ]
    
    @classmethod
    def get_allowed_fields(cls):
        """Get list of allowed fields from environment variable or default"""
        env_fields = os.getenv("ALLOWED_FIELDS")
        if env_fields:
            return [field.strip() for field in env_fields.split(",")]
        return cls.ALLOWED_FIELDS
    
    @classmethod
    def is_field_allowed(cls, field_name: str) -> bool:
        """Check if field should be included in response"""
        return field_name in cls.get_allowed_fields()
    
    @classmethod
    def filter_response_data(cls, data):
        """Filter response data to only include allowed fields"""
        if not data:
            return data
            
        allowed_fields = set(cls.get_allowed_fields())
        
        # Handle different data structures
        if isinstance(data, dict):
            if "data" in data:
                # New flat response structure with data array
                filtered_data = data.copy()
                
                # Filter each offer in the data array
                if "data" in filtered_data and isinstance(filtered_data["data"], list):
                    filtered_data["data"] = [
                        {k: v for k, v in offer.items() if k in allowed_fields}
                        for offer in filtered_data["data"]
                        if isinstance(offer, dict)
                    ]
                
                return filtered_data
            elif "results_by_provider" in data:
                # Legacy response structure (keeping for backward compatibility)
                filtered_data = data.copy()
                
                # Filter each provider's offers
                if "results_by_provider" in filtered_data:
                    for provider_name, provider_result in filtered_data["results_by_provider"].items():
                        if isinstance(provider_result, dict) and "data" in provider_result:
                            if isinstance(provider_result["data"], list):
                                # Filter each offer in the provider's data
                                provider_result["data"] = [
                                    {k: v for k, v in offer.items() if k in allowed_fields}
                                    for offer in provider_result["data"]
                                    if isinstance(offer, dict)
                                ]
                
                return filtered_data
            else:
                # Simple dict filtering
                return {k: v for k, v in data.items() if k in allowed_fields}
        elif isinstance(data, list):
            # List of offers
            return [
                {k: v for k, v in item.items() if k in allowed_fields}
                if isinstance(item, dict) else item
                for item in data
            ]
        
        return data
    
    # Azure SQL Database Configuration
    AZURE_SQL_SERVER = get_secret_directly("azure-sql-server") if KEYVAULT_AVAILABLE else None
    AZURE_SQL_DATABASE = get_secret_directly("azure-sql-database") if KEYVAULT_AVAILABLE else None
    AZURE_SQL_USERNAME = get_secret_directly("azure-sql-username") if KEYVAULT_AVAILABLE else None
    AZURE_SQL_PASSWORD = get_secret_directly("azure-sql-password") if KEYVAULT_AVAILABLE else None
    USE_MANAGED_IDENTITY = os.getenv("USE_MANAGED_IDENTITY", "false").lower() == "true"
    USE_AZURE_AD = (get_secret_directly("azure-sql-use-azure-ad") or "false").lower() == "true" if KEYVAULT_AVAILABLE else False
    SQL_CONNECTION_TIMEOUT = int(get_secret_directly("azure-sql-connection-timeout") or "30") if KEYVAULT_AVAILABLE else 30
    SQL_COMMAND_TIMEOUT = int(get_secret_directly("azure-sql-command-timeout") or "30") if KEYVAULT_AVAILABLE else 30
    
    @classmethod
    def validate_azure_sql_config(cls) -> Dict[str, Any]:
        """Validate Azure SQL configuration"""
        validation_report = {
            "is_configured": False,
            "auth_method": "none",
            "missing_vars": [],
            "warnings": []
        }
        
        # Check basic requirements
        if not cls.AZURE_SQL_SERVER:
            validation_report["missing_vars"].append("AZURE_SQL_SERVER")
        if not cls.AZURE_SQL_DATABASE:
            validation_report["missing_vars"].append("AZURE_SQL_DATABASE")
            
        # Determine authentication method
        if cls.USE_MANAGED_IDENTITY:
            validation_report["auth_method"] = "managed_identity"
            validation_report["is_configured"] = len(validation_report["missing_vars"]) == 0
        elif cls.USE_AZURE_AD:
            validation_report["auth_method"] = "azure_ad"
            validation_report["is_configured"] = len(validation_report["missing_vars"]) == 0
        elif cls.AZURE_SQL_USERNAME and cls.AZURE_SQL_PASSWORD:
            validation_report["auth_method"] = "sql_server"
            validation_report["is_configured"] = len(validation_report["missing_vars"]) == 0
        else:
            validation_report["auth_method"] = "windows"
            validation_report["warnings"].append("Using Windows authentication - may not work in Azure environment")
            validation_report["is_configured"] = len(validation_report["missing_vars"]) == 0
            
        return validation_report
    
    @classmethod
    def get_azure_sql_config(cls) -> Dict[str, Any]:
        """Get Azure SQL configuration for connector"""
        return {
            "server": cls.AZURE_SQL_SERVER,
            "database": cls.AZURE_SQL_DATABASE,
            "username": cls.AZURE_SQL_USERNAME,
            "password": cls.AZURE_SQL_PASSWORD,
            "use_managed_identity": cls.USE_MANAGED_IDENTITY,
            "use_azure_ad": cls.USE_AZURE_AD,
            "connection_timeout": cls.SQL_CONNECTION_TIMEOUT,
            "command_timeout": cls.SQL_COMMAND_TIMEOUT
        }
    
    # Provider configurations
    PROVIDERS = {
        "rate_hawk": {
            "auth_type": "basic",
            "username": get_secret_directly("rate-hawk-username") if KEYVAULT_AVAILABLE else None,
            "password": get_secret_directly("rate-hawk-password") if KEYVAULT_AVAILABLE else None,
            "base_url": get_secret_directly("rate-hawk-base-url") if KEYVAULT_AVAILABLE else "https://api.worldota.net/api/b2b/v3/search/serp/hotels/",
            "timeout": 30,
            "module": "app.services.providers.rate_hawk",
            "class": "RateHawkProvider",
            "required_credentials": ["auth_type", "username", "password"],
            "active": True,
            "meal_filtering": {
                "strategy": "response_level",     # Filter on response level
                "sql_column": "RateHawk",        # CSV column for mappings
                "request_field": None,            # No request filtering
                "response_field": "meal_plan"     # Field in response to filter
            },
            "hotel_mapping": {
                "hotel_id_column": "rate_hawk_hotel_id"     # Database column with hotel IDs
            }
        },
        "goglobal": {
            "auth_type": "api_key", 
            "username": get_secret_directly("goglobal-username") if KEYVAULT_AVAILABLE else None,
            "password": get_secret_directly("goglobal-password") if KEYVAULT_AVAILABLE else None,
            "agency_id": get_secret_directly("goglobal-agency-id") if KEYVAULT_AVAILABLE else None,
            "base_url": get_secret_directly("goglobal-base-url") if KEYVAULT_AVAILABLE else "https://carter.xml.goglobal.travel/xmlwebservice.asmx",
            "timeout": 45,
            "module": "app.services.providers.goglobal",
            "class": "GoGlobalProvider",
            "required_credentials": ["auth_type", "username", "password", "agency_id"],
            "active": True,
            "meal_filtering": {
                "strategy": "request_level",      # Filter on request level
                "sql_column": "GoGlobal",         # CSV column for mappings
                "request_field": "meal_type",     # Field name in request
                "response_field": None            # No response filtering needed
            },
            "hotel_mapping": {
                "hotel_id_column": "goglobal_hotel_id"      # Database column with hotel IDs  
            }
        },
        "tbo": {
            "auth_type": "basic", 
            "username": get_secret_directly("tbo-username") if KEYVAULT_AVAILABLE else None,
            "password": get_secret_directly("tbo-password") if KEYVAULT_AVAILABLE else None,
            "base_url": get_secret_directly("tbo-base-url") if KEYVAULT_AVAILABLE else "http://api.tbotechnology.in/TBOHolidays_HotelAPI/search",
            "timeout": 25,
            "module": "app.services.providers.tbo",
            "class": "TBOProvider",
            "required_credentials": ["auth_type", "username", "password"],
            "active": True,
            "meal_filtering": {
                "strategy": "response_level",     # Filter on response level
                "sql_column": "TBO",              # CSV column for mappings
                "request_field": None,            # No request filtering
                "response_field": "meal_plan"     # Field in response to filter (FIXED: was MealType)
            },
            "hotel_mapping": {
                "hotel_id_column": "tbo_hotel_id"          # Database column with hotel IDs
            }
        }
    }
    
    @classmethod
    def get_active_providers(cls) -> Dict[str, Dict[str, Any]]:
        """Get only active providers from configuration"""
        return {
            name: config 
            for name, config in cls.PROVIDERS.items() 
            if config.get("active", False)
        }
    
    @classmethod
    def load_provider_instances(cls) -> Dict[str, Any]:
        """
        Dynamically load and instantiate provider classes from configuration.
        Returns dictionary of provider_name -> provider_instance
        """
        import importlib
        providers = {}
        
        for provider_name, config in cls.get_active_providers().items():
            try:
                # Import the module
                module_path = config.get("module")
                class_name = config.get("class")
                
                if not module_path or not class_name:
                    config_logger.warning(f"Missing module or class for provider {provider_name}")
                    continue
                
                # Dynamic import
                module = importlib.import_module(module_path)
                provider_class = getattr(module, class_name)
                
                # Instantiate with provider name
                provider_instance = provider_class(provider_name=provider_name)
                providers[provider_name] = provider_instance
                
                config_logger.info(f"Loaded provider: {provider_name}")
                
            except Exception as e:
                config_logger.error(f"Failed to load provider {provider_name}: {e}")
                continue
        
        return providers
    
    @classmethod
    def get_provider_names(cls) -> List[str]:
        """Get list of active provider names"""
        return list(cls.get_active_providers().keys())
    
    # App settings
    DEFAULT_CURRENCY = "EUR"
    DEFAULT_TIMEOUT = 30
    MAX_OFFERS_PER_PROVIDER = 99999
    
    # Circuit Breaker Settings
    CIRCUIT_BREAKER_FAILURE_THRESHOLD = 3
    CIRCUIT_BREAKER_TIMEOUT = 30.0
    CIRCUIT_BREAKER_RESET_TIMEOUT = 60.0
    
    # Retry Settings
    MAX_RETRIES = 3
    RETRY_BASE_DELAY = 1.0
    
    @classmethod
    def _ensure_data_directory(cls) -> None:
        """Ensure the data directory exists for CSV files."""
        data_dir = Path(__file__).parent / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
    
    # API settings
    API_TITLE = "Hotel Aggregator API"
    API_VERSION = "1.0.0"
    
    @classmethod
    def get_provider_config(cls, provider_name: str) -> Optional[Dict[str, Any]]:
        """Get configuration for specific provider"""
        cls._ensure_data_directory()  # Ensure data directory exists
        return cls.PROVIDERS.get(provider_name)
    
    @classmethod
    def get_all_provider_names(cls) -> List[str]:
        """Get list of all configured provider names"""
        return list(cls.PROVIDERS.keys())
    
    @classmethod
    def get_meal_filtering_config(cls, provider_name: str) -> Optional[Dict[str, Any]]:
        """Get meal filtering configuration for specific provider"""
        provider_config = cls.get_provider_config(provider_name)
        if provider_config:
            return provider_config.get("meal_filtering")
        return None
    
    @classmethod
    def get_hotel_mapping_config(cls, provider_name: str) -> Optional[Dict[str, Any]]:
        """Get hotel mapping configuration for specific provider"""
        provider_config = cls.get_provider_config(provider_name)
        if provider_config:
            return provider_config.get("hotel_mapping")
        return None
    
    @classmethod
    def is_azure_environment(cls) -> bool:
        """
        Detect if running in Azure environment based on LOCAL_ENVIRONMENT setting.
        """
        # Check for LOCAL_ENVIRONMENT flag from environment or local.settings.json
        local_env = os.getenv("LOCAL_ENVIRONMENT") or _local_settings.get("LOCAL_ENVIRONMENT", "false")
        local_env = local_env.lower()
        is_local = local_env in ["true", "1", "yes"]
        
        config_logger.debug(f"Environment detection: LOCAL_ENVIRONMENT={local_env}, is_local={is_local}")
        return not is_local
    
    @classmethod
    def get_provider_credentials(cls, provider_name: str) -> Optional[Dict[str, str]]:
        """Get credentials for specific provider, validating required fields."""
        config = cls.get_provider_config(provider_name)
        if config:
            # Get required credentials from provider configuration
            required_keys = config.get("required_credentials", ["auth_type", "username", "password"])
                
            creds = {k: config.get(k) for k in required_keys}
            # Validate all required fields are present and not empty
            if all(creds.get(k) for k in required_keys):
                return creds
            else:
                # Log missing credentials for debugging
                missing = [k for k in required_keys if not creds.get(k)]
                logger = logging.getLogger(__name__)
                logger.warning(f"Missing credentials for {provider_name}: {missing}")
                return None
        return None
    
    @classmethod
    def get_azure_storage_connection_string(cls) -> str:
        """Get Azure Storage connection string"""
        if KEYVAULT_AVAILABLE:
            return get_secret_directly("azure-storage-connection-string") or ""
        else:
            return ""

    @classmethod
    def get_blob_container_name(cls) -> str:
        """Get blob container name for storing responses"""
        if KEYVAULT_AVAILABLE:
            return get_secret_directly("azure-blob-container-name") or "hotel-responses"
        else:
            return "hotel-responses"
    
    @classmethod
    def validate_azure_deployment(cls) -> Dict[str, Any]:
        """
        Validate Azure deployment configuration using Key Vault.
        Returns validation report with missing configurations.
        """
        validation_report = {
            "is_valid": True,
            "missing_secrets": [],
            "missing_files": [],
            "provider_status": {},
            "warnings": [],
            "keyvault_status": "available" if KEYVAULT_AVAILABLE else "unavailable",
            "auth_consistency": True
        }
        
        if not KEYVAULT_AVAILABLE:
            validation_report["warnings"].append("Azure Key Vault service not available")
            validation_report["is_valid"] = False
            return validation_report

        # Validate authentication consistency
        local_environment = os.getenv("LOCAL_ENVIRONMENT", "false").lower() == "true"
        use_managed_identity = os.getenv("USE_MANAGED_IDENTITY", "false").lower() == "true"
        
        # In local environment, we should NOT use managed identity for SQL
        if local_environment and use_managed_identity:
            validation_report["warnings"].append("Warning: LOCAL_ENVIRONMENT=true but USE_MANAGED_IDENTITY=true - this may cause SQL authentication issues")
            validation_report["auth_consistency"] = False
        
        # Check Key Vault secrets for each provider
        for provider_name, provider_config in cls.PROVIDERS.items():
            missing_secrets = []
            
            # Get required credentials from provider configuration
            required_creds = provider_config.get("required_credentials", ["auth_type", "username", "password"])
            
            # Check each required credential (excluding auth_type as it's hardcoded)
            for cred_field in required_creds:
                if cred_field != "auth_type" and not provider_config.get(cred_field):
                    # Map credential field to Key Vault secret name
                    secret_name = f"{provider_name.replace('_', '-')}-{cred_field.replace('_', '-')}"
                    missing_secrets.append(secret_name)
                
            validation_report["provider_status"][provider_name] = {
                "configured": len(missing_secrets) == 0,
                "missing_secrets": missing_secrets
            }
            
            validation_report["missing_secrets"].extend(missing_secrets)
        
        # Check data files existence
        data_files = []
        
        for file_name, file_path in data_files:
            if not file_path.exists():
                validation_report["missing_files"].append({
                    "name": file_name,
                    "path": str(file_path),
                    "exists": False
                })
                validation_report["warnings"].append(f"Missing {file_name} file at {file_path}")
        
        # Check if running in Azure - multiple detection methods for reliability
        # Use existing method for Azure detection
        is_azure = cls.is_azure_environment()
            
        validation_report["is_azure_environment"] = is_azure
        validation_report["azure_detection_details"] = {
            "WEBSITE_INSTANCE_ID": os.getenv("WEBSITE_INSTANCE_ID") is not None,
            "WEBSITE_SITE_NAME": os.getenv("WEBSITE_SITE_NAME") is not None,
            "FUNCTIONS_WORKER_RUNTIME": os.getenv("FUNCTIONS_WORKER_RUNTIME") is not None,
            "WEBSITE_HOSTNAME": os.getenv("WEBSITE_HOSTNAME", ""),
            "FUNCTIONS_EXTENSION_VERSION": os.getenv("FUNCTIONS_EXTENSION_VERSION") is not None,
            "detected_as_azure": is_azure
        }
        
        if is_azure:
            # Azure-specific checks
            app_insights_key = os.getenv("APPINSIGHTS_INSTRUMENTATIONKEY")
            if not app_insights_key:
                validation_report["warnings"].append("Application Insights not configured")
        
        # Overall validation status
        validation_report["is_valid"] = (
            len(validation_report["missing_secrets"]) == 0 and
            len(validation_report["missing_files"]) == 0 and
            KEYVAULT_AVAILABLE
        )
        
        return validation_report
    @classmethod
    def validate_azure_sql_config(cls) -> Dict[str, Any]:
        """Validate Azure SQL configuration"""
        validation_report = {
            "is_configured": False,
            "auth_method": "none",
            "missing_vars": [],
            "warnings": []
        }
        
        # Check basic requirements
        if not cls.AZURE_SQL_SERVER:
            validation_report["missing_vars"].append("AZURE_SQL_SERVER")
        if not cls.AZURE_SQL_DATABASE:
            validation_report["missing_vars"].append("AZURE_SQL_DATABASE")
            
        # Determine authentication method
        if cls.USE_MANAGED_IDENTITY:
            validation_report["auth_method"] = "managed_identity"
            validation_report["is_configured"] = len(validation_report["missing_vars"]) == 0
        elif cls.USE_AZURE_AD:
            validation_report["auth_method"] = "azure_ad"
            validation_report["is_configured"] = len(validation_report["missing_vars"]) == 0
        elif cls.AZURE_SQL_USERNAME and cls.AZURE_SQL_PASSWORD:
            validation_report["auth_method"] = "sql_server"
            validation_report["is_configured"] = len(validation_report["missing_vars"]) == 0
        else:
            validation_report["auth_method"] = "windows"
            validation_report["warnings"].append("Using Windows authentication - may not work in Azure environment")
            validation_report["is_configured"] = len(validation_report["missing_vars"]) == 0
            
        return validation_report
    
    @classmethod
    def get_azure_sql_config(cls) -> Dict[str, Any]:
        """Get Azure SQL configuration for connector"""
        return {
            "server": cls.AZURE_SQL_SERVER,
            "database": cls.AZURE_SQL_DATABASE,
            "username": cls.AZURE_SQL_USERNAME,
            "password": cls.AZURE_SQL_PASSWORD,
            "use_managed_identity": cls.USE_MANAGED_IDENTITY,
            "use_azure_ad": cls.USE_AZURE_AD,
            "connection_timeout": cls.SQL_CONNECTION_TIMEOUT,
            "command_timeout": cls.SQL_COMMAND_TIMEOUT
        }
    
    # Provider configurations
    
    @classmethod
    def log_deployment_status(cls) -> None:
        """Log current deployment configuration status"""
        validation = cls.validate_azure_deployment()
        
        config_logger.info(f"Key Vault status: {validation['keyvault_status']}")
        
        if validation["is_azure_environment"]:
            config_logger.info("Running in Azure environment")
        else:
            config_logger.info("Running in local environment")
            
        if not validation["is_valid"]:
            config_logger.error("Deployment validation failed!")
            if validation.get("missing_secrets"):
                config_logger.error(f"Missing Key Vault secrets: {validation['missing_secrets']}")
            if validation.get("missing_files"):
                config_logger.error(f"Missing files: {validation['missing_files']}")
        else:
            config_logger.info("Deployment validation passed")
            
        for provider, status in validation["provider_status"].items():
            if status["configured"]:
                config_logger.info(f"Provider {provider}: ✅ Configured")
            else:
                missing = status.get("missing_secrets", status.get("missing_vars", []))
                config_logger.warning(f"Provider {provider}: ❌ Missing: {missing}")
    
    @classmethod
    def get_deployment_health(cls) -> Dict[str, Any]:
        """Get deployment health for API endpoints"""
        validation = cls.validate_azure_deployment()
        
        return {
            "environment": "azure" if cls.is_azure_environment() else "local",
            "validation_status": "healthy" if validation["is_valid"] else "unhealthy",
            "providers_configured": sum(1 for p in validation["provider_status"].values() if p["configured"]),
            "total_providers": len(validation["provider_status"]),
            "issues": validation.get("missing_secrets", []) + [f["name"] for f in validation.get("missing_files", [])],
            "warnings": validation["warnings"],
            "azure_detection": validation.get("azure_detection_details", {}),
            "keyvault_status": validation.get("keyvault_status", "unknown")
        }
        
# Global config instance
config = Config()

# Log deployment status on module import
config.log_deployment_status()
