"""
Azure Key Vault Service
Provides secure credential management through Azure Key Vault integration.
"""

import os
import json
import logging
from typing import Dict, Optional, Any
from pathlib import Path
from azure.identity import DefaultAzureCredential, ManagedIdentityCredential
from azure.keyvault.secrets import SecretClient
from azure.core.exceptions import AzureError

logger = logging.getLogger(__name__)

def _load_local_settings() -> Dict[str, str]:
    """Load settings from local.settings.json if available"""
    try:
        settings_file = Path(__file__).parent.parent.parent / "local.settings.json"
        if settings_file.exists():
            with open(settings_file, 'r') as f:
                settings = json.load(f)
                return settings.get("Values", {})
    except Exception as e:
        logger.debug(f"Could not load local.settings.json: {e}")
    return {}

# Load local settings
_local_settings = _load_local_settings()

class AzureKeyVaultService:
    """Service for managing secrets from Azure Key Vault"""
    
    def __init__(self, vault_url: Optional[str] = None):
        """
        Initialize Azure Key Vault client.
        
        Args:
            vault_url: Azure Key Vault URL. If None, uses AZURE_KEY_VAULT_URL env var.
        """
        self.vault_url = vault_url or os.getenv("AZURE_KEY_VAULT_URL")
        self.client = None
        self._secrets_cache = {}
        self.cache_enabled = os.getenv("CLEAR_KEYVAULT_CACHE_AFTER_REQUEST", "true").lower() == "true"
        
        if not self.vault_url:
            logger.warning("Azure Key Vault URL not configured. Service will operate in fallback mode.")
            return
            
        try:
            self._initialize_client()
            logger.info(f"Azure Key Vault service initialized for: {self.vault_url}")
        except Exception as e:
            logger.error(f"Failed to initialize Azure Key Vault client: {e}")
            self.client = None
    
    def _initialize_client(self):
        """Initialize the Key Vault client with appropriate credentials"""
        try:
            is_azure = self._is_azure_environment()
            logger.info(f"Initializing Key Vault client - Azure environment: {is_azure}")
            
            # Always try ManagedIdentityCredential first in Azure Functions
            # as it's more reliable than DefaultAzureCredential in Azure Functions environment
            if is_azure:
                logger.info("Using Managed Identity for Key Vault authentication")
                credential = ManagedIdentityCredential()
            else:
                logger.info("Using Default Azure Credential for Key Vault authentication")
                credential = DefaultAzureCredential()
            
            self.client = SecretClient(vault_url=self.vault_url, credential=credential)
            
            # Test connection by trying to list secrets (just to verify access)
            # This will raise an exception if authentication fails
            logger.info("Testing Key Vault connection...")
            secrets_list = list(self.client.list_properties_of_secrets(max_page_size=3))
            logger.info(f"Successfully connected to Key Vault. Found {len(secrets_list)} secrets (sample)")
            
            # Log first few secret names for debugging (without values)
            for secret_prop in secrets_list:
                logger.debug(f"Available secret: {secret_prop.name}")
            
        except Exception as e:
            logger.error(f"Failed to authenticate with Key Vault: {e}")
            logger.error(f"Vault URL: {self.vault_url}")
            logger.error(f"Azure environment detected: {is_azure}")
            self.client = None
            raise
    
    def _is_azure_environment(self) -> bool:
        """Check if running in Azure environment based on LOCAL_ENVIRONMENT setting"""
        # Check for LOCAL_ENVIRONMENT flag from environment or local.settings.json
        local_env = os.getenv("LOCAL_ENVIRONMENT") or _local_settings.get("LOCAL_ENVIRONMENT", "false")
        local_env = local_env.lower()
        is_local = local_env in ["true", "1", "yes"]
        
        logger.debug(f"Environment detection: LOCAL_ENVIRONMENT={local_env}, is_local={is_local}")
        # When LOCAL_ENVIRONMENT="true" → we are local → use DefaultAzureCredential
        # When LOCAL_ENVIRONMENT="false" → we are in Azure → use ManagedIdentity
        return not is_local
    
    def get_secret(self, secret_name: str) -> Optional[str]:
        """
        Retrieve a secret from Key Vault.
        
        Args:
            secret_name: Name of the secret in Key Vault
            
        Returns:
            Secret value or None if not found
        """
        # Check cache first
        if secret_name in self._secrets_cache:
            logger.debug(f"Retrieved secret '{secret_name}' from cache")
            return self._secrets_cache[secret_name]
        
        # If Key Vault client not available, return None
        if not self.client:
            logger.error(f"Key Vault client not available for secret '{secret_name}'")
            return None
        
        try:
            logger.info(f"Attempting to retrieve secret '{secret_name}' from Key Vault")
            secret = self.client.get_secret(secret_name)
            secret_value = secret.value
            
            # Cache the secret
            self._secrets_cache[secret_name] = secret_value
            
            logger.info(f"Successfully retrieved secret '{secret_name}' from Key Vault (length: {len(secret_value) if secret_value else 0})")
            return secret_value
            
        except AzureError as e:
            logger.error(f"Failed to retrieve secret '{secret_name}' from Key Vault: {e}")
            logger.error(f"Error type: {type(e).__name__}")
            return None
        except Exception as e:
            logger.error(f"Unexpected error retrieving secret '{secret_name}': {e}")
            logger.error(f"Error type: {type(e).__name__}")
            return None
    
    def get_multiple_secrets(self, secret_mapping: Dict[str, str]) -> Dict[str, Optional[str]]:
        """
        Retrieve multiple secrets at once.
        
        Args:
            secret_mapping: Dict mapping result keys to secret names in Key Vault
            
        Returns:
            Dict with result keys and their secret values (or None if not found)
        """
        results = {}
        for result_key, secret_name in secret_mapping.items():
            results[result_key] = self.get_secret(secret_name)
        return results
    
    def clear_cache(self):
        """Clear the secrets cache"""
        self._secrets_cache.clear()
        logger.debug("Key Vault secrets cache cleared")
    
    def is_available(self) -> bool:
        """Check if Key Vault service is available"""
        return self.client is not None
    
    def get_service_info(self) -> Dict[str, Any]:
        """Get service information for diagnostics"""
        return {
            "vault_url": self.vault_url,
            "is_available": self.is_available(),
            "is_azure_environment": self._is_azure_environment(),
            "cache_enabled": self.cache_enabled,
            "cached_secrets_count": len(self._secrets_cache)
        }


# Global Key Vault service instance
_keyvault_service: Optional[AzureKeyVaultService] = None

def get_keyvault_service() -> AzureKeyVaultService:
    """Get or create global Key Vault service instance"""
    global _keyvault_service
    if _keyvault_service is None:
        _keyvault_service = AzureKeyVaultService()
    return _keyvault_service

def get_secret_directly(secret_name: str) -> Optional[str]:
    """
    Get secret directly from Key Vault without fallback.
    
    Args:
        secret_name: Name of the secret in Key Vault
        
    Returns:
        Secret value or None if not found
    """
    return get_keyvault_service().get_secret(secret_name)