import json
import logging
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional
from azure.storage.blob import BlobServiceClient
from azure.core.exceptions import AzureError
from app.config import Config

logger = logging.getLogger(__name__)

class BlobStorageService:
    def __init__(self):
        """Initialize Azure Blob Storage client"""
        self.connection_string = Config.get_azure_storage_connection_string()
        self.container_name = Config.get_blob_container_name()
        self.blob_service_client = None
        
        if self.connection_string:
            try:
                self.blob_service_client = BlobServiceClient.from_connection_string(
                    self.connection_string
                )
                # Ensure container exists
                self._ensure_container_exists()
                logger.info("Azure Blob Storage client initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize Blob Storage client: {e}")
                self.blob_service_client = None
        else:
            logger.warning("Azure Storage connection string not configured - blob storage disabled")

    def _ensure_container_exists(self):
        """Create container if it doesn't exist"""
        try:
            container_client = self.blob_service_client.get_container_client(
                self.container_name
            )
            if not container_client.exists():
                container_client.create_container()
                logger.info(f"Created container: {self.container_name}")
        except Exception as e:
            logger.error(f"Error ensuring container exists: {e}")

    async def save_response_async(self, request_id: str, response_data: Dict[str, Any], 
                                 user: Optional[str] = None):
        """
        Save response data to Azure Blob Storage asynchronously (non-blocking)
        
        Args:
            request_id: Unique request identifier
            response_data: Complete response data to save
            user: Optional user identifier
        """
        if not self.blob_service_client:
            logger.debug("Blob Storage not available - skipping save")
            return

        try:
            # Generate timestamp for file naming
            now = datetime.utcnow()
            timestamp_str = now.strftime("%Y%m%d_%H%M%S")
            
            # Generate blob name with timestamp: api-response/{timestamp}_{request_id}_user_{user}.json
            if user:
                blob_name = f"api-response/{timestamp_str}_{request_id}_user_{user}.json"
            else:
                blob_name = f"api-response/{timestamp_str}_{request_id}.json"

            # Prepare minimal data for storage (just what we need)
            storage_data = {
                "request_id": request_id,
                "timestamp": now.isoformat(),
                "user": user,
                "response": response_data
            }

            # Convert to JSON
            json_data = json.dumps(storage_data, indent=2, ensure_ascii=False, default=str)
            
            # Calculate file size
            file_size_bytes = len(json_data.encode('utf-8'))
            file_size_kb = file_size_bytes / 1024
            
            # Upload to blob storage in background
            await asyncio.get_event_loop().run_in_executor(
                None, 
                self._upload_blob, 
                blob_name, 
                json_data
            )

            return {"blob_name": blob_name, "size_bytes": file_size_bytes, "size_kb": file_size_kb}
            
        except Exception as e:
            # Log error but don't raise - we don't want to affect API response
            logger.error(f"Failed to save response {request_id} to blob storage: {e}")

    def _upload_blob(self, blob_name: str, json_data: str):
        """Synchronous blob upload (called from executor)"""
        blob_client = self.blob_service_client.get_blob_client(
            container=self.container_name,
            blob=blob_name
        )
        
        blob_client.upload_blob(
            json_data,
            overwrite=True,
            content_type="application/json"
        )

# Global instance
blob_storage_service = BlobStorageService()
