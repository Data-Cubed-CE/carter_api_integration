import asyncio
import concurrent.futures
from typing import Optional, List, Dict
import logging
import pandas as pd

from app.services.azure_sql_connector import create_azure_sql_connector_from_env
from app.config import config

logger = logging.getLogger(__name__)

class HotelMapping:
    """Azure SQL Database hotel mapping service for all providers"""
    
    def __init__(self):
        self.sql_connector = None
        self.provider_configs: Dict[str, Dict] = {}
        self._load_provider_configs()
        self._initialize_sql_connector()
    
    def _load_provider_configs(self):
        """Load provider configurations for hotel mapping"""
        try:
            for provider_name in config.get_active_providers():
                provider_config = config.get_hotel_mapping_config(provider_name)
                if provider_config:
                    self.provider_configs[provider_name] = provider_config
                    logger.info(f"Loaded hotel mapping config for {provider_name}")
                else:
                    logger.warning(f"No hotel mapping config found for {provider_name}")
        except Exception as e:
            logger.error(f"Error loading provider configs: {e}")
    
    def _initialize_sql_connector(self):
        """Initialize Azure SQL connector"""
        try:
            # Check if Azure SQL is configured
            sql_config = config.validate_azure_sql_config()
            if not sql_config['is_configured']:
                logger.error("Azure SQL not configured, cannot initialize hotel mapping")
                self.sql_connector = None
                return
                
            self.sql_connector = create_azure_sql_connector_from_env()
            logger.info("Azure SQL connector initialized for hotel mapping")
        except Exception as e:
            logger.error(f"Failed to initialize SQL connector: {e}")
            self.sql_connector = None
    
    async def _get_hotel_data_async(self, ref_hotel_name: str) -> Optional[pd.DataFrame]:
        """Async method to get hotel data for all providers from database"""
        if not self.sql_connector:
            logger.error("SQL connector not available")
            return None
        
        try:
            # Get all provider hotel_id columns
            columns = ['ref_hotel_name']
            for provider_config in self.provider_configs.values():
                hotel_id_column = provider_config.get("hotel_id_column")
                if hotel_id_column:
                    columns.append(hotel_id_column)
            
            if len(columns) == 1:  # Only ref_hotel_name
                logger.error("No provider hotel_id_columns configured")
                return None
            
            # Build query to get only this specific hotel
            columns_str = ', '.join(f'[{col}]' for col in columns)
            query = f"""
            SELECT TOP 1 {columns_str}
            FROM [dbo].[hotel_mappings]
            WHERE [ref_hotel_name] = ?
            """
            
            results = await self.sql_connector.execute_query(query, (ref_hotel_name,))
            
            if not results:
                logger.warning(f"No hotel data found for '{ref_hotel_name}'")
                return None
            
            # Convert to pandas DataFrame
            hotel_df = pd.DataFrame(results)
            logger.info(f"Loaded hotel data for '{ref_hotel_name}' with {len(hotel_df)} row(s)")
            return hotel_df
            
        except Exception as e:
            logger.error(f"Error loading hotel data for '{ref_hotel_name}': {e}")
            return None
    
    def get_hotel_id(self, ref_hotel_name: str, provider: str) -> Optional[str]:
        """
        Get hotel ID for provider using exact ref_hotel_name match from database.
        
        Args:
            ref_hotel_name: Exact hotel name from database ref_hotel_name column
            provider: Provider name (rate_hawk, goglobal, etc.)
            
        Returns:
            Hotel ID for the provider or None if not found
        """
        try:
            if provider not in self.provider_configs:
                logger.error(f"No configuration found for provider '{provider}'")
                return None
                
            provider_config = self.provider_configs[provider]
            hotel_id_column = provider_config.get("hotel_id_column")
            
            if not hotel_id_column:
                logger.error(f"Missing hotel_id_column configuration for provider '{provider}'")
                return None
            
            # Get hotel data for this specific hotel
            def run_async_in_thread():
                """Run async method in a separate thread to avoid event loop conflicts"""
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(self._get_hotel_data_async(ref_hotel_name))
                finally:
                    loop.close()
            
            # Run in thread pool to avoid "event loop already running" error
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_async_in_thread)
                hotel_df = future.result(timeout=30)
            
            if hotel_df is None or hotel_df.empty:
                logger.warning(f"No data found for hotel '{ref_hotel_name}'")
                return None
            
            # Extract hotel ID for the provider
            if hotel_id_column in hotel_df.columns:
                hotel_id = hotel_df[hotel_id_column].iloc[0]
                hotel_id_str = str(hotel_id) if pd.notna(hotel_id) and hotel_id else None
                if hotel_id_str:
                    logger.info(f"{provider.upper()}: Mapped '{ref_hotel_name}' to '{hotel_id_str}'")
                    return hotel_id_str
                else:
                    logger.warning(f"{provider.upper()}: Hotel '{ref_hotel_name}' found but no ID for provider")
                    return None
            else:
                logger.error(f"Column '{hotel_id_column}' not found in hotel data")
                return None
                
        except Exception as e:
            logger.error(f"Error mapping hotel '{ref_hotel_name}' for {provider}: {e}")
            return None
    
    def get_ref_hotel_name_by_provider_id(self, provider_id: str, provider: str) -> Optional[str]:
        """
        Reverse lookup: Get ref_hotel_name from provider-specific hotel ID.
        
        Args:
            provider_id: Hotel ID from the provider
            provider: Provider name (rate_hawk, goglobal, tbo)
            
        Returns:
            Reference hotel name if found, None otherwise
        """
        try:
            if provider not in self.provider_configs:
                logger.error(f"No configuration found for provider '{provider}'")
                return None
                
            provider_config = self.provider_configs[provider]
            hotel_id_column = provider_config.get("hotel_id_column")
            
            if not hotel_id_column:
                logger.error(f"Missing hotel_id_column configuration for provider '{provider}'")
                return None
            
            # Get data using async method
            def run_async_in_thread():
                """Run async reverse lookup in a separate thread"""
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                try:
                    return loop.run_until_complete(self._get_ref_hotel_name_by_provider_id_async(provider_id, hotel_id_column))
                finally:
                    loop.close()
            
            # Run in thread pool to avoid "event loop already running" error
            with concurrent.futures.ThreadPoolExecutor() as executor:
                future = executor.submit(run_async_in_thread)
                ref_hotel_name = future.result(timeout=30)
            
            if ref_hotel_name:
                logger.debug(f"{provider.upper()}: Reverse mapped ID '{provider_id}' to '{ref_hotel_name}'")
                return ref_hotel_name
            else:
                logger.warning(f"{provider.upper()}: Provider ID '{provider_id}' not found")
                return None
                
        except Exception as e:
            logger.error(f"Error in reverse mapping for {provider} ID '{provider_id}': {e}")
            return None
    
    async def _get_ref_hotel_name_by_provider_id_async(self, provider_id: str, hotel_id_column: str) -> Optional[str]:
        """Async method for reverse lookup by provider ID"""
        if not self.sql_connector:
            logger.error("SQL connector not available")
            return None
        
        try:
            # Build query to find ref_hotel_name by provider ID
            query = f"""
            SELECT TOP 1 [ref_hotel_name]
            FROM [dbo].[hotel_mappings]
            WHERE [{hotel_id_column}] = ?
            """
            
            results = await self.sql_connector.execute_query(query, (provider_id,))
            
            if not results:
                return None
            
            ref_hotel_name = results[0].get('ref_hotel_name')
            return ref_hotel_name if ref_hotel_name else None
            
        except Exception as e:
            logger.error(f"Error in reverse lookup query for ID '{provider_id}': {e}")
            return None

# Global instance
hotel_mapping_service = HotelMapping()

def get_hotel_mapping_service() -> HotelMapping:
    """Get global hotel mapping service instance"""
    return hotel_mapping_service
