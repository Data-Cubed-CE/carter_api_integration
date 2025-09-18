"""
Session Manager for HTTP connections
Centralized session management with connection pooling and optimization
"""

import aiohttp
import asyncio
import logging
from typing import Dict, Optional
from app.config import config

logger = logging.getLogger(__name__)

class SessionManager:
    """
    Centralized HTTP session manager with optimizations:
    - Connection pooling per provider
    - DNS caching
    - Keep-alive connections
    - Auth-specific session reuse
    """
    
    def __init__(self):
        """Initialize the session manager"""
        self._sessions: Dict[str, aiohttp.ClientSession] = {}
        self._global_session: Optional[aiohttp.ClientSession] = None
        self._lock = asyncio.Lock()  # Prevent race conditions
        
    async def get_session(self, provider_name: str) -> aiohttp.ClientSession:
        """
        Get optimized HTTP session for provider.
        
        Args:
            provider_name: Name of the provider (tbo, rate_hawk, goglobal)
            
        Returns:
            aiohttp.ClientSession: Optimized session for the provider
        """
        async with self._lock:  # Thread-safe session creation
            if provider_name not in self._sessions:
                await self._create_provider_session(provider_name)
                
            session = self._sessions[provider_name]
            
            # Check if session is still valid
            if session.closed:
                logger.warning(f"Session for {provider_name} was closed, recreating")
                await self._create_provider_session(provider_name)
                session = self._sessions[provider_name]
                
            return session
    
    async def _create_provider_session(self, provider_name: str):
        """
        Create optimized session for specific provider.
        
        Args:
            provider_name: Provider name to create session for
        """
        try:
            provider_config = config.get_provider_config(provider_name)
            
            if not provider_config:
                raise ValueError(f"No configuration found for provider: {provider_name}")
            
            # Create optimized connector with connection pooling
            connector = aiohttp.TCPConnector(
                limit=50,              # Total connection pool size
                limit_per_host=20,     # Max connections per host
                ttl_dns_cache=300,     # DNS cache TTL (5 minutes)
                use_dns_cache=True,    # Enable DNS caching
                keepalive_timeout=30,  # Keep connections alive for 30s
                enable_cleanup_closed=True  # Clean up closed connections
            )
            
            # Configure timeout
            timeout = aiohttp.ClientTimeout(
                total=provider_config.get('timeout', 30),
                connect=30  # Connection timeout
            )
            
            # Check authentication type
            auth_type = provider_config.get('auth_type')
            
            if auth_type == 'basic':
                # Provider needs Basic Auth - create dedicated session
                auth = aiohttp.BasicAuth(
                    provider_config['username'], 
                    provider_config['password']
                )
                
                session = aiohttp.ClientSession(
                    connector=connector,
                    auth=auth,
                    timeout=timeout
                )
                
                logger.debug(f"Created Basic Auth session for {provider_name}")
                
            else:
                # No auth or API key auth - can reuse global session
                if not self._global_session or self._global_session.closed:
                    self._global_session = aiohttp.ClientSession(
                        connector=connector,
                        timeout=timeout
                    )
                    logger.debug("Created global session for non-auth providers")
                
                session = self._global_session
                logger.debug(f"Reusing global session for {provider_name}")
            
            self._sessions[provider_name] = session
            
        except Exception as e:
            logger.error(f"Failed to create session for {provider_name}: {e}")
            # Fallback to basic session
            self._sessions[provider_name] = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=30)
            )
    
    async def close_all_sessions(self):
        """
        Close all managed sessions.
        Should be called on application shutdown.
        """
        logger.info("Closing all HTTP sessions")
        
        # Close provider-specific sessions
        for provider_name, session in self._sessions.items():
            if session and not session.closed:
                await session.close()
                logger.debug(f"Closed session for {provider_name}")
        
        # Close global session
        if self._global_session and not self._global_session.closed:
            await self._global_session.close()
            logger.debug("Closed global session")
        
        # Clear references
        self._sessions.clear()
        self._global_session = None
    
    async def close_provider_session(self, provider_name: str):
        """
        Close session for specific provider.
        
        Args:
            provider_name: Provider to close session for
        """
        if provider_name in self._sessions:
            session = self._sessions[provider_name]
            if session and not session.closed:
                await session.close()
                logger.debug(f"Closed session for {provider_name}")
            del self._sessions[provider_name]
    
    def get_session_stats(self) -> Dict[str, Dict]:
        """
        Get statistics about current sessions.
        Useful for monitoring and debugging.
        
        Returns:
            Dict with session statistics
        """
        stats = {}
        
        for provider_name, session in self._sessions.items():
            stats[provider_name] = {
                'closed': session.closed,
                'connector_limit': getattr(session.connector, 'limit', 'unknown'),
                'connector_limit_per_host': getattr(session.connector, 'limit_per_host', 'unknown')
            }
        
        if self._global_session:
            stats['global_session'] = {
                'closed': self._global_session.closed,
                'connector_limit': getattr(self._global_session.connector, 'limit', 'unknown'),
                'connector_limit_per_host': getattr(self._global_session.connector, 'limit_per_host', 'unknown')
            }
        
        return stats

# Global instance
session_manager = SessionManager()