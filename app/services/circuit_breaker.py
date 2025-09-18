import asyncio
import time
from enum import Enum
from typing import Callable, Any, Optional
import logging

logger = logging.getLogger(__name__)

class CircuitState(Enum):
    CLOSED = "closed"      # Normal operation
    OPEN = "open"          # Circuit is open, rejecting calls
    HALF_OPEN = "half_open"  # Testing if service is back

class CircuitBreaker:
    """Circuit breaker pattern implementation for provider calls"""
    
    def __init__(
        self, 
        failure_threshold: int,
        timeout: float,
        reset_timeout: float,
        name: str = "circuit_breaker"
    ):
        """
        Initialize circuit breaker.
        
        Args:
            failure_threshold: Number of consecutive failures before opening circuit
            timeout: Timeout for individual function calls (in seconds)  
            reset_timeout: Time to wait before attempting half-open (in seconds)
            name: Name for logging and identification
        """
        # Validate constructor parameters
        if not isinstance(failure_threshold, int) or failure_threshold <= 0:
            raise ValueError("failure_threshold must be a positive integer")
        if not isinstance(timeout, (int, float)) or timeout <= 0:
            raise ValueError("timeout must be a positive number")
        if not isinstance(reset_timeout, (int, float)) or reset_timeout <= 0:
            raise ValueError("reset_timeout must be a positive number")
        if not isinstance(name, str) or not name.strip():
            raise ValueError("name must be a non-empty string")
            
        self.failure_threshold = failure_threshold
        self.timeout = timeout
        self.reset_timeout = reset_timeout
        self.name = name.strip()
        
        self._failure_count = 0
        self._last_failure_time: Optional[float] = None
        self._state = CircuitState.CLOSED
    
    @property
    def failure_count(self):
        """Read-only access to failure count"""
        return self._failure_count

    @property
    def last_failure_time(self):
        """Read-only access to last failure time"""
        return self._last_failure_time
        
    @property
    def state(self) -> CircuitState:
        """Get current circuit state"""
        self._update_state()
        return self._state
    
    def _update_state(self):
        """Update circuit state based on time and failures"""
        if self._state == CircuitState.OPEN:
            if self._last_failure_time and (time.time() - self._last_failure_time) >= self.reset_timeout:
                self._state = CircuitState.HALF_OPEN
                logger.info(f"Circuit breaker {self.name} transitioning to HALF_OPEN: Testing if service has recovered after {int(self.reset_timeout)}s timeout")
                
    async def call(self, func: Callable, *args, **kwargs) -> Any:
        """Execute function through circuit breaker"""
        self._update_state()
        
        if self._state == CircuitState.OPEN:
            raise CircuitBreakerOpenError(f"Service protection activated: The {self.name} booking service experienced {self.failure_threshold} consecutive failures and has been temporarily disabled to prevent further issues. Service will automatically retry in {int(self.reset_timeout)} seconds.")
        
        try:
            result = await func(*args, **kwargs)
            self._on_success()
            return result
            
        except Exception as e:
            self._on_failure()
            raise
    
    def _on_success(self):
        """Handle successful call"""
        self._failure_count = 0
        if self._state == CircuitState.HALF_OPEN:
            self._state = CircuitState.CLOSED
            logger.info(f"Circuit breaker {self.name} recovered: Service test successful, returning to normal operation (CLOSED state)")
    
    def _on_failure(self):
        """Handle failed call"""
        self._failure_count += 1
        self._last_failure_time = time.time()
        
        if self._failure_count >= self.failure_threshold:
            self._state = CircuitState.OPEN
            logger.warning(f"Circuit breaker {self.name} ACTIVATED: Service disabled after {self._failure_count}/{self.failure_threshold} consecutive failures. Protection timeout: {int(self.reset_timeout)}s")
    
    def reset(self):
        """Manually reset circuit breaker"""
        self._failure_count = 0
        self._last_failure_time = None
        self._state = CircuitState.CLOSED
        logger.info(f"Circuit breaker {self.name} manually reset: All failure counters cleared, service restored to normal operation")

class CircuitBreakerOpenError(Exception):
    """Raised when circuit breaker is open"""
    pass
