"""
Room Categorization Service - Simple Room Category Assignment
Assigns categories to rooms based on room name using YAML configuration.
"""
import logging
from typing import Optional

logger = logging.getLogger(__name__)

class RoomCategorizerService:
    """Service for categorizing rooms based on room name only"""
    
    def __init__(self):
        """Initialize the room categorizer"""
        self.logger = logging.getLogger(__name__)
        self._parser_cache = {}
        
    def get_room_class(self, room_name: str) -> Optional[str]:
        """
        Get room category based on room name using YAML configuration.
        
        Args:
            room_name: Room name to categorize (e.g., "Junior Suite Ocean View")
            
        Returns:
            Room category (e.g., "suite") or None if not determined
        """
        if not room_name:
            return None
            
        try:
            # Get or create parser (cached for performance)
            parser = self._get_parser()
            
            # Parse room_class directly from room name using YAML patterns
            room_class = parser.parse_room_class(room_name)
            
            if room_class:
                self.logger.debug(f"Categorized '{room_name}' as '{room_class}'")
                return room_class
            else:
                self.logger.debug(f"No category found for '{room_name}'")
                return None
                
        except Exception as e:
            self.logger.error(f"Error categorizing room '{room_name}': {e}")
            return None
    
    def _get_parser(self):
        """Get or create room parser with caching"""
        if 'parser' not in self._parser_cache:
            try:
                from app.data.room_mapper.universal_room_parser import RoomDataParser
                self._parser_cache['parser'] = RoomDataParser(provider='universal')
                self.logger.debug("Room parser initialized successfully")
            except ImportError as e:
                self.logger.error(f"Failed to import RoomDataParser: {e}")
                raise
        
        return self._parser_cache['parser']


# Global instance for compatibility with existing code
_room_mapping_service = None

def get_room_mapping_service() -> RoomCategorizerService:
    """Get global room categorization service instance"""
    global _room_mapping_service
    if _room_mapping_service is None:
        _room_mapping_service = RoomCategorizerService()
    return _room_mapping_service