"""
Room Mapping Service - Hotel Room ID Mapping and Categorization
Maps rooms between different providers and assigns consistent IDs and categories.
"""
import pandas as pd
import hashlib
import logging
import os
import re
from typing import Dict, List, Optional, Any
from pathlib import Path

logger = logging.getLogger(__name__)

class RoomMappingService:
    """Service for mapping hotel rooms between providers with consistent IDs"""
    
    def __init__(self, room_mappings_file: str = None):
        """Initialize with room mappings file"""
        # Initialize logger first
        self.logger = logging.getLogger(__name__)
        
        # Cache for standardized room data to avoid reloading files
        self._standardized_cache = {}
        
        if room_mappings_file is None:
            # Default path to room mappings in app/data directory
            current_dir = Path(__file__).parent.parent
            room_mappings_file = current_dir / "data" / "room_mappings.csv"
        
        self.mappings_df = self._load_mappings(str(room_mappings_file))
        
        if not self.mappings_df.empty:
            self.logger.info(f"Loaded {len(self.mappings_df)} room mappings from {room_mappings_file}")
        else:
            self.logger.warning(f"No room mappings loaded from {room_mappings_file}")
    
    def _extract_base_room_name(self, room_name: str) -> str:
        """Extract room name before parentheses for comparison
        
        Examples:
        - "Deluxe Garden Double Pool Suite (full double bed) (bed type is subject to availability)" 
          -> "Deluxe Garden Double Pool Suite"
        - "Standard Room (king bed)" -> "Standard Room"
        - "Premium Suite" -> "Premium Suite"
        """
        if not room_name:
            return ""
        
        # Find first opening parenthesis and take everything before it
        paren_index = room_name.find('(')
        if paren_index > 0:
            return room_name[:paren_index].strip()
        
        # If no parentheses found, return original name stripped
        return room_name.strip()
    
    def _load_mappings(self, file_path: str) -> pd.DataFrame:
        """Load room mappings from CSV file"""
        try:
            if not os.path.exists(file_path):
                self.logger.error(f"Room mappings file not found: {file_path}")
                return pd.DataFrame()
            
            df = pd.read_csv(file_path)
            
            # Validate required columns - updated to match actual CSV structure
            required_columns = [
                'reference_id', 'ref_hotel_name', 'goglobal_room_name', 
                'ratehawk_room_name', 'matched', 'confidence', 'data_source'
            ]
            
            missing_columns = [col for col in required_columns if col not in df.columns]
            if missing_columns:
                self.logger.error(f"Missing required columns in room mappings: {missing_columns}")
                return pd.DataFrame()
            
            # Add missing category columns with default 'Other' value
            if 'goglobal_category' not in df.columns:
                df['goglobal_category'] = 'Other'
            if 'rate_hawk_category' not in df.columns:
                df['rate_hawk_category'] = 'Other'
            
            # Rename ratehawk_room_name to rate_hawk_room_name for consistency
            if 'ratehawk_room_name' in df.columns and 'rate_hawk_room_name' not in df.columns:
                df['rate_hawk_room_name'] = df['ratehawk_room_name']
                
            return df
            
        except Exception as e:
            self.logger.error(f"Failed to load room mappings: {e}")
            return pd.DataFrame()
    
    def _generate_room_mapping_id(self, hotel_id: int, goglobal_room: str = None, rate_hawk_room: str = None) -> str:
        """Generate consistent room mapping ID for matched rooms"""
        # Create a consistent identifier for matched rooms
        key_parts = [str(hotel_id)]
        
        if goglobal_room and str(goglobal_room) != 'nan':
            key_parts.append(f"gg:{goglobal_room}")
        if rate_hawk_room and str(rate_hawk_room) != 'nan':
            key_parts.append(f"rh:{rate_hawk_room}")
        
        # Create hash for consistent ID
        key_string = "|".join(sorted(key_parts))
        return hashlib.md5(key_string.encode()).hexdigest()[:12]
    
    def _normalize_category(self, category: str) -> str:
        """Normalize category to one of: Standard, Premium, Apartament, Other"""
        if pd.isna(category) or not category or str(category).lower() == 'nan':
            return "Other"
        
        category = str(category).strip()
        
        # Map categories to our 4 standard categories
        if category in ['Standard']:
            return "Standard"
        elif category in ['Premium']:
            return "Premium"
        elif category in ['Apartament']:
            return "Apartament"
        else:
            return "Other"
    
    def find_room_mapping_by_name(self, hotel_id: int, room_name: str, provider: str) -> Optional[Dict]:
        """Find room mapping for a specific room name and provider"""
        try:
            if self.mappings_df.empty:
                return None

            # Filter by hotel_id (reference_id in CSV)
            hotel_mappings = self.mappings_df[
                self.mappings_df['reference_id'] == hotel_id
            ]

            if hotel_mappings.empty:
                return None

            # Extract base room name (before parentheses) for comparison
            base_room_name = self._extract_base_room_name(room_name)
            
            matching_row = None

            if provider.lower() == 'goglobal':
                # Look for exact match using base room name in goglobal_room_name
                matching_row = hotel_mappings[
                    (hotel_mappings['goglobal_room_name'].str.strip() == base_room_name) &
                    (hotel_mappings['goglobal_room_name'].notna())
                ]
            elif provider.lower() == 'rate_hawk':
                # Look for exact match using base room name in rate_hawk_room_name
                matching_row = hotel_mappings[
                    (hotel_mappings['rate_hawk_room_name'].str.strip() == base_room_name) &
                    (hotel_mappings['rate_hawk_room_name'].notna())
                ]
            
            if matching_row is not None and not matching_row.empty:
                row = matching_row.iloc[0]
                
                # Generate consistent room mapping ID
                room_mapping_id = self._generate_room_mapping_id(
                    hotel_id, 
                    row.get('goglobal_room_name'), 
                    row.get('rate_hawk_room_name')
                )
                
                # Determine category - prefer goglobal, fallback to rate_hawk
                category = self._normalize_category(row.get('goglobal_category'))
                if category == "Other":
                    category = self._normalize_category(row.get('rate_hawk_category'))
                
                return {
                    'room_mapping_id': room_mapping_id,
                    'category': category,
                    'confidence': float(row.get('confidence', 0.0)),
                    'matched': bool(row.get('matched', False)),
                    'goglobal_room_name': row.get('goglobal_room_name'),
                    'rate_hawk_room_name': row.get('rate_hawk_room_name'),
                    'data_source': row.get('data_source', 'unknown')
                }
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error finding room mapping: {e}")
            return None
    
    def enhance_offer_with_mapping(self, offer: Dict, hotel_id: int, provider: str) -> Dict:
        """Enhance single offer with room mapping information"""
        enhanced_offer = offer.copy()
        room_name = offer.get('room_name', '')
        
        # Find mapping for this room
        mapping = self.find_room_mapping_by_name(hotel_id, room_name, provider)
        
        if mapping:
            enhanced_offer.update({
                'room_mapping_id': mapping['room_mapping_id'],
                'category': mapping['category'],
                'mapping_confidence': mapping['confidence'],
                'is_matched': mapping['matched'],
                'data_source': mapping['data_source']
            })
        else:
            # For unmapped rooms, create individual ID and assign "Other" category
            enhanced_offer.update({
                'room_mapping_id': f"unmapped_{provider}_{hotel_id}_{hashlib.md5(room_name.encode()).hexdigest()[:8]}",
                'category': "Other",
                'mapping_confidence': 0.0,
                'is_matched': False,
                'data_source': f'{provider}_only'
            })
        
        return enhanced_offer
    
    def enhance_provider_results(self, results: List[Dict], hotel_id: int, provider: str) -> List[Dict]:
        """Enhance all offers from a provider with room mapping information"""
        enhanced_results = []
        
        for offer in results:
            enhanced_offer = self.enhance_offer_with_mapping(offer, hotel_id, provider)
            enhanced_results.append(enhanced_offer)
        
        return enhanced_results
    
    def get_mapping_stats(self, hotel_id: int) -> Dict:
        """Get mapping statistics for a specific hotel"""
        try:
            if self.mappings_df.empty:
                return {"error": "No mappings loaded"}
            
            hotel_mappings = self.mappings_df[
                self.mappings_df['reference_id'] == hotel_id
            ]
            
            if hotel_mappings.empty:
                return {
                    "hotel_id": hotel_id,
                    "total_mappings": 0,
                    "matched_rooms": 0,
                    "goglobal_rooms": 0,
                    "rate_hawk_rooms": 0,
                    "categories": {}
                }
            
            matched_count = len(hotel_mappings[hotel_mappings['matched'] == True])
            goglobal_count = len(hotel_mappings[hotel_mappings['goglobal_room_name'].notna()])
            rate_hawk_count = len(hotel_mappings[hotel_mappings['rate_hawk_room_name'].notna()])
            
            # Count categories
            categories = {}
            for _, row in hotel_mappings.iterrows():
                gg_cat = self._normalize_category(row.get('goglobal_category'))
                rh_cat = self._normalize_category(row.get('rate_hawk_category'))
                
                if gg_cat != "Other":
                    categories[gg_cat] = categories.get(gg_cat, 0) + 1
                elif rh_cat != "Other":
                    categories[rh_cat] = categories.get(rh_cat, 0) + 1
                else:
                    categories["Other"] = categories.get("Other", 0) + 1
            
            return {
                "hotel_id": hotel_id,
                "total_mappings": len(hotel_mappings),
                "matched_rooms": matched_count,
                "goglobal_rooms": goglobal_count,
                "rate_hawk_rooms": rate_hawk_count,
                "categories": categories
            }
            
        except Exception as e:
            self.logger.error(f"Error getting mapping stats: {e}")
            return {"error": str(e)}
    
    def process_search_response(self, response: Dict, hotel_id: int) -> Dict:
        """Process complete search response and add room mapping enhancements"""
        try:
            enhanced_response = response.copy()
            
            # Collect all offers from all providers
            all_offers = []
            provider_offers = {}
            
            # Extract offers from each provider
            results_by_provider = response.get('results_by_provider', {})
            for provider_name, provider_result in results_by_provider.items():
                if provider_result.get('status') == 'success' and provider_result.get('data'):
                    provider_offers[provider_name] = []
                    for offer in provider_result['data']:
                        # Enhance each offer with room mapping
                        enhanced_offer = self.enhance_offer_with_mapping(offer, hotel_id, provider_name)
                        enhanced_offer['provider'] = provider_name
                        all_offers.append(enhanced_offer)
                        provider_offers[provider_name].append(enhanced_offer)
            
            # Group offers by room_mapping_id
            mapped_rooms = {}
            unmapped_rooms = []
            
            for offer in all_offers:
                room_mapping_id = offer.get('room_mapping_id')
                if offer.get('is_matched', False):
                    # Matched room - group by mapping ID
                    if room_mapping_id not in mapped_rooms:
                        mapped_rooms[room_mapping_id] = {
                            'room_mapping_id': room_mapping_id,
                            'category': offer.get('category', 'Other'),
                            'confidence': offer.get('mapping_confidence', 0.0),
                            'goglobal_room_name': None,
                            'rate_hawk_room_name': None,
                            'offers': []
                        }
                        
                        # Get room names from mapping
                        mapping_info = self.find_room_mapping_by_name(
                            hotel_id, offer.get('room_name', ''), offer.get('provider', '')
                        )
                        if mapping_info:
                            mapped_rooms[room_mapping_id]['goglobal_room_name'] = mapping_info.get('goglobal_room_name')
                            mapped_rooms[room_mapping_id]['rate_hawk_room_name'] = mapping_info.get('rate_hawk_room_name')
                    
                    mapped_rooms[room_mapping_id]['offers'].append(offer)
                else:
                    # Unmapped room
                    unmapped_rooms.append(offer)
            
            # Calculate best offers by category (with 15% free cancellation bonus)
            best_offers_by_category = self._calculate_best_offers_by_category(mapped_rooms, unmapped_rooms)
            
            # Create enhanced results structure
            enhanced_response['enhanced_results'] = {
                'mapped_rooms': mapped_rooms,
                'unmapped_rooms': unmapped_rooms,
                'best_offers_by_category': best_offers_by_category,
                'mapping_stats': {
                    'total_offers': len(all_offers),
                    'mapped_offers': sum(len(room['offers']) for room in mapped_rooms.values()),
                    'unmapped_offers': len(unmapped_rooms),
                    'unique_room_mappings': len(mapped_rooms),
                    'categories_found': list(set(room.get('category', 'Other') for room in mapped_rooms.values()))
                }
            }
            
            # Enhance summary with mapping statistics
            if 'summary' in enhanced_response:
                enhanced_response['summary']['room_mapping'] = enhanced_response['enhanced_results']['mapping_stats']
            
            return enhanced_response
            
        except Exception as e:
            self.logger.error(f"Error processing search response: {e}")
            # Return original response if processing fails
            return response
    
    def _calculate_best_offers_by_category(self, mapped_rooms: Dict, unmapped_rooms: List[Dict]) -> Dict:
        """Calculate best offers by category with free cancellation bonus"""
        best_offers = {}
        
        # Process mapped rooms by category
        category_offers = {}
        for room_mapping_id, room_data in mapped_rooms.items():
            category = room_data.get('category', 'Other')
            if category not in category_offers:
                category_offers[category] = []
            
            for offer in room_data['offers']:
                category_offers[category].append({
                    'offer': offer,
                    'room_mapping_id': room_mapping_id,
                    'is_mapped': True
                })
        
        # Add unmapped rooms to "Other" category
        if 'Other' not in category_offers:
            category_offers['Other'] = []
        
        for offer in unmapped_rooms:
            category_offers['Other'].append({
                'offer': offer,
                'room_mapping_id': offer.get('room_mapping_id'),
                'is_mapped': False
            })
        
        # Find best offer in each category
        for category, offers in category_offers.items():
            best_offer_data = self._find_best_offer_in_category(offers)
            if best_offer_data:
                best_offers[category] = best_offer_data
        
        return best_offers
    
    def _find_best_offer_in_category(self, offers: List[Dict]) -> Optional[Dict]:
        """Find best offer in category with 15% free cancellation bonus"""
        if not offers:
            return None
        
        best_offer = None
        best_score = float('inf')
        
        for offer_data in offers:
            offer = offer_data['offer']
            
            try:
                base_price = float(offer.get('total_price', 0))
                if base_price <= 0:
                    continue
                
                # Apply 15% bonus for free cancellation
                adjusted_price = base_price
                if self._has_free_cancellation(offer):
                    adjusted_price = base_price * 0.85  # 15% discount for free cancellation
                
                if adjusted_price < best_score:
                    best_score = adjusted_price
                    best_offer = {
                        'room_mapping_id': offer_data['room_mapping_id'],
                        'category': offer.get('category', 'Other'),
                        'is_mapped': offer_data['is_mapped'],
                        'original_price': base_price,
                        'adjusted_price': adjusted_price,
                        'has_free_cancellation': self._has_free_cancellation(offer),
                        'cancellation_bonus_applied': self._has_free_cancellation(offer),
                        'best_offer': offer
                    }
                    
            except (ValueError, TypeError) as e:
                self.logger.warning(f"Error processing offer price: {e}")
                continue
        
        return best_offer
    
    def _has_free_cancellation(self, offer: Dict) -> bool:
        """Check if offer has free cancellation"""
        # Check various fields that might indicate free cancellation
        free_cancellation_until = offer.get('free_cancellation_until')
        if free_cancellation_until and free_cancellation_until != "":
            return True
        
        # Check cancellation_policy string for "Free cancellation"
        cancellation_policy = offer.get('cancellation_policy', "")
        if isinstance(cancellation_policy, str) and "Free cancellation" in cancellation_policy:
            return True
        
        # Check if cancellation_policy is a dict
        if isinstance(cancellation_policy, dict):
            if cancellation_policy.get('free_cancellation_until'):
                return True
            
            policies = cancellation_policy.get('policies', [])
            if policies and len(policies) > 0:
                first_policy = policies[0]
                if isinstance(first_policy, dict) and first_policy.get('penalty_amount', 0) == 0:
                    return True
        
        return False

    def _load_standardized_rooms(self, provider: str) -> pd.DataFrame:
        """Load standardized room data for a provider (with caching)"""
        # Check cache first
        if provider in self._standardized_cache:
            return self._standardized_cache[provider]
        
        provider_file_map = {
            'rate_hawk': '01_api_rate_hawk_rooms_STANDARDIZED.csv',
            'goglobal': '02_api_goglobal_rooms_STANDARDIZED.csv', 
            'tbo': '03_api_tbo_rooms_STANDARDIZED.csv'
        }
        
        if provider not in provider_file_map:
            self.logger.warning(f"No standardized file mapping for provider: {provider}")
            return pd.DataFrame()
        
        # Path to standardized files
        current_dir = Path(__file__).parent.parent
        file_path = current_dir / "data" / provider_file_map[provider]
        
        try:
            if file_path.exists():
                df = pd.read_csv(file_path)
                self.logger.debug(f"Loaded {len(df)} standardized rooms for {provider}")
                # Cache the result
                self._standardized_cache[provider] = df
                return df
            else:
                self.logger.warning(f"Standardized file not found: {file_path}")
                empty_df = pd.DataFrame()
                self._standardized_cache[provider] = empty_df
                return empty_df
        except Exception as e:
            self.logger.error(f"Error loading standardized rooms for {provider}: {e}")
            empty_df = pd.DataFrame()
            self._standardized_cache[provider] = empty_df
            return empty_df
    
    def get_room_class(self, provider: str, room_name: str) -> Optional[str]:
        """Get room_class from standardized CSV files based on room name"""
        if not room_name or not provider:
            return None
        
        # Load standardized data for provider
        df = self._load_standardized_rooms(provider)
        if df.empty:
            return None
        
        # Clean and normalize room name for matching
        clean_room_name = self._extract_base_room_name(room_name)
        
        # Try exact match first
        exact_match = df[df['room_name'].str.strip().str.lower() == clean_room_name.lower()]
        if not exact_match.empty and 'room_class' in exact_match.columns:
            room_class = exact_match.iloc[0]['room_class']
            self.logger.debug(f"Exact match for {provider} '{clean_room_name}' -> '{room_class}'")
            return room_class
        
        # Try partial match (room name contains search term)
        partial_match = df[df['room_name'].str.contains(clean_room_name, case=False, na=False)]
        if not partial_match.empty and 'room_class' in partial_match.columns:
            room_class = partial_match.iloc[0]['room_class']
            self.logger.debug(f"Partial match for {provider} '{clean_room_name}' -> '{room_class}'")
            return room_class
        
        # Try reverse partial match (search term contains room name) - fixed logic
        for _, row in df.iterrows():
            if pd.notna(row.get('room_name')):
                row_name_lower = row['room_name'].lower()
                if row_name_lower in clean_room_name.lower() and 'room_class' in df.columns:
                    room_class = row['room_class']
                    self.logger.debug(f"Reverse match for {provider} '{clean_room_name}' -> '{room_class}'")
                    return room_class
        
        self.logger.debug(f"No room_class match found for {provider} '{clean_room_name}'")
        return None

# Global instance
_room_mapping_service = None

def get_room_mapping_service() -> RoomMappingService:
    """Get global room mapping service instance"""
    global _room_mapping_service
    if _room_mapping_service is None:
        _room_mapping_service = RoomMappingService()
    return _room_mapping_service
