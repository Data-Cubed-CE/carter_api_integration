#!/usr/bin/env python3
"""
Universal Room Data Parser
Parses room data from providers and creates standardized columns
Each function handles one standardized column
"""

import pandas as pd
import yaml
import re
from pathlib import Path
from typing import Optional, Union

class RoomDataParser:
    """Universal parser for room data standardization"""
    
    def __init__(self, provider: str = 'universal'):
        """Initialize parser with configuration"""
        self.provider = provider
        self.config = self._load_config()
        
    def _load_config(self) -> dict:
        """Load room mappings configuration"""
        config_path = Path('app/config/room_mappings_config.yaml')
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    
    # ===== COLUMN PARSING FUNCTIONS =====
    
    def parse_main_name(self, room_name: str) -> str:
        """
        Parse main_name: Clean room name by removing promotional content
        
        Args:
            room_name: Original room name from provider
            
        Returns:
            Cleaned main_name
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_main_name_{self.provider}'):
            return getattr(self, f'_parse_main_name_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            return ''
        
        main_name = str(room_name)
        
        # Apply removal patterns
        cleaning_patterns = self.config.get('room_name_cleaning', {}).get('remove_patterns', [])
        
        for pattern_config in cleaning_patterns:
            pattern = pattern_config['pattern']
            main_name = re.sub(pattern, '', main_name, flags=re.IGNORECASE)
        
        # Apply final cleanup
        final_cleanup = self.config.get('room_name_cleaning', {}).get('final_cleanup', [])
        
        for pattern_config in final_cleanup:
            pattern = pattern_config['pattern']
            replacement = pattern_config.get('replacement', '')
            main_name = re.sub(pattern, replacement, main_name)
        
        return main_name.strip()
    
    def parse_bedrooms_count(self, room_name: str) -> int:
        """
        Parse bedrooms_count: Extract number of bedrooms from room name
        
        Args:
            room_name: Room name to parse
            
        Returns:
            Number of bedrooms (None if not found)
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_bedrooms_count_{self.provider}'):
            return getattr(self, f'_parse_bedrooms_count_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            return None
            
        room_name_lower = str(room_name).lower()
        
        # Get patterns from config
        bedroom_patterns = self.config.get('parsing_patterns', {}).get('bedrooms_count', {})
        patterns = bedroom_patterns.get('patterns', [])
        default_value = bedroom_patterns.get('default', 0)
        
        # Try each pattern
        for pattern_config in patterns:
            pattern = pattern_config['pattern']
            group = pattern_config.get('group', 1)
            fixed_value = pattern_config.get('value', None)
            case_insensitive = pattern_config.get('case_insensitive', True)
            
            flags = re.IGNORECASE if case_insensitive else 0
            match = re.search(pattern, room_name_lower, flags=flags)
            
            if match:
                # If pattern has a fixed value, return it
                if fixed_value is not None:
                    return int(fixed_value)
                try:
                    return int(match.group(group))
                except (ValueError, IndexError):
                    continue
        
        return default_value
    
    def parse_room_capacity(self, room_name: str) -> int:
        """
        Parse room_capacity: Extract maximum guest capacity from room name
        
        Args:
            room_name: Room name to parse
            
        Returns:
            Maximum guest capacity (None if not found)
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_room_capacity_{self.provider}'):
            return getattr(self, f'_parse_room_capacity_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            return None
            
        room_name_lower = str(room_name).lower()
        
        # Get patterns from config
        capacity_config = self.config.get('parsing_patterns', {}).get('room_capacity', {})
        patterns = capacity_config.get('patterns', [])
        keywords = capacity_config.get('keywords', {})
        default_value = capacity_config.get('default', 2)
        
        # Try regex patterns first (highest priority)
        for pattern_config in patterns:
            pattern = pattern_config['pattern']
            group = pattern_config.get('group', 1)
            fixed_value = pattern_config.get('value', None)
            case_insensitive = pattern_config.get('case_insensitive', True)
            
            flags = re.IGNORECASE if case_insensitive else 0
            match = re.search(pattern, room_name_lower, flags=flags)
            
            if match:
                if fixed_value is not None:
                    return int(fixed_value)
                try:
                    return int(match.group(group))
                except (ValueError, IndexError):
                    continue
        
        # Try keywords
        for keyword, capacity in keywords.items():
            if keyword.lower() in room_name_lower:
                return int(capacity)
        
        return default_value

    def parse_room_keywords(self, room_name: str) -> str:
        """
        Parse room_keywords: Extract unique keywords from room name that are not captured by other attributes
        
        Args:
            room_name: Room name to parse
            
        Returns:
            Comma-separated string of unique keywords (default None if not found)
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_room_keywords_{self.provider}'):
            return getattr(self, f'_parse_room_keywords_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            return None
            
        room_name_str = str(room_name).strip()
        
        # Get config
        keywords_config = self.config.get('parsing_patterns', {}).get('room_keywords', {})
        exclude_from_sections = keywords_config.get('exclude_from_sections', [])
        exclude_words = keywords_config.get('exclude_words', [])
        force_include = keywords_config.get('force_include', [])
        min_word_length = keywords_config.get('min_word_length', 3)
        max_keywords = keywords_config.get('max_keywords', 5)
        case_insensitive = keywords_config.get('case_insensitive', True)
        default_value = keywords_config.get('default', None)
        
        # Build exclude_words list from config sections
        all_exclude_words = list(exclude_words)  # Start with manual exclude_words
        
        # Add words from specified config sections
        for section_path in exclude_from_sections:
            section_words = self._get_words_from_config_section(section_path)
            all_exclude_words.extend(section_words)
        
        # Convert exclude and force_include lists to lowercase if case insensitive
        if case_insensitive:
            exclude_words_lower = [word.lower() for word in all_exclude_words]
            force_include_lower = [word.lower() for word in force_include]
        else:
            exclude_words_lower = all_exclude_words
            force_include_lower = force_include
        
        # Clean room name and split into words
        # Remove common punctuation and split
        import re
        
        # Remove parentheses, brackets, commas, and other common separators
        cleaned_name = re.sub(r'[(),\[\]{}|*-]', ' ', room_name_str)
        
        # Split into words and clean
        words = cleaned_name.split()
        
        # Extract keywords
        keywords = []
        
        for word in words:
            # Clean word (remove punctuation at start/end)
            clean_word = re.sub(r'^[^\w]+|[^\w]+$', '', word)
            
            if not clean_word:
                continue
                
            # Convert to lowercase for comparison if case insensitive
            word_for_comparison = clean_word.lower() if case_insensitive else clean_word
            
            # Check minimum length
            if len(clean_word) < min_word_length:
                continue
            
            # Check if word should be force included
            if word_for_comparison in force_include_lower:
                if clean_word not in keywords:  # Avoid duplicates
                    keywords.append(clean_word)
                continue
            
            # Check if word should be excluded
            if word_for_comparison in exclude_words_lower:
                continue
            
            # Check if it's a number (usually not interesting as keyword)
            if clean_word.isdigit():
                continue
            
            # Add keyword if not already present
            if clean_word not in keywords:
                keywords.append(clean_word)
            
            # Stop if we've reached max keywords
            if len(keywords) >= max_keywords:
                break
        
        # Return keywords as comma-separated string or default
        if keywords:
            return ', '.join(keywords)
        else:
            return default_value

    def _get_words_from_config_section(self, section_path: str) -> list:
        """
        Extract words from a specific config section path
        
        Args:
            section_path: Dot-separated path to config section (e.g., 'room_class.keywords')
            
        Returns:
            List of words from that section
        """
        words = []
        
        try:
            # Split path into parts
            path_parts = section_path.split('.')
            
            # Navigate through config structure
            current_section = self.config
            
            for part in path_parts:
                if isinstance(current_section, dict) and part in current_section:
                    current_section = current_section[part]
                else:
                    # Path not found, return empty list
                    return words
            
            # Extract words based on section structure
            if isinstance(current_section, dict):
                # If it's a dict, look for keywords in values
                for key, value in current_section.items():
                    if isinstance(value, list):
                        # Add all items from list
                        words.extend([str(item) for item in value])
                    elif isinstance(value, str):
                        # Add single string, split by spaces and commas
                        import re
                        split_words = re.split(r'[,\s]+', value)
                        words.extend([word.strip() for word in split_words if word.strip()])
                    elif key == 'keywords' and isinstance(value, dict):
                        # Handle nested keywords structure
                        for subkey, subvalue in value.items():
                            if isinstance(subvalue, str):
                                words.append(subkey)  # Use the key as keyword
                                # Also add words from the value
                                split_words = re.split(r'[,\s]+', subvalue)
                                words.extend([word.strip() for word in split_words if word.strip()])
                            
            elif isinstance(current_section, list):
                # If it's a list, add all items
                words.extend([str(item) for item in current_section])
            
            # Clean up words (remove multi-word phrases, keep only single words)
            clean_words = []
            for word in words:
                word = str(word).strip()
                if word and ' ' not in word:  # Only single words
                    clean_words.append(word)
                else:
                    # Split multi-word phrases
                    import re
                    split_words = re.split(r'[,\s]+', word)
                    clean_words.extend([w.strip() for w in split_words if w.strip() and ' ' not in w.strip()])
            
            return clean_words
            
        except Exception as e:
            # If any error occurs, return empty list
            print(f"Warning: Could not extract words from config section '{section_path}': {e}")
            return words

    def parse_room_area(self, room_name: str) -> tuple:
        """
        Parse room_area: Extract room area from room name (sq ft, m2, etc.)
        
        Args:
            room_name: Room name to parse
            
        Returns:
            Tuple (area_m2, area_sqft) - both values provided
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_room_area_{self.provider}'):
            return getattr(self, f'_parse_room_area_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            return (None, None)
            
        room_name_str = str(room_name)
        
        # Get patterns from config
        area_config = self.config.get('parsing_patterns', {}).get('room_area', {})
        patterns = area_config.get('patterns', [])
        default_value = area_config.get('default', None)
        case_insensitive = area_config.get('case_insensitive', True)
        
        # Try each pattern
        for pattern_config in patterns:
            pattern = pattern_config['pattern']
            group = pattern_config.get('group', 1)
            unit = pattern_config.get('unit', 'm2')
            
            flags = re.IGNORECASE if case_insensitive else 0
            match = re.search(pattern, room_name_str, flags=flags)
            
            if match:
                try:
                    area_value = float(match.group(group))
                    
                    # Return both m2 and sqft
                    if unit == 'sq_ft':
                        # Original is square feet
                        area_sqft = round(area_value, 1)
                        area_m2 = round(area_value * 0.092903, 1)  # Convert to m2
                    else:  # unit == 'm2'
                        # Original is square meters
                        area_m2 = round(area_value, 1)
                        area_sqft = round(area_value * 10.764, 1)  # Convert to sqft
                    
                    return (area_m2, area_sqft)
                except (ValueError, IndexError):
                    continue
        
        return (default_value, default_value)
    
    def parse_room_class(self, room_name: str) -> str:
        """
        Parse room_class: Extract room class from room name (room, suite, villa, etc.)
        
        Args:
            room_name: Room name to parse
            
        Returns:
            Room class (default 'room' if not found)
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_room_class_{self.provider}'):
            return getattr(self, f'_parse_room_class_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            return None
            
        room_name_lower = str(room_name).lower()
        
        # Get config
        class_config = self.config.get('parsing_patterns', {}).get('room_class', {})
        priority_patterns = class_config.get('priority_patterns', [])
        keywords = class_config.get('keywords', {})
        default_value = class_config.get('default', None)
        case_insensitive = class_config.get('case_insensitive', True)
        
        # First check priority patterns (most specific, context-aware)
        for pattern_config in priority_patterns:
            pattern = pattern_config['pattern']
            value = pattern_config['value']
            
            flags = re.IGNORECASE if case_insensitive else 0
            if re.search(pattern, room_name_lower, flags=flags):
                return value
        
        # Then search for keywords in order of priority (longest first)
        sorted_keywords = sorted(keywords.keys(), key=len, reverse=True)
        
        for keyword in sorted_keywords:
            if case_insensitive:
                if keyword.lower() in room_name_lower:
                    return keywords[keyword]
            else:
                if keyword in room_name:
                    return keywords[keyword]
        
        return default_value

    def parse_room_quality(self, room_name: str) -> str:
        """
        Parse room_quality: Extract room quality from room name (grand, comfort, classic, etc.)
        
        Args:
            room_name: Room name to parse
            
        Returns:
            Room quality (default None if not found)
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_room_quality_{self.provider}'):
            return getattr(self, f'_parse_room_quality_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            quality_config = self.config.get('parsing_patterns', {}).get('room_quality', {})
            return quality_config.get('default', None)
            
        room_name_lower = str(room_name).lower()
        
        # Get keywords from config
        quality_config = self.config.get('parsing_patterns', {}).get('room_quality', {})
        keywords = quality_config.get('keywords', {})
        default_value = quality_config.get('default', None)
        case_insensitive = quality_config.get('case_insensitive', True)
        
        # Search for keywords in order of priority (longest first)
        sorted_keywords = sorted(keywords.keys(), key=len, reverse=True)
        
        for keyword in sorted_keywords:
            if case_insensitive:
                if keyword.lower() in room_name_lower:
                    return keywords[keyword]
            else:
                if keyword in room_name:
                    return keywords[keyword]
        
        return default_value

    def parse_room_quality_category(self, room_name: str) -> str:
        """
        Parse room_quality_category: Extract room quality category from room name (entry, enhanced, high, etc.)
        
        Args:
            room_name: Room name to parse
            
        Returns:
            Room quality category (default None if not found)
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_room_quality_category_{self.provider}'):
            return getattr(self, f'_parse_room_quality_category_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            return None
            
        room_name_lower = str(room_name).lower()
        
        # Get keywords from config
        quality_config = self.config.get('parsing_patterns', {}).get('room_quality_category', {})
        keywords = quality_config.get('keywords', {})
        default_value = quality_config.get('default', None)
        case_insensitive = quality_config.get('case_insensitive', True)
        
        # Search for keywords in order of priority (longest first)
        sorted_keywords = sorted(keywords.keys(), key=len, reverse=True)
        
        for keyword in sorted_keywords:
            if case_insensitive:
                if keyword.lower() in room_name_lower:
                    return keywords[keyword]
            else:
                if keyword in room_name:
                    return keywords[keyword]
        
        return default_value

    def parse_bedding_config(self, room_name: str) -> str:
        """
        Parse bedding_config: Extract bedding configuration from room name (single, double, twin, etc.)
        
        Args:
            room_name: Room name to parse
            
        Returns:
            Bedding configuration (default 'undefined' if not found)
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_bedding_config_{self.provider}'):
            return getattr(self, f'_parse_bedding_config_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            bedding_config = self.config.get('parsing_patterns', {}).get('bedding_config', {})
            return bedding_config.get('default', None)
            
        room_name_lower = str(room_name).lower()
        
        # Get keywords from config
        bedding_config = self.config.get('parsing_patterns', {}).get('bedding_config', {})
        keywords = bedding_config.get('keywords', {})
        default_value = bedding_config.get('default', None)
        case_insensitive = bedding_config.get('case_insensitive', True)
        
        # Search for keywords in order of priority (longest first)
        sorted_keywords = sorted(keywords.keys(), key=len, reverse=True)
        
        for keyword in sorted_keywords:
            if case_insensitive:
                if keyword.lower() in room_name_lower:
                    return keywords[keyword]
            else:
                if keyword in room_name:
                    return keywords[keyword]
        
        return default_value

    def parse_bedding_type(self, room_name: str) -> str:
        """
        Parse bedding_type: Extract original bedding description from room name
        
        Args:
            room_name: Original room name from provider
            
        Returns:
            Original bedding description found in room name
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_bedding_type_{self.provider}'):
            return getattr(self, f'_parse_bedding_type_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            return ''
        
        room_name_lower = str(room_name).lower()
        
        # Get bedding_config keywords to find original descriptions
        bedding_config = self.config['parsing_patterns']['bedding_config']
        keywords = bedding_config.get('keywords', {})
        case_insensitive = bedding_config.get('case_insensitive', True)
        
        # Search for keywords and return the standardized keyword
        sorted_keywords = sorted(keywords.keys(), key=len, reverse=True)
        
        for keyword in sorted_keywords:
            if case_insensitive:
                keyword_lower = keyword.lower()
                if keyword_lower in room_name_lower:
                    # Return the standardized keyword (not the original text from room_name)
                    return keyword
            else:
                if keyword in room_name:
                    return keyword
        
        return None  # No bedding type found

    def parse_room_view(self, room_name: str) -> str:
        """
        Parse room_view: Extract view type from room name (sea_view, city_view, etc.)
        
        Args:
            room_name: Room name to parse
            
        Returns:
            Room view type (default 'no_view' if not found)
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_room_view_{self.provider}'):
            return getattr(self, f'_parse_room_view_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            view_config = self.config.get('parsing_patterns', {}).get('room_view', {})
            return view_config.get('default', None)
            
        room_name_lower = str(room_name).lower()
        
        # Get keywords from config
        view_config = self.config.get('parsing_patterns', {}).get('room_view', {})
        keywords = view_config.get('keywords', {})
        default_value = view_config.get('default', None)
        case_insensitive = view_config.get('case_insensitive', True)
        
        # Search for keywords in order of priority (longest first)
        sorted_keywords = sorted(keywords.keys(), key=len, reverse=True)
        
        for keyword in sorted_keywords:
            if case_insensitive:
                if keyword.lower() in room_name_lower:
                    return keywords[keyword]
            else:
                if keyword in room_name:
                    return keywords[keyword]
        
        return default_value

    def parse_balcony(self, room_name: str) -> int:
        """
        Parse balcony: Check if room has balcony/terrace/patio
        
        Args:
            room_name: Room name to parse
            
        Returns:
            1 if has balcony, None if not found
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_balcony_{self.provider}'):
            return getattr(self, f'_parse_balcony_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            return None
            
        room_name_lower = str(room_name).lower()
        
        # Get keywords from config
        boolean_config = self.config.get('parsing_patterns', {}).get('boolean_features', {})
        balcony_config = boolean_config.get('balcony', {})
        keywords = balcony_config.get('keywords', [])
        default_value = balcony_config.get('default', 0)
        
        # Search for any balcony-related keywords
        for keyword in keywords:
            if keyword.lower() in room_name_lower:
                return 1
        
        return default_value

    def parse_family_room(self, room_name: str) -> int:
        """
        Parse family_room: Check if room is family-friendly
        
        Args:
            room_name: Room name to parse
            
        Returns:
            1 if family room, None if not found
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_family_room_{self.provider}'):
            return getattr(self, f'_parse_family_room_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            return None
            
        room_name_lower = str(room_name).lower()
        
        # Get keywords from config
        boolean_config = self.config.get('parsing_patterns', {}).get('boolean_features', {})
        family_config = boolean_config.get('family_room', {})
        keywords = family_config.get('keywords', [])
        default_value = family_config.get('default', 0)
        
        # Search for any family-related keywords
        for keyword in keywords:
            if keyword.lower() in room_name_lower:
                return 1
        
        return default_value

    def parse_club_room(self, room_name: str) -> int:
        """
        Parse club_room: Check if room is club/executive level
        
        Args:
            room_name: Room name to parse
            
        Returns:
            1 if club room, None if not found
        """
        # Check for provider-specific logic first
        if hasattr(self, f'_parse_club_room_{self.provider}'):
            return getattr(self, f'_parse_club_room_{self.provider}')(room_name)
        
        # Default universal logic
        if pd.isna(room_name) or room_name == '':
            return None
            
        room_name_lower = str(room_name).lower()
        
        # Get keywords from config
        boolean_config = self.config.get('parsing_patterns', {}).get('boolean_features', {})
        club_config = boolean_config.get('club_room', {})
        keywords = club_config.get('keywords', [])
        default_value = club_config.get('default', 0)
        
        # Search for any club-related keywords
        for keyword in keywords:
            if keyword.lower() in room_name_lower:
                return 1
        
        return default_value

    # ===== PROVIDER-SPECIFIC METHODS =====
    
    def _parse_main_name_tbo(self, room_name: str) -> str:
        """TBO-specific main_name parsing - take only text before first comma"""
        if pd.isna(room_name) or room_name == '':
            return ''
        
        main_name = str(room_name).strip()
        
        # TBO SPECIFIC LOGIC: Take only text before first comma
        # Example: "Superior Room, 1 King Bed (Palace, with Waterpark Access),NonSmoking" 
        #          -> "Superior Room"
        if ',' in main_name:
            main_name = main_name.split(',')[0].strip()
        
        # Apply additional TBO-specific cleaning patterns if needed
        tbo_patterns = [
            r'\s*-\s*TBO Special Deal.*',
            r'\s*\[TBO Offer\].*',
        ]
        
        for pattern in tbo_patterns:
            main_name = re.sub(pattern, '', main_name, flags=re.IGNORECASE)
        
        # Apply final cleanup (remove extra spaces, normalize)
        main_name = re.sub(r'\s+', ' ', main_name).strip()
        
        return main_name
    
    def _parse_bedrooms_count_ratehawk(self, room_name: str) -> int:
        """RateHawk-specific bedrooms parsing (example)"""
        if pd.isna(room_name) or room_name == '':
            return 0
        
        # RateHawk może mieć inne konwencje nazewnictwa
        room_name_lower = str(room_name).lower()
        
        # Przykład: RateHawk używa "br" zamiast "bedroom"
        ratehawk_patterns = [
            (r'(\d+)\s*br\b', 1),
            (r'(\d+)\s*-\s*br\b', 1),
        ]
        
        for pattern, group in ratehawk_patterns:
            match = re.search(pattern, room_name_lower)
            if match:
                try:
                    return int(match.group(group))
                except ValueError:
                    continue
        
        # Fallback to universal logic
        return self._parse_bedrooms_count_universal(room_name)
    
    def _apply_universal_cleaning(self, room_name: str) -> str:
        """Apply universal cleaning patterns"""
        cleaning_patterns = self.config.get('room_name_cleaning', {}).get('remove_patterns', [])
        
        for pattern_config in cleaning_patterns:
            pattern = pattern_config['pattern']
            room_name = re.sub(pattern, '', room_name, flags=re.IGNORECASE)
        
        # Apply final cleanup
        final_cleanup = self.config.get('room_name_cleaning', {}).get('final_cleanup', [])
        
        for pattern_config in final_cleanup:
            pattern = pattern_config['pattern']
            replacement = pattern_config.get('replacement', '')
            room_name = re.sub(pattern, replacement, room_name)
        
        return room_name.strip()
    
    def _parse_bedrooms_count_universal(self, room_name: str) -> int:
        """Universal bedrooms parsing logic"""
        room_name_lower = str(room_name).lower()
        
        # Get patterns from config
        bedroom_patterns = self.config.get('parsing_patterns', {}).get('bedrooms_count', {})
        patterns = bedroom_patterns.get('patterns', [])
        default_value = bedroom_patterns.get('default', 0)
        
        # Try each pattern
        for pattern_config in patterns:
            pattern = pattern_config['pattern']
            group = pattern_config.get('group', 1)
            fixed_value = pattern_config.get('value', None)
            case_insensitive = pattern_config.get('case_insensitive', True)
            
            flags = re.IGNORECASE if case_insensitive else 0
            match = re.search(pattern, room_name_lower, flags=flags)
            
            if match:
                # If pattern has a fixed value, return it
                if fixed_value is not None:
                    return int(fixed_value)
                try:
                    return int(match.group(group))
                except (ValueError, IndexError):
                    continue
        
        return default_value
    
    def process_api(self, input_csv_path: str, output_csv_path: str, provider: str) -> bool:
        """
        Process API data by applying all parsing functions to create standardized columns
        
        Args:
            input_csv_path: Path to input CSV file
            output_csv_path: Path to output CSV file
            provider: API provider name
            
        Returns:
            True if successful, False otherwise
        """
        try:
            # Set provider for this processing session
            self.provider = provider
            
            # Load data
            df = pd.read_csv(input_csv_path)
            
            # Handle main_name based on provider
            if provider == 'ratehawk':
                # RateHawk: Use existing main_name from source file (already clean)
                # main_name column already exists and is good quality
                pass
            else:
                # Other APIs: Generate main_name by processing room_name
                df['main_name'] = df['room_name'].apply(self.parse_main_name)
            
            # Apply all other parsing functions to create standardized columns
            df['bedrooms_count'] = df['room_name'].apply(self.parse_bedrooms_count)
            df['room_capacity'] = df['room_name'].apply(self.parse_room_capacity)
            
            # Parse room area (returns tuple: area_m2, area_sqft)
            area_results = df['room_name'].apply(self.parse_room_area)
            df['room_area_m2'] = [result[0] for result in area_results]
            df['room_area_sqft'] = [result[1] for result in area_results]
            
            df['room_quality'] = df['room_name'].apply(self.parse_room_quality)
            df['bedding_type'] = df['room_name'].apply(self.parse_bedding_type)
            df['bedding_config'] = df['room_name'].apply(self.parse_bedding_config)
            df['room_class'] = df['room_name'].apply(self.parse_room_class)
            df['balcony'] = df['room_name'].apply(self.parse_balcony)
            df['family_room'] = df['room_name'].apply(self.parse_family_room)
            df['club_room'] = df['room_name'].apply(self.parse_club_room)
            df['room_view'] = df['room_name'].apply(self.parse_room_view)
            df['room_keywords'] = df['room_name'].apply(self.parse_room_keywords)
            
            # Select and reorder columns according to target structure
            target_columns = [
                'reference_id', 'ref_hotel_name', 'hotel_id', 'hotel_name', 'room_name', 
                'main_name', 'bedrooms_count', 'room_capacity', 'room_area_m2', 'room_area_sqft',
                'room_quality', 'bedding_type', 'bedding_config', 'room_class', 'balcony', 
                'family_room', 'club_room', 'room_view', 'room_keywords'
            ]
            
            # Create output dataframe with only target columns
            output_df = df[target_columns]
            
            # Replace undefined and 0 values with null for better data quality
            # Apply to specific columns where these values should be null
            columns_to_clean = [
                'room_quality', 'bedding_type', 'bedding_config', 'room_class', 'room_view', 'room_keywords'
            ]
            
            for col in columns_to_clean:
                if col in output_df.columns:
                    output_df[col] = output_df[col].replace(['undefined', 'undefined_value', '0'], None)
            
            # Also clean numeric columns where 0 should be null
            numeric_columns_to_clean = ['bedrooms_count', 'room_capacity', 'balcony', 'family_room', 'club_room']
            
            for col in numeric_columns_to_clean:
                if col in output_df.columns:
                    output_df[col] = output_df[col].replace([0, '0'], None)
            
            # Clean room area columns where 0.0 should be null
            area_columns_to_clean = ['room_area_m2', 'room_area_sqft']
            for col in area_columns_to_clean:
                if col in output_df.columns:
                    output_df[col] = output_df[col].replace([0.0, '0.0', 0], None)
            
            # Save processed data
            output_df.to_csv(output_csv_path, index=False)
            
            return True
            
        except Exception as e:
            print(f"Error processing {provider}: {e}")
            return False
    



def process_goglobal_step_by_step(step: str = 'main_name', provider: str = 'goglobal'):
    """
    Process room data step by step, adding one column at a time
    
    Args:
        step: Which column to add ('main_name', 'bedrooms_count', etc.)
        provider: Provider name for specific logic ('goglobal', 'tbo', 'ratehawk', 'universal')
    """
    
    print("=" * 80)
    print(f"PROCESSING {provider.upper()} ROOM DATA - STEP: {step.upper()}")
    print("=" * 80)
    
    # Initialize parser with provider
    parser = RoomDataParser(provider=provider)
    
    # File paths
    if step == 'main_name':
        input_file = Path('app/data/02_api_goglobal_rooms.csv')
        output_file = Path('app/data/02_api_goglobal_rooms_step1_main_name.csv')
    else:
        # Use previous step's output as input
        previous_step = get_previous_step(step)
        input_file = Path(f'app/data/02_api_goglobal_rooms_step{get_step_number(previous_step)}_{previous_step}.csv')
        output_file = Path(f'app/data/02_api_goglobal_rooms_step{get_step_number(step)}_{step}.csv')
    
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    
    # Check if input file exists
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return
    
    # Load data
    print("Loading data...")
    df = pd.read_csv(input_file)
    print(f"Loaded {len(df)} room records from {len(df['hotel_id'].unique())} hotels")
    
    # Process based on step
    if step == 'main_name':
        print("Parsing main_name column...")
        df['main_name'] = df['room_name'].apply(parser.parse_main_name)
        show_main_name_examples(df)
        
    elif step == 'bedrooms_count':
        print("Parsing bedrooms_count column...")
        df['bedrooms_count'] = df['room_name'].apply(parser.parse_bedrooms_count)
        show_bedrooms_examples(df)
        
    elif step == 'room_capacity':
        print("Parsing room_capacity column...")
        df['room_capacity'] = df['room_name'].apply(parser.parse_room_capacity)
        show_room_capacity_examples(df)
        
    elif step == 'room_area':
        print("Parsing room_area columns...")
        area_results = df['room_name'].apply(parser.parse_room_area)
        df['room_area_m2'] = [result[0] for result in area_results]
        df['room_area_sqft'] = [result[1] for result in area_results]
        show_room_area_examples(df)
        
    elif step == 'room_class':
        print("Parsing room_class column...")
        df['room_class'] = df['room_name'].apply(parser.parse_room_class)
        show_room_class_examples(df)
        
    elif step == 'room_quality':
        print("Parsing room_quality column...")
        df['room_quality'] = df['room_name'].apply(parser.parse_room_quality)
        show_room_quality_examples(df)
        
    elif step == 'room_quality_category':
        print("Parsing room_quality_category column...")
        df['room_quality_category'] = df['room_name'].apply(parser.parse_room_quality_category)
        show_room_quality_category_examples(df)
        
    elif step == 'bedding_config':
        print("Parsing bedding_config column...")
        df['bedding_config'] = df['room_name'].apply(parser.parse_bedding_config)
        show_bedding_config_examples(df)
        
    elif step == 'bedding_type':
        print("Parsing bedding_type column...")
        df['bedding_type'] = df['room_name'].apply(parser.parse_bedding_type)
        show_bedding_type_examples(df)
    
    elif step == 'room_view':
        print("Parsing room_view column...")
        df['room_view'] = df['room_name'].apply(parser.parse_room_view)
        show_room_view_examples(df)
    
    elif step == 'balcony':
        print("Parsing balcony column...")
        df['balcony'] = df['room_name'].apply(parser.parse_balcony)
        show_balcony_examples(df)
    
    elif step == 'family_room':
        print("Parsing family_room column...")
        df['family_room'] = df['room_name'].apply(parser.parse_family_room)
        show_family_room_examples(df)
    
    elif step == 'club_room':
        print("Parsing club_room column...")
        df['club_room'] = df['room_name'].apply(parser.parse_club_room)
        show_club_room_examples(df)
    
    else:
        print(f"Unknown step: {step}")
        return
    
    # Save to file - keep only standard schema columns
    print("Saving processed data...")
    
    # Get standard schema columns from config
    standard_columns = list(parser.config['standard_schema'].keys())
    
    # Keep only columns that exist in both dataframe and standard schema
    columns_to_keep = [col for col in standard_columns if col in df.columns]
    df_filtered = df[columns_to_keep]
    
    print(f"Keeping {len(columns_to_keep)} standard columns: {columns_to_keep}")
    df_filtered.to_csv(output_file, index=False)
    print(f"Data saved to: {output_file}")
    
    print(f"Step '{step}' completed successfully!")
    return output_file

def show_main_name_examples(df):
    """Show main_name parsing examples"""
    print("Sample results:")
    for i, row in df.head(5).iterrows():
        if row['room_name'] != row['main_name']:
            print(f"CLEANED: '{row['room_name']}' -> '{row['main_name']}'")
        else:
            print(f"UNCHANGED: '{row['room_name']}'")

def show_bedrooms_examples(df):
    """Show bedrooms_count parsing examples"""
    print("Sample results:")
    examples = df[df['bedrooms_count'] > 0].head(5)
    for _, row in examples.iterrows():
        print(f"'{row['room_name']}' -> Bedrooms: {row['bedrooms_count']}")

def show_room_capacity_examples(df):
    """Show room_capacity parsing examples"""
    print("Sample results:")
    examples = df.head(8)  # Show more examples since most rooms will have capacity
    for _, row in examples.iterrows():
        print(f"'{row['room_name']}' -> Capacity: {row['room_capacity']} guests")

def show_room_area_examples(df):
    """Show room_area parsing examples"""
    print("Sample results:")
    # Show examples with areas found
    examples_with_area = df[(df['room_area_m2'] > 0) | (df['room_area_sqft'] > 0)].head(5)
    
    if len(examples_with_area) > 0:
        print("Rooms with areas found:")
        for _, row in examples_with_area.iterrows():
            print(f"'{row['room_name']}' -> {row['room_area_m2']} m² / {row['room_area_sqft']} sqft")
    else:
        print("No room areas found in the data")
    
    # Show distribution
    area_count = len(df[(df['room_area_m2'] > 0) | (df['room_area_sqft'] > 0)])
    total_count = len(df)
    print(f"Rooms with areas: {area_count}/{total_count} ({area_count/total_count*100:.1f}%)")
    
    # Show area statistics
    if area_count > 0:
        print(f"Area range (m²): {df[df['room_area_m2'] > 0]['room_area_m2'].min():.1f} - {df[df['room_area_m2'] > 0]['room_area_m2'].max():.1f}")
        print(f"Area range (sqft): {df[df['room_area_sqft'] > 0]['room_area_sqft'].min():.1f} - {df[df['room_area_sqft'] > 0]['room_area_sqft'].max():.1f}")

def show_room_class_examples(df):
    """Show room_class parsing examples"""
    print("Sample results:")
    # Show different room classes
    class_counts = df['room_class'].value_counts()
    print(f"Room class distribution: {dict(class_counts)}")
    
    # Show examples for each class type found
    for room_class in class_counts.index[:5]:
        example = df[df['room_class'] == room_class].iloc[0]
        print(f"'{example['room_name']}' -> Class: {room_class}")

def show_room_quality_examples(df):
    """Show room_quality parsing examples"""
    print("Sample results:")
    # Show different room qualities
    quality_counts = df['room_quality'].value_counts()
    print(f"Room quality distribution: {dict(quality_counts)}")
    
    # Show examples for each quality type found
    for room_quality in quality_counts.index[:5]:
        example = df[df['room_quality'] == room_quality].iloc[0]
        print(f"'{example['room_name']}' -> Quality: {room_quality}")

def show_room_quality_category_examples(df):
    """Show room_quality_category parsing examples"""
    print("Sample results:")
    # Show different room quality categories
    category_counts = df['room_quality_category'].value_counts()
    print(f"Room quality category distribution: {dict(category_counts)}")
    
    # Show examples for each category type found
    for category in category_counts.index[:5]:
        example = df[df['room_quality_category'] == category].iloc[0]
        print(f"'{example['room_name']}' -> Category: {category}")

def show_bedding_config_examples(df):
    """Show bedding_config parsing examples"""
    print("Sample results:")
    # Show different bedding configurations
    bedding_counts = df['bedding_config'].value_counts()
    print(f"Bedding config distribution: {dict(bedding_counts)}")
    
    # Show examples for each bedding type found
    for bedding_type in bedding_counts.index[:5]:
        example = df[df['bedding_config'] == bedding_type].iloc[0]
        print(f"'{example['room_name']}' -> Bedding: {bedding_type}")

def show_bedding_type_examples(df):
    """Show bedding_type parsing examples"""
    print("Sample results:")
    # Show examples with original bedding types found
    examples_with_bedding = df[df['bedding_type'] != 'undefined'].head(10)
    
    if len(examples_with_bedding) > 0:
        print("Original bedding descriptions found:")
        for _, row in examples_with_bedding.iterrows():
            print(f"'{row['room_name']}' -> Original: '{row['bedding_type']}'")
    else:
        print("No original bedding descriptions found in the data")
    
    # Show distribution of found vs not found
    found_count = len(df[df['bedding_type'] != 'undefined'])
    total_count = len(df)
    print(f"\nBedding descriptions found: {found_count}/{total_count} ({found_count/total_count*100:.1f}%)")

def show_room_view_examples(df):
    """Show room_view parsing examples"""
    print("Sample results:")
    # Show different view types
    view_counts = df['room_view'].value_counts()
    print(f"Room view distribution: {dict(view_counts)}")
    
    # Show examples for each view type found (excluding no_view)
    for view_type in view_counts.index:
        if view_type != 'no_view':
            example = df[df['room_view'] == view_type].iloc[0]
            print(f"'{example['room_name']}' -> View: {view_type}")
    
    # Show distribution
    view_count = len(df[df['room_view'] != 'no_view'])
    total_count = len(df)
    print(f"Rooms with views found: {view_count}/{total_count} ({view_count/total_count*100:.1f}%)")

def show_balcony_examples(df):
    """Show balcony parsing examples"""
    print("Sample results:")
    # Show balcony distribution
    balcony_counts = df['balcony'].value_counts()
    print(f"Balcony distribution: {dict(balcony_counts)}")
    
    # Show examples with balconies
    examples_with_balcony = df[df['balcony'] == 1].head(5)
    if len(examples_with_balcony) > 0:
        print("Rooms with balconies:")
        for _, row in examples_with_balcony.iterrows():
            print(f"'{row['room_name']}' -> Balcony: Yes")
    else:
        print("No rooms with balconies found in the data")
    
    # Show distribution
    balcony_count = len(df[df['balcony'] == 1])
    total_count = len(df)
    print(f"Rooms with balconies: {balcony_count}/{total_count} ({balcony_count/total_count*100:.1f}%)")

def show_family_room_examples(df):
    """Show family_room parsing examples"""
    print("Sample results:")
    # Show family room distribution
    family_counts = df['family_room'].value_counts()
    print(f"Family room distribution: {dict(family_counts)}")
    
    # Show examples of family rooms
    examples_family = df[df['family_room'] == 1].head(5)
    if len(examples_family) > 0:
        print("Family rooms:")
        for _, row in examples_family.iterrows():
            print(f"'{row['room_name']}' -> Family: Yes")
    else:
        print("No family rooms found in the data")
    
    # Show distribution
    family_count = len(df[df['family_room'] == 1])
    total_count = len(df)
    print(f"Family rooms: {family_count}/{total_count} ({family_count/total_count*100:.1f}%)")

def show_club_room_examples(df):
    """Show club_room parsing examples"""
    print("Sample results:")
    # Show club room distribution
    club_counts = df['club_room'].value_counts()
    print(f"Club room distribution: {dict(club_counts)}")
    
    # Show examples of club rooms
    examples_club = df[df['club_room'] == 1].head(5)
    if len(examples_club) > 0:
        print("Club/Executive rooms:")
        for _, row in examples_club.iterrows():
            print(f"'{row['room_name']}' -> Club: Yes")
    else:
        print("No club/executive rooms found in the data")
    
    # Show distribution
    club_count = len(df[df['club_room'] == 1])
    total_count = len(df)
    print(f"Club/Executive rooms: {club_count}/{total_count} ({club_count/total_count*100:.1f}%)")

def get_previous_step(step):
    """Get previous step name"""
    # Updated steps including room_area and boolean features
    steps = ['main_name', 'bedrooms_count', 'room_capacity', 'room_area', 'room_class', 'room_quality', 'room_quality_category', 'bedding_config', 'bedding_type', 'room_view', 'balcony', 'family_room', 'club_room']
    try:
        index = steps.index(step)
        return steps[index - 1] if index > 0 else 'main_name'
    except ValueError:
        return 'main_name'

def get_step_number(step):
    """Get step number"""
    # Updated steps including room_area and boolean features
    steps = ['main_name', 'bedrooms_count', 'room_capacity', 'room_area', 'room_class', 'room_quality', 'room_quality_category', 'bedding_config', 'bedding_type', 'room_view', 'balcony', 'family_room', 'club_room']
    try:
        return steps.index(step) + 1
    except ValueError:
        return 1

if __name__ == "__main__":
    import sys
    step = sys.argv[1] if len(sys.argv) > 1 else 'main_name'
    
    # Handle show examples functions
    if step.startswith('show_') and step.endswith('_examples'):
        # Load the latest data file to show examples
        import glob
        latest_files = glob.glob('app/data/02_api_goglobal_rooms_step*.csv')
        if latest_files:
            latest_file = max(latest_files)
            df = pd.read_csv(latest_file)
            
            # Call the appropriate show function
            if step == 'show_bedding_type_examples':
                show_bedding_type_examples(df)
            elif step == 'show_bedding_config_examples':
                show_bedding_config_examples(df)
            elif step == 'show_room_view_examples':
                show_room_view_examples(df)
            elif step == 'show_balcony_examples':
                show_balcony_examples(df)
            elif step == 'show_family_room_examples':
                show_family_room_examples(df)
            elif step == 'show_club_room_examples':
                show_club_room_examples(df)
            elif step == 'show_room_quality_examples':
                show_room_quality_examples(df)
            elif step == 'show_room_quality_category_examples':
                show_room_quality_category_examples(df)
            elif step == 'show_room_class_examples':
                show_room_class_examples(df)
            elif step == 'show_room_capacity_examples':
                show_room_capacity_examples(df)
            elif step == 'show_room_area_examples':
                show_room_area_examples(df)
            elif step == 'show_bedrooms_examples':
                show_bedrooms_examples(df)
            elif step == 'show_main_name_examples':
                show_main_name_examples(df)
            else:
                print(f"Unknown show function: {step}")
        else:
            print("No data files found to show examples")
    else:
        # Regular step processing
        process_goglobal_step_by_step(step)
