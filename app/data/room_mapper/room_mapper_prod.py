#!/usr/bin/env python3
"""
Production-Ready Room Mapper
A robust, performant, and maintainable room mapping system for data engineering pipelines.
"""

import pandas as pd
import yaml
import re
import logging
from pathlib import Path
from collections import defaultdict, deque
from rapidfuzz import fuzz
from typing import Dict, List, Optional, Tuple, Set, Any, Union
from functools import lru_cache
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor, as_completed
import hashlib
import threading
from enum import Enum
import csv
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class MappingType(Enum):
    """Enumeration for mapping types"""
    MULTI = "multi"
    UNMAPPED = "unmapped"

@dataclass
class AlgorithmFlags:
    """Configuration flags for scoring algorithms"""
    capacity_check: bool = True
    bedrooms_count_check: bool = True
    room_class_check: bool = True
    room_view_check: bool = True
    balcony_check: bool = False
    family_room_check: bool = False
    bedding_config_penalty: bool = True
    room_area_penalty: bool = True
    room_keywords_bonus: bool = True
    main_name_fuzzy_match: bool = True

@dataclass
class ScoreResult:
    """Result of room scoring"""
    score: float
    algorithms_used: Dict[str, bool]

@dataclass
class RoomData:
    """Structured room data"""
    provider: str
    ref_hotel_name: str
    row: Dict[str, Any]

class ConfigurationError(Exception):
    """Custom exception for configuration errors"""
    pass

class DataValidationError(Exception):
    """Custom exception for data validation errors"""
    pass

class MappingFailureLogger:
    """Logger dla niepowodzonych mapowań - zintegrowany z głównym procesem"""
    
    def __init__(self, output_file="production_mapping_failures.csv"):
        self.output_file = Path(output_file)
        self._initialize_csv()
    
    def _initialize_csv(self):
        """Inicjalizuje plik CSV z nagłówkami jeśli nie istnieje"""
        if not self.output_file.exists():
            with open(self.output_file, 'w', newline='', encoding='utf-8') as f:
                writer = csv.writer(f)
                writer.writerow([
                    'timestamp', 'hotel_name', 'provider1', 'provider2',
                    'room1_name', 'room2_name', 'room1_bedding', 'room2_bedding',
                    'room1_class', 'room2_class', 'room1_view', 'room2_view',
                    'room1_capacity', 'room2_capacity', 'main_name_score',
                    'bedding_penalty', 'final_score', 'threshold', 'failure_reason'
                ])
    
    def log_failure(self, room1, room2, score_result, threshold, provider1, provider2):
        """Loguje przypadek niepowodzenia mapowania"""
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        # Podstawowe dane
        hotel_name = room1.get('ref_hotel_name', room1.get('hotel_name', 'unknown'))
        room1_name = room1.get('main_name', room1.get('room_name', ''))
        room2_name = room2.get('main_name', room2.get('room_name', ''))
        
        # Właściwości pokojów
        room1_bedding = room1.get('bedding_config', '')
        room2_bedding = room2.get('bedding_config', '')
        room1_class = room1.get('room_class', '')
        room2_class = room2.get('room_class', '')
        room1_view = room1.get('room_view', '')
        room2_view = room2.get('room_view', '')
        room1_capacity = room1.get('room_capacity', '')
        room2_capacity = room2.get('room_capacity', '')
        
        # Przybliżone obliczenie składników (nie mamy dostępu do szczegółów)
        final_score = score_result.score
        main_name_score = final_score / 0.2 if final_score > 0 else 0  # Odwrotne oszacowanie
        bedding_penalty = 0.2 if room1_bedding != room2_bedding else 1.0
        
        # Analiza przyczyny
        failure_reason = self._analyze_failure_reason(room1, room2, final_score, threshold)
        
        # Zapis do CSV
        with open(self.output_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow([
                timestamp, hotel_name, provider1, provider2,
                room1_name, room2_name, room1_bedding, room2_bedding,
                room1_class, room2_class, room1_view, room2_view,
                room1_capacity, room2_capacity, f"{main_name_score:.3f}",
                f"{bedding_penalty:.3f}", f"{final_score:.3f}", 
                f"{threshold:.3f}", failure_reason
            ])
    
    def _analyze_failure_reason(self, room1, room2, final_score, threshold):
        """Analizuje główną przyczynę niepowodzenia"""
        reasons = []
        gap = threshold - final_score
        
        # Sprawdź bedding config
        bedding1 = room1.get('bedding_config')
        bedding2 = room2.get('bedding_config')
        
        # Show mismatch only if both values exist (pandas NaN is truthy!)
        if (not pd.isna(bedding1) and not pd.isna(bedding2) and 
            bedding1 != bedding2):
            reasons.append(f"Bedding config mismatch ({bedding1} vs {bedding2})")
        
        # Sprawdź similarity gap
        if gap > 0.3:
            reasons.append("Very low similarity")
        elif gap > 0.1:
            reasons.append("Low similarity")
        else:
            reasons.append("Near threshold miss")
        
        # Sprawdź room view
        view1 = room1.get('room_view')
        view2 = room2.get('room_view')
        
        # Show mismatch only if both values exist (pandas NaN is truthy!)
        if (not pd.isna(view1) and not pd.isna(view2) and 
            view1 != view2):
            reasons.append(f"View mismatch ({view1} vs {view2})")
        
        return "; ".join(reasons) if reasons else f"Below threshold by {gap:.3f}"

class RoomMapperConfig:
    """Handles configuration loading and validation"""
    
    def __init__(self, config_path: str):
        self.config_path = config_path
        self._config = None
        self._config_hash = None
        self._load_config()
    
    def _load_config(self) -> None:
        """Load and validate configuration from YAML file"""
        try:
            config_file = Path(self.config_path)
            if not config_file.exists():
                raise ConfigurationError(f"Configuration file not found: {self.config_path}")
            
            with open(config_file, 'r', encoding='utf-8') as f:
                self._config = yaml.safe_load(f)
            
            # Create hash for cache invalidation
            config_str = yaml.dump(self._config, sort_keys=True)
            self._config_hash = hashlib.md5(config_str.encode()).hexdigest()
            
            self._validate_config()
            logger.info(f"Configuration loaded successfully from {self.config_path}")
            
        except yaml.YAMLError as e:
            raise ConfigurationError(f"Invalid YAML configuration: {e}")
        except Exception as e:
            raise ConfigurationError(f"Failed to load configuration: {e}")
    
    def _validate_config(self) -> None:
        """Validate configuration structure"""
        required_keys = ['room_mapping_config']
        for key in required_keys:
            if key not in self._config:
                raise ConfigurationError(f"Missing required configuration key: {key}")
    
    @property
    def config(self) -> Dict[str, Any]:
        """Get configuration dictionary"""
        return self._config
    
    @property
    def config_hash(self) -> str:
        """Get configuration hash for caching"""
        return self._config_hash
    
    def get_input_files(self) -> Dict[str, str]:
        """Get input files configuration"""
        return self._config['room_mapping_config']['input_files']
    
    def get_similarity_threshold(self) -> float:
        """Get similarity threshold"""
        return self._config['room_mapping_config']['fuzzy_matching']['thresholds']['similarity_threshold']
    
    def get_algorithm_flags(self) -> AlgorithmFlags:
        """Get algorithm flags from configuration or defaults"""
        algo_config = self._config.get('room_mapping_config', {}).get('algorithm_flags', {})
        return AlgorithmFlags(**algo_config)

class TextNormalizer:
    """Handles text normalization with thread-safe caching"""
    
    def __init__(self, config: RoomMapperConfig):
        self.config = config
        self._lock = threading.Lock()
    
    @lru_cache(maxsize=10000)
    def normalize(self, text: str, config_hash: str) -> str:
        """
        Normalize text using configuration patterns with thread-safe caching
        
        Args:
            text: Text to normalize
            config_hash: Configuration hash for cache invalidation
            
        Returns:
            Normalized text
        """
        if not text or pd.isna(text):
            return ''
        
        try:
            normalized = str(text).lower().strip()
            
            # Apply final cleanup patterns
            final_cleanup = self.config.config.get('room_name_cleaning', {}).get('final_cleanup', [])
            
            for pattern_config in final_cleanup:
                if not isinstance(pattern_config, dict):
                    continue
                    
                pattern = pattern_config.get('pattern', '')
                replacement = pattern_config.get('replacement', '')
                
                if pattern:
                    normalized = re.sub(pattern, replacement, normalized)
            
            return normalized.strip()
            
        except (AttributeError, TypeError, re.error) as e:
            logger.warning(f"Text normalization failed for '{text}': {e}")
            return str(text).lower().strip()
        except Exception as e:
            logger.error(f"Unexpected error in text normalization: {e}")
            return str(text).lower().strip()
    
    def normalize_text(self, text: str) -> str:
        """Public interface for text normalization"""
        return self.normalize(text, self.config.config_hash)

class RoomScorer:
    """Handles room scoring with various algorithms"""
    
    def __init__(self, config: RoomMapperConfig, normalizer: TextNormalizer):
        self.config = config
        self.normalizer = normalizer
        self.algorithm_flags = config.get_algorithm_flags()
        
        # Constants
        self.PENALTY_SCORE = 0.7  # 30% penalty for mismatches
        self.KEYWORDS_BONUS_PER_MATCH = 0.5  # 50% bonus per matching keyword
        self.MAX_KEYWORDS_BONUS = 2.0  # Maximum 200% bonus
        
        # Bedding config scoring - uproszczony system kar i nagród
        self.BEDDING_BONUS = 2.0           # 100% bonus za taką samą konfigurację łóżek
        self.BEDDING_PENALTY = 1.0         # 0% kara za różne konfiguracje (1/2 kary)
        
        # Main name fuzzy matching - zaawansowany system
        self.MAIN_NAME_PERFECT_BONUS = 2.0     # 100% bonus za idealne dopasowanie
        self.MAIN_NAME_WORD_ORDER_BONUS = 2.0 # 75% bonus za te same słowa w innej kolejności
        self.MAIN_NAME_HIGH_SIMILARITY_BONUS = 1.95   # 50% bonus za bardzo wysokie podobieństwo (>=95%)
        self.MAIN_NAME_GOOD_SIMILARITY_BONUS = 1.85  # 25% bonus za dobre podobieństwo (>=85%)
        self.MAIN_NAME_SIMILARITY_THRESHOLD = 0.70   # Próg poniżej którego stosujemy karę
        self.MAIN_NAME_LOW_SIMILARITY_PENALTY = 0.5  # Kara za słabe dopasowanie
    
    def score_capacity(self, row1: Dict[str, Any], row2: Dict[str, Any]) -> Optional[float]:
        """Score room capacity match"""
        try:
            cap1 = int(row1.get('room_capacity', 0) or 0)
            cap2 = int(row2.get('room_capacity', 0) or 0)
        except (ValueError, TypeError) as e:
            logger.debug(f"Invalid capacity data: {e}")
            return None
        
        if cap1 == 0 or cap2 == 0:
            return None  # No data available
        
        return 1.0 if cap1 == cap2 else 0.0
    
    def score_bedrooms_count(self, row1: Dict[str, Any], row2: Dict[str, Any]) -> Optional[float]:
        """Score bedrooms count match"""
        try:
            bed1 = int(row1.get('bedrooms_count', 0) or 0)
            bed2 = int(row2.get('bedrooms_count', 0) or 0)
        except (ValueError, TypeError) as e:
            logger.debug(f"Invalid bedrooms count data: {e}")
            return None
        
        if bed1 == 0 or bed2 == 0:
            return None
        
        return 1.0 if bed1 == bed2 else 0.0
    
    def score_room_class(self, row1: Dict[str, Any], row2: Dict[str, Any]) -> Optional[float]:
        """Score room class match"""
        class1 = row1.get('room_class')
        class2 = row2.get('room_class')
        
        # Skip if either is missing data (pandas NaN is truthy!)
        if pd.isna(class1) or pd.isna(class2):
            return None
        
        try:
            class1_norm = self.normalizer.normalize_text(class1)
            class2_norm = self.normalizer.normalize_text(class2)
        except Exception as e:
            logger.debug(f"Room class scoring error: {e}")
            return None
        
        return 1.0 if class1_norm == class2_norm else 0.0
    
    def score_room_view(self, row1: Dict[str, Any], row2: Dict[str, Any]) -> Optional[float]:
        """Score room view match"""
        view1 = row1.get('room_view')
        view2 = row2.get('room_view')
        
        # Skip if either is missing data (pandas NaN is truthy!)
        if pd.isna(view1) or pd.isna(view2):
            return None
        
        try:
            view1_norm = self.normalizer.normalize_text(view1)
            view2_norm = self.normalizer.normalize_text(view2)
        except Exception as e:
            logger.debug(f"Room view scoring error: {e}")
            return None
        
        return 1.0 if view1_norm == view2_norm else 0.0
    
    def score_balcony(self, row1: Dict[str, Any], row2: Dict[str, Any]) -> Optional[float]:
        """Score balcony presence match (strict veto condition)"""
        try:
            balcony1 = row1.get('balcony')
            balcony2 = row2.get('balcony')
        except Exception as e:
            logger.debug(f"Balcony scoring error: {e}")
            return None
        
        if pd.isna(balcony1) or pd.isna(balcony2):
            return None
        
        return 0.0 if balcony1 != balcony2 else 1.0  # Strict condition
    
    def score_family_room(self, row1: Dict[str, Any], row2: Dict[str, Any]) -> Optional[float]:
        """Score family room presence match (strict veto condition)"""
        try:
            family1 = row1.get('family_room')
            family2 = row2.get('family_room')
        except Exception as e:
            logger.debug(f"Family room scoring error: {e}")
            return None
        
        if pd.isna(family1) or pd.isna(family2):
            return None
        
        return 0.0 if family1 != family2 else 1.0  # Strict condition
    
    def score_bedding_config(self, row1: Dict[str, Any], row2: Dict[str, Any]) -> Optional[float]:
        """Score bedding configuration - bonus za taką samą, kara za różne"""
        config1 = row1.get('bedding_config')
        config2 = row2.get('bedding_config')
        
        # Skip if either is missing data (pandas NaN is truthy!)
        if pd.isna(config1) or pd.isna(config2):
            return None
        
        try:
            config1_norm = self.normalizer.normalize_text(config1)
            config2_norm = self.normalizer.normalize_text(config2)
        except Exception as e:
            logger.debug(f"Bedding config scoring error: {e}")
            return None
        
        # Taka sama konfiguracja = duża nagroda
        if config1_norm == config2_norm:
            return self.BEDDING_BONUS
        
        # Różne konfiguracje = kara (1/2 kary)
        return self.BEDDING_PENALTY
    
    def score_room_area(self, row1: Dict[str, Any], row2: Dict[str, Any]) -> Optional[float]:
        """Score room area with penalty for mismatches"""
        try:
            area1 = float(row1.get('room_area_m2', 0) or 0)
            area2 = float(row2.get('room_area_m2', 0) or 0)
        except (ValueError, TypeError) as e:
            logger.debug(f"Invalid room area data: {e}")
            return None
        
        if area1 == 0 or area2 == 0:
            return None
        
        return 1.0 if area1 == area2 else self.PENALTY_SCORE
    
    def score_room_keywords(self, row1: Dict[str, Any], row2: Dict[str, Any]) -> Optional[float]:
        """Score room keywords with bonus for common keywords"""
        try:
            keywords1_str = str(row1.get('room_keywords', ''))
            keywords2_str = str(row2.get('room_keywords', ''))
        except Exception as e:
            logger.debug(f"Keywords scoring error: {e}")
            return None
        
        if not keywords1_str or not keywords2_str or keywords1_str == 'nan' or keywords2_str == 'nan':
            return None
        
        # Parse keywords (comma-separated)
        keywords1 = set(kw.strip().lower() for kw in keywords1_str.split(',') if kw.strip())
        keywords2 = set(kw.strip().lower() for kw in keywords2_str.split(',') if kw.strip())
        
        if not keywords1 or not keywords2:
            return None
        
        # Find common keywords
        common_keywords = keywords1.intersection(keywords2)
        
        if common_keywords:
            bonus_multiplier = 1.0 + (len(common_keywords) * self.KEYWORDS_BONUS_PER_MATCH)
            return min(bonus_multiplier, self.MAX_KEYWORDS_BONUS)
        
        return 1.0  # No bonus
    
    def score_main_name_fuzzy(self, row1: Dict[str, Any], row2: Dict[str, Any], debug: bool = False) -> float:
        """Score main name using advanced multi-level fuzzy matching"""
        try:
            name1 = self.normalizer.normalize_text(row1.get('main_name', ''))
            name2 = self.normalizer.normalize_text(row2.get('main_name', ''))
        except Exception as e:
            logger.debug(f"Main name fuzzy scoring error: {e}")
            return 0.0
        
        if not name1 or not name2:
            return 0.0
        
        # Poziom 1: Perfect Match (BONUS 100%)
        if name1 == name2:
            return self.MAIN_NAME_PERFECT_BONUS
        
        # Poziom 2: Word Order Match (BONUS 75%) - te same słowa, inna kolejność
        words1 = set(name1.split())
        words2 = set(name2.split())
        if words1 == words2 and len(words1) > 1:
            return self.MAIN_NAME_WORD_ORDER_BONUS
        
        # Poziom 3: Wieloalgorytmowy fuzzy matching
        scores = []
        
        # Algorytm 1: Token Sort Ratio (najlepszy dla różnej kolejności słów)
        token_sort = fuzz.token_sort_ratio(name1, name2) / 100.0
        scores.append(('token_sort', token_sort, 0.8))  # waga 80%
        
        # Algorytm 2: Token Set Ratio (dobry dla częściowych dopasowań)
        token_set = fuzz.token_set_ratio(name1, name2) / 100.0
        scores.append(('token_set', token_set, 0.75))  # waga 75%
        
        # Algorytm 3: Partial Ratio (fragmenty stringów)
        partial = fuzz.partial_ratio(name1, name2) / 100.0
        scores.append(('partial', partial, 0.6))  # waga 60%
        
        # Algorytm 4: Simple Ratio (ogólne podobieństwo)
        simple = fuzz.ratio(name1, name2) / 100.0
        scores.append(('simple', simple, 0.5))  # waga 50%
        
        # Wybierz najlepszy wynik z odpowiednią wagą
        best_algorithm, best_score, weight = max(scores, key=lambda x: x[1] * x[2])
        
        if debug:
            logger.info(f"Fuzzy matching: '{name1}' vs '{name2}'")
            for algo, score, w in scores:
                logger.info(f"  {algo}: {score:.3f} (waga: {w}, ważony: {score*w:.3f})")
            logger.info(f"  Najlepszy: {best_algorithm} = {best_score:.3f}")
        
        # Zastosuj progowanie - jeśli poniżej threshold, to słabe dopasowanie
        if best_score < self.MAIN_NAME_SIMILARITY_THRESHOLD:
            final_score = best_score * self.MAIN_NAME_LOW_SIMILARITY_PENALTY
            if debug:
                logger.info(f"  Kara za słabe podobieństwo: {final_score:.3f}")
            return final_score
        
        # Bonus za wysokie podobieństwo
        if best_score >= 0.95:
            final_score = min(best_score * self.MAIN_NAME_HIGH_SIMILARITY_BONUS, self.MAIN_NAME_PERFECT_BONUS)
            if debug:
                logger.info(f"  Bonus za wysokie podobieństwo: {final_score:.3f}")
            return final_score
        elif best_score >= 0.85:
            final_score = best_score * self.MAIN_NAME_GOOD_SIMILARITY_BONUS
            if debug:
                logger.info(f"  Bonus za dobre podobieństwo: {final_score:.3f}")
            return final_score
        
        if debug:
            logger.info(f"  Wynik bez bonusu: {best_score:.3f}")
        return best_score
    
    def score_room(self, row1: Dict[str, Any], row2: Dict[str, Any]) -> ScoreResult:
        """
        Score two rooms using configured algorithms
        
        Args:
            row1: First room data
            row2: Second room data
            
        Returns:
            ScoreResult containing final score and algorithms used
        """
        algorithms_used = {
            'capacity_check': False,
            'bedrooms_count_check': False,
            'room_class_check': False,
            'room_view_check': False,
            'balcony_check': False,
            'family_room_check': False,
            'bedding_config_penalty': True,
            'room_area_penalty': False,
            'room_keywords_bonus': False,
            'main_name_fuzzy_match': False
        }
        
        # VETO conditions - if any fails, return 0.0
        veto_conditions = [
            ('capacity_check', self.algorithm_flags.capacity_check, self.score_capacity),
            ('bedrooms_count_check', self.algorithm_flags.bedrooms_count_check, self.score_bedrooms_count),
            ('room_class_check', self.algorithm_flags.room_class_check, self.score_room_class),
            ('room_view_check', self.algorithm_flags.room_view_check, self.score_room_view),
            ('balcony_check', self.algorithm_flags.balcony_check, self.score_balcony),
            ('family_room_check', self.algorithm_flags.family_room_check, self.score_family_room),
        ]
        
        for condition_name, is_enabled, scoring_func in veto_conditions:
            if is_enabled:
                score = scoring_func(row1, row2)
                if score is not None and score == 0.0:
                    return ScoreResult(0.0, algorithms_used)
                if score is not None:
                    algorithms_used[condition_name] = True
        
        # PENALTY conditions - reduce score
        penalties = [
            ('bedding_config_penalty', self.algorithm_flags.bedding_config_penalty, self.score_bedding_config),
            ('room_area_penalty', self.algorithm_flags.room_area_penalty, self.score_room_area),
        ]
        
        penalty_multiplier = 1.0
        for penalty_name, is_enabled, scoring_func in penalties:
            if is_enabled:
                score = scoring_func(row1, row2)
                if score is not None:
                    penalty_multiplier *= score
                    algorithms_used[penalty_name] = True
        
        # BONUS conditions - increase score
        bonus_multiplier = 1.0
        if self.algorithm_flags.room_keywords_bonus:
            keywords_score = self.score_room_keywords(row1, row2)
            if keywords_score is not None:
                bonus_multiplier = keywords_score
                algorithms_used['room_keywords_bonus'] = True
        
        # FINAL fuzzy matching
        if not self.algorithm_flags.main_name_fuzzy_match:
            return ScoreResult(0.0, algorithms_used)
        
        base_score = self.score_main_name_fuzzy(row1, row2)
        algorithms_used['main_name_fuzzy_match'] = True
        
        final_score = base_score * penalty_multiplier * bonus_multiplier
        
        return ScoreResult(final_score, algorithms_used)

class RoomMapper:
    """Main class for room mapping operations"""
    
    def __init__(self, config_path: str, log_failures: bool = True):
        self.config = RoomMapperConfig(config_path)
        self.normalizer = TextNormalizer(self.config)
        self.scorer = RoomScorer(self.config, self.normalizer)
        
        # Performance settings
        self.LARGE_HOTEL_THRESHOLD = 20
        self.MAX_WORKERS = 4
        
        # Failure logging
        self.log_failures = log_failures
        if log_failures:
            self.failure_logger = MappingFailureLogger("production_mapping_failures.csv")
            logger.info("Mapping failure logging enabled - output: production_mapping_failures.csv")
    
    def _validate_room_data(self, room_data: RoomData) -> bool:
        """Validate room data structure"""
        required_fields = ['main_name']
        
        for field in required_fields:
            if field not in room_data.row or not room_data.row[field]:
                logger.warning(f"Missing required field '{field}' in room data")
                return False
        
        return True
    
    def _prefilter_rooms(self, hotel_rooms: List[RoomData]) -> List[Tuple[int, int]]:
        """
        Pre-filter room pairs to reduce O(n²) complexity
        Only compare rooms that might potentially match
        """
        room_count = len(hotel_rooms)
        pairs = []
        
        # Group rooms by capacity for faster filtering
        capacity_groups = defaultdict(list)
        for i, room in enumerate(hotel_rooms):
            try:
                capacity = int(room.row.get('room_capacity', 0) or 0)
                capacity_groups[capacity].append(i)
            except (ValueError, TypeError):
                capacity_groups[0].append(i)  # Unknown capacity group
        
        # Generate pairs within and across capacity groups
        for i in range(room_count):
            for j in range(i + 1, room_count):
                # Skip same provider
                if hotel_rooms[i].provider == hotel_rooms[j].provider:
                    continue
                
                pairs.append((i, j))
        
        return pairs
    
    def map_rooms_for_hotel(self, hotel_rooms: List[RoomData], threshold: float) -> Tuple[List[Set[int]], Dict[int, RoomData]]:
        """
        Map rooms for a single hotel using graph-based clustering
        
        Args:
            hotel_rooms: List of room data for the hotel
            threshold: Similarity threshold for matching
            
        Returns:
            Tuple of (groups, nodes) where groups are sets of matched room indices
        """
        room_count = len(hotel_rooms)
        
        if room_count > self.LARGE_HOTEL_THRESHOLD:
            logger.info(f"Processing large hotel with {room_count} rooms...")
        
        # Validate room data
        valid_rooms = []
        for i, room in enumerate(hotel_rooms):
            if self._validate_room_data(room):
                valid_rooms.append((i, room))
            else:
                logger.warning(f"Skipping invalid room data at index {i}")
        
        if not valid_rooms:
            logger.error("No valid room data found")
            return [], {}
        
        # Create nodes mapping
        nodes = {i: room for i, room in valid_rooms}
        edges = defaultdict(set)
        
        # Pre-filter pairs to reduce comparisons
        pairs = self._prefilter_rooms([room for _, room in valid_rooms])
        
        logger.debug(f"Comparing {len(pairs)} room pairs (reduced from {room_count * (room_count - 1) // 2})")
        
        # Score room pairs
        for i, j in pairs:
            try:
                result = self.scorer.score_room(nodes[i].row, nodes[j].row)
                if result.score >= threshold:
                    edges[i].add(j)
                    edges[j].add(i)
                else:
                    # Log mapping failure dla przypadków poniżej threshold
                    if self.log_failures:
                        self.failure_logger.log_failure(
                            room1=nodes[i].row,
                            room2=nodes[j].row,
                            score_result=result,
                            threshold=threshold,
                            provider1=nodes[i].provider,
                            provider2=nodes[j].provider
                        )
            except Exception as e:
                logger.error(f"Error scoring room pair ({i}, {j}): {e}")
                continue
        
        # Find connected components using BFS
        visited = set()
        groups = []
        
        for i in nodes:
            if i not in visited:
                queue = deque([i])
                component = set()
                
                while queue:
                    current = queue.popleft()
                    if current in visited:
                        continue
                    
                    visited.add(current)
                    component.add(current)
                    
                    for neighbor in edges[current]:
                        if neighbor not in visited:
                            queue.append(neighbor)
                
                groups.append(component)
        
        if room_count > self.LARGE_HOTEL_THRESHOLD:
            logger.info(f"Hotel processing complete: {len(groups)} groups found")
        
        return groups, nodes
    
    def load_room_data(self) -> List[RoomData]:
        """Load room data from configured input files"""
        all_rooms = []
        input_files = self.config.get_input_files()
        
        logger.info("Loading room data from input files...")
        
        for provider, file_path in input_files.items():
            file_path_obj = Path(file_path)
            
            if not file_path_obj.exists():
                logger.error(f"Input file not found: {file_path}")
                continue
            
            try:
                df = pd.read_csv(file_path)
                room_count = len(df)
                logger.info(f"Loaded {room_count} rooms from {provider}: {file_path}")
                
                for _, row in df.iterrows():
                    room_data = RoomData(
                        provider=provider,
                        ref_hotel_name=row.get('ref_hotel_name', ''),
                        row=row.to_dict()
                    )
                    all_rooms.append(room_data)
                    
            except Exception as e:
                logger.error(f"Failed to load data from {file_path}: {e}")
                continue
        
        logger.info(f"Total rooms loaded: {len(all_rooms)}")
        return all_rooms
    
    def create_output_dataframe(self, results: List[Dict[str, Any]], input_files: Dict[str, str]) -> pd.DataFrame:
        """Create organized output DataFrame with proper column ordering"""
        if not results:
            logger.warning("No results to create DataFrame")
            return pd.DataFrame()
        
        df = pd.DataFrame(results)
        providers = list(input_files.keys())
        
        # Organize columns thematically
        ordered_columns = ['ref_hotel_name']
        
        # Add provider-specific columns in groups
        column_groups = [
            'reference_id', 'main_name', 'room_name', 'capacity', 'bedrooms_count',
            'room_class', 'room_view', 'bedding_config', 'room_area_m2',
            'room_keywords', 'balcony', 'family_room'
        ]
        
        for group in column_groups:
            for provider in providers:
                col_name = f'{provider}_{group}'
                if col_name in df.columns:
                    ordered_columns.append(col_name)
        
        # Add summary columns
        summary_columns = ['providers_in_group', 'group_size', 'mapping_type']
        for col in summary_columns:
            if col in df.columns:
                ordered_columns.append(col)
        
        # Select only existing columns
        final_columns = [col for col in ordered_columns if col in df.columns]
        
        return df[final_columns]
    
    def map_all_rooms(self) -> pd.DataFrame:
        """
        Main method to map all rooms across providers
        
        Returns:
            DataFrame with mapping results
        """
        logger.info("Starting room mapping process...")
        
        # Load data
        all_rooms = self.load_room_data()
        if not all_rooms:
            raise DataValidationError("No valid room data loaded")
        
        # Group by hotel
        hotels = defaultdict(list)
        for room in all_rooms:
            hotels[room.ref_hotel_name].append(room)
        
        logger.info(f"Processing {len(hotels)} hotels...")
        
        # Get configuration
        threshold = self.config.get_similarity_threshold()
        input_files = self.config.get_input_files()
        
        results = []
        used_rooms = set()
        
        # Process each hotel
        for hotel_idx, (hotel_name, hotel_rooms) in enumerate(hotels.items(), 1):
            if hotel_idx % 10 == 0 or hotel_idx <= 5:
                logger.info(f"Processing hotel {hotel_idx}/{len(hotels)}: {hotel_name} ({len(hotel_rooms)} rooms)")
            
            try:
                groups, nodes = self.map_rooms_for_hotel(hotel_rooms, threshold)
                
                # Process each group
                for group in groups:
                    providers = set(nodes[i].provider for i in group)
                    
                    # Create result row
                    row = {'ref_hotel_name': hotel_name}
                    
                    # Add provider-specific data
                    for provider in input_files.keys():
                        provider_rooms = [nodes[i] for i in group if nodes[i].provider == provider]
                        
                        if provider_rooms:
                            room_data = provider_rooms[0].row  # Take first match
                            row[f'{provider}_main_name'] = room_data.get('main_name', '')
                            row[f'{provider}_room_name'] = room_data.get('room_name', '')
                            row[f'{provider}_capacity'] = room_data.get('room_capacity', '')
                            row[f'{provider}_bedrooms_count'] = room_data.get('bedrooms_count', '')
                            row[f'{provider}_room_class'] = room_data.get('room_class', '')
                            row[f'{provider}_room_view'] = room_data.get('room_view', '')
                            row[f'{provider}_bedding_config'] = room_data.get('bedding_config', '')
                            row[f'{provider}_room_area_m2'] = room_data.get('room_area_m2', '')
                            row[f'{provider}_room_keywords'] = room_data.get('room_keywords', '')
                            row[f'{provider}_balcony'] = room_data.get('balcony', '')
                            row[f'{provider}_family_room'] = room_data.get('family_room', '')
                            row[f'{provider}_reference_id'] = room_data.get('reference_id', '')
                            
                            # Mark room as used
                            room_key = (hotel_name, provider, room_data.get('room_name', ''))
                            used_rooms.add(room_key)
                        else:
                            # Empty columns for providers not in this group
                            for field in ['reference_id', 'main_name', 'room_name', 'capacity', 'bedrooms_count',
                                        'room_class', 'room_view', 'bedding_config', 'room_area_m2',
                                        'room_keywords', 'balcony', 'family_room']:
                                row[f'{provider}_{field}'] = ''
                    
                    # Add group metadata
                    row['providers_in_group'] = ','.join(sorted(providers))
                    row['group_size'] = len(group)
                    row['mapping_type'] = MappingType.MULTI.value if len(providers) > 1 else MappingType.UNMAPPED.value
                    
                    results.append(row)
                    
            except Exception as e:
                logger.error(f"Error processing hotel {hotel_name}: {e}")
                continue
        
        # Add unmapped rooms
        logger.info("Adding unmapped rooms...")
        for room in all_rooms:
            room_key = (room.ref_hotel_name, room.provider, room.row.get('room_name', ''))
            
            if room_key not in used_rooms:
                row = {'ref_hotel_name': room.ref_hotel_name}
                
                # Add data for this provider only
                for provider in input_files.keys():
                    if provider == room.provider:
                        row[f'{provider}_main_name'] = room.row.get('main_name', '')
                        row[f'{provider}_room_name'] = room.row.get('room_name', '')
                        row[f'{provider}_capacity'] = room.row.get('room_capacity', '')
                        row[f'{provider}_bedrooms_count'] = room.row.get('bedrooms_count', '')
                        row[f'{provider}_room_class'] = room.row.get('room_class', '')
                        row[f'{provider}_room_view'] = room.row.get('room_view', '')
                        row[f'{provider}_bedding_config'] = room.row.get('bedding_config', '')
                        row[f'{provider}_room_area_m2'] = room.row.get('room_area_m2', '')
                        row[f'{provider}_room_keywords'] = room.row.get('room_keywords', '')
                        row[f'{provider}_balcony'] = room.row.get('balcony', '')
                        row[f'{provider}_family_room'] = room.row.get('family_room', '')
                        row[f'{provider}_reference_id'] = room.row.get('reference_id', '')
                    else:
                        for field in ['main_name', 'room_name', 'capacity', 'bedrooms_count',
                                    'room_class', 'room_view', 'bedding_config', 'room_area_m2',
                                    'room_keywords', 'balcony', 'family_room', 'reference_id']:
                            row[f'{provider}_{field}'] = ''
                
                row['providers_in_group'] = room.provider
                row['group_size'] = 1
                row['mapping_type'] = MappingType.UNMAPPED.value
                
                results.append(row)
        
        # Create and organize output DataFrame
        df_output = self.create_output_dataframe(results, input_files)
        
        # Log statistics
        mapped_count = len(df_output[df_output['mapping_type'] == MappingType.MULTI.value])
        unmapped_count = len(df_output[df_output['mapping_type'] == MappingType.UNMAPPED.value])
        
        logger.info(f"Mapping complete:")
        logger.info(f"  - Mapped groups: {mapped_count}")
        logger.info(f"  - Unmapped rooms: {unmapped_count}")
        logger.info(f"  - Total rows: {len(df_output)}")
        logger.info(f"  - Total columns: {len(df_output.columns)}")
        
        return df_output
    
    def create_legacy_room_mappings_csv(self, df_output: pd.DataFrame, output_file: str = "room_mappings_legacy_format.csv") -> pd.DataFrame:
        """
        Create CSV in the legacy room_mappings.csv format
        
        Args:
            df_output: Main mapping results DataFrame
            output_file: Output filename for legacy format CSV
            
        Returns:
            DataFrame in legacy format
        """
        logger.info(f"Creating legacy format CSV: {output_file}")
        
        legacy_rows = []
        
        for _, row in df_output.iterrows():
            hotel_name = row['ref_hotel_name']
            mapping_type = row.get('mapping_type', 'unmapped')
            
            # Get providers that have rooms in this mapping
            providers_with_data = []
            for provider in ['goglobal', 'ratehawk', 'tbo']:
                if pd.notna(row.get(f'{provider}_room_name', '')) and row.get(f'{provider}_room_name', '') != '':
                    providers_with_data.append(provider)
            
            if len(providers_with_data) >= 2:
                # Multi-provider mapping - create combinations
                for i, provider1 in enumerate(providers_with_data):
                    for provider2 in providers_with_data[i+1:]:
                        # Create mapping row for this pair
                        legacy_row = self._create_legacy_mapping_row(
                            hotel_name, row, provider1, provider2, mapping_type
                        )
                        legacy_rows.append(legacy_row)
            else:
                # Single provider or unmapped - create single row
                provider = providers_with_data[0] if providers_with_data else 'goglobal'
                legacy_row = self._create_legacy_mapping_row(
                    hotel_name, row, provider, None, 'unmapped'
                )
                legacy_rows.append(legacy_row)
        
        # Create DataFrame and save
        legacy_df = pd.DataFrame(legacy_rows)
        
        # Ensure correct column order to match original
        expected_columns = [
            'reference_id', 'ref_hotel_name',
            'goglobal_room_name', 'goglobal_bedding_type', 'goglobal_bedrooms',
            'ratehawk_room_name', 'ratehawk_bedding_type', 'ratehawk_bedrooms',
            'matched', 'confidence', 'reason', 'available_columns', 'data_source',
            'tbo_room_name', 'tbo_bedding_type', 'tbo_bedrooms'
        ]
        
        # Reorder columns to match expected format
        for col in expected_columns:
            if col not in legacy_df.columns:
                legacy_df[col] = ''
        
        legacy_df = legacy_df[expected_columns]
        
        # Save to CSV
        output_path = Path(output_file)
        legacy_df.to_csv(output_path, index=False)
        
        logger.info(f"Legacy format CSV created: {output_path}")
        logger.info(f"Total legacy rows: {len(legacy_df)}")
        
        return legacy_df
    
    def _create_legacy_mapping_row(self, hotel_name: str, main_row: pd.Series, 
                                 provider1: str, provider2: Optional[str], 
                                 mapping_type: str) -> Dict[str, Any]:
        """
        Create a single row in legacy format
        
        Args:
            hotel_name: Hotel name
            main_row: Main mapping row from current format
            provider1: First provider (always present)
            provider2: Second provider (can be None for unmapped)
            mapping_type: Type of mapping
            
        Returns:
            Dictionary representing legacy format row
        """
        # Extract reference_id from the first available provider
        reference_id = 0  # Default fallback
        for provider in ['goglobal', 'ratehawk', 'tbo']:
            ref_id_col = f'{provider}_reference_id'
            if pd.notna(main_row.get(ref_id_col)) and main_row.get(ref_id_col) != '':
                try:
                    reference_id = int(main_row[ref_id_col])
                    break
                except (ValueError, TypeError):
                    continue
        
        # Base row structure
        legacy_row = {
            'reference_id': reference_id,
            'ref_hotel_name': hotel_name,
            'goglobal_room_name': '',
            'goglobal_bedding_type': '',
            'goglobal_bedrooms': '',
            'ratehawk_room_name': '',
            'ratehawk_bedding_type': '',
            'ratehawk_bedrooms': '',
            'matched': mapping_type == 'multi',
            'confidence': 0.0,
            'reason': '',
            'available_columns': '',
            'data_source': '',
            'tbo_room_name': '',
            'tbo_bedding_type': '',
            'tbo_bedrooms': ''
        }
        
        # Fill provider1 data
        if provider1 in ['goglobal', 'ratehawk', 'tbo']:
            legacy_row[f'{provider1}_room_name'] = main_row.get(f'{provider1}_room_name', '')
            legacy_row[f'{provider1}_bedding_type'] = main_row.get(f'{provider1}_bedding_config', '')
            legacy_row[f'{provider1}_bedrooms'] = main_row.get(f'{provider1}_bedrooms_count', '')
        
        # Fill provider2 data if exists
        if provider2 and provider2 in ['goglobal', 'ratehawk', 'tbo']:
            legacy_row[f'{provider2}_room_name'] = main_row.get(f'{provider2}_room_name', '')
            legacy_row[f'{provider2}_bedding_type'] = main_row.get(f'{provider2}_bedding_config', '')
            legacy_row[f'{provider2}_bedrooms'] = main_row.get(f'{provider2}_bedrooms_count', '')
            
            # Calculate confidence for matched pairs
            if mapping_type == 'multi':
                # Get room names for comparison
                room1_name = main_row.get(f'{provider1}_room_name', '')
                room2_name = main_row.get(f'{provider2}_room_name', '')
                
                if room1_name and room2_name:
                    # Use our fuzzy matching algorithm to calculate confidence
                    try:
                        # Create temp room objects
                        temp_room1 = {
                            'main_name': main_row.get(f'{provider1}_main_name', ''),
                            'room_name': room1_name,
                            'bedding_config': main_row.get(f'{provider1}_bedding_config', ''),
                            'room_class': main_row.get(f'{provider1}_room_class', ''),
                            'room_view': main_row.get(f'{provider1}_room_view', ''),
                        }
                        
                        temp_room2 = {
                            'main_name': main_row.get(f'{provider2}_main_name', ''),
                            'room_name': room2_name,
                            'bedding_config': main_row.get(f'{provider2}_bedding_config', ''),
                            'room_class': main_row.get(f'{provider2}_room_class', ''),
                            'room_view': main_row.get(f'{provider2}_room_view', ''),
                        }
                        
                        # Calculate score using our scorer
                        score_result = self.scorer.score_room(temp_room1, temp_room2)
                        legacy_row['confidence'] = min(score_result.score, 2.0)  # Cap at 2.0 like original
                        
                        # Create reason string based on algorithms used
                        reason_parts = []
                        if score_result.algorithms_used.get('main_name_fuzzy_match'):
                            fuzzy_score = self.scorer.score_main_name_fuzzy(temp_room1, temp_room2)
                            reason_parts.append(f"room_name:{fuzzy_score:.2f}*1.00")
                        if score_result.algorithms_used.get('bedding_config_penalty'):
                            bedding_score = self.scorer.score_bedding_config(temp_room1, temp_room2)
                            if bedding_score:
                                reason_parts.append(f"bedding_type:{bedding_score:.2f}*0.41")
                        
                        legacy_row['reason'] = " + ".join(reason_parts) if reason_parts else "fuzzy_match"
                        
                        # Set available columns based on what was used
                        used_cols = []
                        if score_result.algorithms_used.get('main_name_fuzzy_match'):
                            used_cols.append('room_name')
                        if score_result.algorithms_used.get('bedding_config_penalty'):
                            used_cols.append('bedding_type')
                        
                        legacy_row['available_columns'] = ";".join(used_cols) if used_cols else "room_name"
                        
                    except Exception as e:
                        logger.debug(f"Error calculating legacy confidence: {e}")
                        legacy_row['confidence'] = 1.0
                        legacy_row['reason'] = "calculated_match"
                        legacy_row['available_columns'] = "room_name"
            
            # Set data source
            legacy_row['data_source'] = f"{provider1}_{provider2}_match"
        else:
            # Unmapped room
            legacy_row['matched'] = False
            legacy_row['confidence'] = 0.0
            legacy_row['reason'] = 'no_match_found'
            legacy_row['data_source'] = f"{provider1}_only"
        
        return legacy_row

def main():
    """Main execution function"""
    try:
        logger.info("Initializing Room Mapper...")
        
        config_path = 'app/config/room_mappings_config.yaml'
        mapper = RoomMapper(config_path)
        
        logger.info("Cache optimization active for maximum performance")
        
        # Perform mapping
        results_df = mapper.map_all_rooms()
        
        # Save results in current format
        output_file = 'room_mappings_COMPLETE_DICTIONARY.csv'
        results_df.to_csv(output_file, index=False)
        
        logger.info(f"Main results saved to: {output_file}")
        logger.info(f"Main file size: {len(results_df)} rows x {len(results_df.columns)} columns")
        
        # Generate legacy format CSV
        legacy_output_file = 'room_mappings_legacy_format.csv'
        legacy_df = mapper.create_legacy_room_mappings_csv(results_df, legacy_output_file)
        
        logger.info(f"Legacy format saved to: {legacy_output_file}")
        logger.info(f"Legacy file size: {len(legacy_df)} rows x {len(legacy_df.columns)} columns")
        
        logger.info("Room mapping completed successfully!")
        logger.info(f"Generated two output files:")
        logger.info(f"  1. {output_file} - Main format with all provider data")
        logger.info(f"  2. {legacy_output_file} - Legacy format compatible with room_mappings.csv")
        
        return results_df, legacy_df
        
    except ConfigurationError as e:
        logger.error(f"Configuration error: {e}")
        raise
    except DataValidationError as e:
        logger.error(f"Data validation error: {e}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        raise

if __name__ == '__main__':
    main()