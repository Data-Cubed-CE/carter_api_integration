import pandas as pd
import numpy as np
from rapidfuzz import fuzz
import re
import phonetics
from typing import Dict, List, Tuple, Optional
import warnings
import os
warnings.filterwarnings('ignore')

class ProductionHotelMatcher:
    def __init__(self, api_source: str = 'universal'):
        self.reference_hotels = None
        self.api_hotels = None
        self.api_source = api_source
        
        # Multi-API support for Direct Matching Only
        self.api_hotels_dict = {}  # Store multiple APIs
        
        # Universal components - work with ANY API (simplified)
        self.universal_brands = self._create_universal_brands()
        self.stop_words = self._create_enhanced_stop_words()
        self.premium_keywords = self._create_premium_keywords()
        
        # API-specific enhancements (optional)
        self.api_enhancements = self._load_api_enhancements(api_source)
        
    def _create_universal_brands(self) -> List[str]:
        """Universal luxury brands - work with all APIs"""
        return [
            'four seasons', 'atlantis', 'banyan tree', 'mandarin oriental',
            'one only', 'one & only', 'angsana', 'shangri-la', 'ritz carlton', 
            'ritz-carlton', 'st regis', 'waldorf astoria', 'hilton', 'marriott', 
            'hyatt', 'intercontinental', 'sheraton', 'westin', 'doubletree', 
            'regent', 'fairmont', 'raffles', 'rosewood', 'belmond', 'six senses',
            'chedi', 'dusit thani', 'movenpick', 'mövenpick', 'joali', 'conrad',
            'park hyatt', 'grand hyatt', 'andaz', 'aloft', 'w hotels', 'le meridien',
            'luxury collection', 'autograph collection', 'jw marriott'
        ]
    
    def _create_enhanced_stop_words(self) -> List[str]:
        """Enhanced stop words based on API analysis"""
        return [
            # Basic hotel types
            'hotel', 'resort', 'spa', 'suites', 'inn', 'lodge', 'motel',
            # Accommodation types
            'apartment', 'apartments', 'villa', 'house', 'studio', 'bedroom',
            # Descriptors
            'luxury', 'grand', 'royal', 'palace', 'club', 'boutique', 'deluxe',
            # Location words
            'beach', 'view', 'pool', 'marina', 'bay', 'island',
            # Common words
            'in', 'by', 'with', 'and', 'the', 'at', 'of', 'for',
            # Numbers
            '1', '2', '3', '4', '5', 'one', 'two', 'three', 'four', 'five',

            '&', '-'
        ]
    
    def _create_premium_keywords(self) -> List[str]:
        """Premium indicators that work across all APIs"""
        return [
            'luxury', 'private', 'boutique', 'grand', 'deluxe', 'royal', 
            'retreat', 'premium', 'palace', 'collection', 'signature', 
            'exclusive', 'estate', 'reserve', 'imperial',
            'beach', 'oceanfront', 'seafront', 'waterfront', 'marina', 'bay'
        ]
    
    def _load_api_enhancements(self, api_source: str) -> Dict:
        """Load API-specific enhancements if available"""
        enhancements = {
            'rate_hawk': {
                'has_chain_field': True,
                'smart_id_analysis': True,
                'confidence_boost': 0.03
            },
            'goglobal': {
                'has_chain_field': False,
                'smart_id_analysis': False,
                'confidence_boost': 0.02
            },
            'tbo': {
                'has_chain_field': False,
                'smart_id_analysis': True,
                'confidence_boost': 0.02
            },
            'universal': {
                'has_chain_field': True,
                'smart_id_analysis': True,
                'confidence_boost': 0.0
            }
        }
        return enhancements.get(api_source, enhancements['universal'])
    
    def _extract_id_components_smart(self, api_id: str, ref_hotel: pd.Series) -> Dict:
        """Smart dynamic analysis of API ID against reference hotel data"""
        if not api_id or len(str(api_id)) < 3:
            return {'confidence_boost': 0.0, 'matches': []}
        
        # Split ID into meaningful components
        id_parts = re.split(r'[_\-\s\.]+', str(api_id).lower())
        id_parts = [part.strip() for part in id_parts if len(part) > 2 and not part.isdigit()]
        
        if not id_parts:
            return {'confidence_boost': 0.0, 'matches': []}
        
        # Reference data for comparison
        ref_brand = ref_hotel.get('brand', '').lower() if ref_hotel.get('brand') else ''
        ref_city = ref_hotel.get('city', '').lower() if ref_hotel.get('city') else ''
        ref_country = ref_hotel.get('country_raw', '').lower() if ref_hotel.get('country_raw') else ''
        ref_name_words = ref_hotel.get('normalized_name', '').split() if ref_hotel.get('normalized_name') else []
        
        matches = []
        confidence_boost = 0.0
        
        # City variant mappings for international names
        city_variants = {
            'warsaw': ['warszawa', 'warsaw'],
            'prague': ['praga', 'praha', 'prague'], 
            'vienna': ['wiedeń', 'wien', 'vienna'],
            'munich': ['monachium', 'münchen', 'munich'],
            'florence': ['florencja', 'firenze', 'florence'],
            'rome': ['rzym', 'roma', 'rome'],
            'paris': ['paryż', 'paris'],
            'london': ['londyn', 'london'],
            'istanbul': ['stambul', 'istanbul'],
            'athens': ['ateny', 'athens'],
            'budapest': ['budapeszt', 'budapest'],
            'madrid': ['madryt', 'madrid'],
            'lisbon': ['lizbona', 'lisboa', 'lisbon'],
            'zurich': ['zurych', 'zurich'],
            'geneva': ['genewa', 'genève', 'geneva'],
            'dubai': ['dubaj', 'dubai'],
            'moscow': ['moskwa', 'moscow'],
            'beijing': ['pekin', 'beijing']
        }
        
        # Check each ID component against reference data
        for part in id_parts:
            if len(part) < 3:
                continue
                
            # Brand matching
            if ref_brand and len(ref_brand) > 2:
                brand_similarity = fuzz.ratio(part, ref_brand) / 100.0
                if brand_similarity > 0.85:
                    confidence_boost += 0.06
                    matches.append(f"Brand: '{part}' matches '{ref_brand}'")
                    continue
            
            # City matching (direct)
            if ref_city and len(ref_city) > 2:
                city_similarity = fuzz.ratio(part, ref_city) / 100.0
                if city_similarity > 0.80:
                    confidence_boost += 0.04
                    matches.append(f"City: '{part}' matches '{ref_city}'")
                    continue
            
            # City variant matching
            if ref_city:
                for canonical, variants in city_variants.items():
                    if part in variants and ref_city in variants:
                        confidence_boost += 0.04
                        matches.append(f"City variant: '{part}' ~ '{ref_city}'")
                        break
            
            # Hotel name word matching
            for ref_word in ref_name_words:
                if len(ref_word) > 3:
                    word_similarity = fuzz.ratio(part, ref_word) / 100.0
                    if word_similarity > 0.85:
                        confidence_boost += 0.02
                        matches.append(f"Name part: '{part}' matches '{ref_word}'")
                        break
            
            # Country matching (less common but useful)
            if ref_country and len(ref_country) > 3:
                country_similarity = fuzz.ratio(part, ref_country) / 100.0
                if country_similarity > 0.85:
                    confidence_boost += 0.03
                    matches.append(f"Country: '{part}' matches '{ref_country}'")
        
        # Cap the boost to prevent over-enhancement
        confidence_boost = min(confidence_boost, 0.15)
        
        return {
            'confidence_boost': confidence_boost,
            'matches': matches,
            'id_parts': id_parts
        }
    
    def _try_smart_api_enhancements(self, base_result: Dict, ref_hotel: pd.Series, api_hotel: pd.Series) -> Dict:
        """Smart enhancement using dynamic ID analysis"""
        if not base_result:
            return base_result
        
        api_id = api_hotel.get('id', '')
        if not api_id:
            return base_result
        
        # Perform smart analysis
        analysis = self._extract_id_components_smart(api_id, ref_hotel)
        
        if analysis['confidence_boost'] > 0.01:  # Only enhance if meaningful boost
            enhanced_result = base_result.copy()
            original_confidence = enhanced_result['confidence']
            
            enhanced_result['confidence'] = min(
                original_confidence + analysis['confidence_boost'], 
                0.98
            )
            
            # Add enhancement details to reason
            if analysis['matches']:
                enhancement_details = '; '.join(analysis['matches'][:2])  # Show top 2 matches
                enhanced_result['reason'] += f" + Smart ID Analysis: {enhancement_details}"
            
            return enhanced_result
        
        return base_result
    
    def _extract_brand_from_name(self, hotel_name: str) -> Optional[str]:
        """Universal brand extraction from hotel names"""
        if not hotel_name:
            return None
        name_lower = hotel_name.lower()
        for brand in self.universal_brands:
            if brand in name_lower:
                return brand
        return None
    
    def _normalize_hotel_name(self, name: str) -> str:
        """Universal name normalization"""
        if pd.isna(name) or not name:
            return ""
        
        name = str(name).lower().strip()
        name = re.sub(r'[^\w\s]', ' ', name)
        
        # Convert numbers to words
        number_map = {'1': 'one', '2': 'two', '3': 'three', '4': 'four', '5': 'five'}
        for digit, word in number_map.items():
            name = name.replace(f' {digit} ', f' {word} ')
        
        words = [word for word in name.split() if word not in self.stop_words and len(word) > 1]
        
        # Remove duplicates while preserving order
        seen = set()
        unique_words = []
        for word in words:
            if word not in seen:
                unique_words.append(word)
                seen.add(word)
        
        return ' '.join(unique_words)
    
    def _normalize_city_name(self, city: str) -> str:
        if not city:
            return ""
        city_mappings = {
            'dubaj': 'dubai',
            'krabi': 'krabi',
            'desroches': 'desroches island'
        }
        city_clean = city.lower().strip()
        return city_mappings.get(city_clean, city_clean)
    
    def load_reference_hotels(self, csv_path: str) -> pd.DataFrame:
        print("\n" + "="*60)
        print(" LOADING REFERENCE HOTELS")
        print("="*60)
        
        df = pd.read_csv(csv_path)
        print(f" Loaded file: {csv_path}")
        print(f" Found {len(df)} reference hotels")
        
        print("\n🧹 Processing reference data...")
        print(" Using NEW format - all data already separated!")
        
        # Use provided data directly - no parsing needed!
        df['clean_hotel'] = df['Hotel'].fillna('').str.strip()
        df['country_raw'] = df['Country'].fillna('').str.strip()
        df['city_raw'] = df['City'].fillna('').str.strip()
        
        # Use provided ISO codes directly - much simpler!
        print("🗺️ Using provided ISO codes (no mapping needed)...")
        df['country_iso'] = df['ISO'].fillna('').str.strip().str.upper()
        
        # Create Lokalizacja for backward compatibility
        df['clean_location'] = df['country_raw'] + ', ' + df['city_raw']
        df['Lokalizacja'] = df['clean_location']
        
        # Filter out empty/invalid records
        valid_records = (
            df['clean_hotel'].notna() & 
            (df['clean_hotel'].str.strip() != '') & 
            (df['clean_hotel'] != '-') &
            (df['clean_hotel'] != 'test')  # Remove test entries
        )
        df = df[valid_records].copy()
        print(f" Filtered to {len(df)} valid hotel records")
        
        # Check for empty ISO codes
        empty_iso = df['country_iso'].isna() | (df['country_iso'] == '')
        if empty_iso.any():
            empty_countries = df[empty_iso]['country_raw'].unique()
            print(f" Hotels with missing ISO codes: {len(df[empty_iso])}")
            print(f" Countries missing ISO: {list(empty_countries)}")
        
        df['city'] = df['city_raw'].apply(self._normalize_city_name)
        df['normalized_name'] = df['clean_hotel'].apply(self._normalize_hotel_name)
        df['brand'] = df['clean_hotel'].apply(self._extract_brand_from_name)
        df['reference_id'] = df.index.astype(str).str.zfill(3)
        
        country_stats = df['country_iso'].value_counts()
        print(f" Countries distribution:")
        for country, count in country_stats.head(5).items():
            print(f"   {country}: {count} hotels")
        
        brands_found = df['brand'].dropna().nunique()
        print(f" Universal brands detected: {brands_found}")
        
        self.reference_hotels = df
        print(f" Processed {len(df)} reference hotels")
        return df
    
    def load_api_hotels(self, csv_path: str, api_name: str = None) -> pd.DataFrame:
        """Universal API hotel loader with smart pre-filtering by reference countries"""
        print("\n" + "="*60)
        print(f" LOADING {api_name.upper() if api_name else 'API'} HOTELS")
        print("="*60)
        
        df = pd.read_csv(csv_path)
        print(f" Loaded file: {csv_path}")
        print(f" Found {len(df):,} total API hotels")
        
        # SMART PRE-FILTERING: Only countries from reference list
        if self.reference_hotels is not None:
            reference_countries = set(self.reference_hotels['country_iso'].dropna().unique())
            print(f" Reference countries: {len(reference_countries)} → {sorted(reference_countries)}")
            
            # Auto-detect API format and get country column
            if 'HotelID' in df.columns:  # GoGlobal format
                country_col = 'IsoCode'
                print(" Detected GoGlobal format")
            elif 'HotelCode' in df.columns:  # TBO format
                country_col = 'CountryCode'
                print(" Detected TBO format")
            else:  # Rate Hawk format
                country_col = 'country'
                print(" Detected Rate Hawk format")
            
            # Filter API hotels to only reference countries
            original_count = len(df)
            df_filtered = df[df[country_col].fillna('').str.upper().str.strip().isin(reference_countries)]
            filtered_count = len(df_filtered)
            reduction = ((original_count - filtered_count) / original_count) * 100
            
            print(f" PRE-FILTERING RESULTS:")
            print(f"    Original: {original_count:,} hotels")
            print(f"    Filtered: {filtered_count:,} hotels")
            print(f"    Reduction: {reduction:.1f}% ({original_count - filtered_count:,} hotels removed)")
            print(f"    Performance gain: ~{reduction:.0f}% faster processing")
            
            # Show country-wise filtering stats
            api_countries_before = set(df[country_col].fillna('').str.upper().str.strip().unique())
            api_countries_after = set(df_filtered[country_col].fillna('').str.upper().str.strip().unique())
            removed_countries = api_countries_before - reference_countries
            
            if removed_countries:
                print(f"    Removed countries: {sorted(list(removed_countries)[:10])}{'...' if len(removed_countries) > 10 else ''}")
            
            # Country overlap analysis
            overlap_countries = reference_countries & api_countries_before
            missing_countries = reference_countries - api_countries_before
            
            print(f"    Country overlap: {len(overlap_countries)}/{len(reference_countries)} ({len(overlap_countries)/len(reference_countries)*100:.1f}%)")
            if missing_countries:
                print(f"    Missing in API: {sorted(list(missing_countries))}")
            
            df = df_filtered
        else:
            print(" No reference hotels loaded yet - keeping all API hotels")
            print(" Load reference hotels first for optimal performance")
        
        # Auto-detect API format and standardize columns
        if 'HotelID' in df.columns:  # GoGlobal format
            print(" Mapping GoGlobal columns to standard format...")
            df_std = pd.DataFrame()
            df_std['id'] = df['HotelID'].astype(str)
            df_std['name'] = df['Name'].fillna('').str.strip()
            df_std['city'] = df['City'].fillna('').str.strip()
            df_std['country'] = df['IsoCode'].fillna('').str.upper().str.strip()
            df_std['address'] = df['Address'].fillna('').str.strip()
            df_std['latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
            df_std['longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')
            df_std['hotel_chain'] = ''  # GoGlobal doesn't have chain field
        elif 'HotelCode' in df.columns:  # TBO format
            print(" Mapping TBO columns to standard format...")
            df_std = pd.DataFrame()
            df_std['id'] = df['HotelCode'].astype(str)
            df_std['name'] = df['HotelName'].fillna('').str.strip()
            df_std['city'] = df['CityName'].fillna('').str.strip()
            df_std['country'] = df['CountryCode'].fillna('').str.upper().str.strip()
            df_std['address'] = df['Address'].fillna('').str.strip()
            
            # Parse coordinates from Map field (format: "lat|lng")
            df_std['latitude'] = pd.to_numeric(df['Map'].str.split('|').str[0], errors='coerce')
            df_std['longitude'] = pd.to_numeric(df['Map'].str.split('|').str[1], errors='coerce')
            df_std['hotel_chain'] = ''  # TBO doesn't have chain field
        else:  # Rate Hawk format (or similar)
            print("🔧 Using Rate Hawk standard columns...")
            df_std = df.copy()
            if 'hotel_chain' not in df_std.columns:
                df_std['hotel_chain'] = df_std.get('chain', '').fillna('')
        
        print("\n Processing API data...")
        df_std['clean_name'] = df_std['name'].fillna('').str.strip()
        df_std['clean_city'] = df_std['city'].fillna('').str.strip()
        df_std['clean_address'] = df_std['address'].fillna('').str.strip()
        df_std['clean_chain'] = df_std['hotel_chain'].fillna('').str.strip()
        
        df_std['country_iso'] = df_std['country'].fillna('').str.upper().str.strip()
        df_std['normalized_name'] = df_std['clean_name'].apply(self._normalize_hotel_name)
        df_std['city_normalized'] = df_std['clean_city'].apply(self._normalize_city_name)
        df_std['brand_from_name'] = df_std['clean_name'].apply(self._extract_brand_from_name)
        
        df_std['lat'] = pd.to_numeric(df_std['latitude'], errors='coerce')
        df_std['lng'] = pd.to_numeric(df_std['longitude'], errors='coerce')
        
        country_stats = df_std['country_iso'].value_counts()
        print(f" Final API countries distribution:")
        for country, count in country_stats.head(5).items():
            print(f"   {country}: {count:,} hotels")
        
        coords_valid = df_std[['lat', 'lng']].notna().all(axis=1).sum()
        print(f" Hotels with coordinates: {coords_valid:,}/{len(df_std):,} ({coords_valid/len(df_std)*100:.1f}%)")
        
        if api_name:
            self.api_hotels_dict[api_name] = df_std
            print(f" Stored {api_name} hotels: {len(df_std):,}")
        else:
            self.api_hotels = df_std
            print(f" Processed {len(df_std):,} API hotels")
        
        return df_std
    
    def _calculate_universal_features(self, ref_hotel: pd.Series, api_hotel: pd.Series) -> Dict:
        """Calculate features that work with any API"""
        ref_name = ref_hotel['normalized_name']
        api_name = api_hotel['normalized_name']
        
        if not ref_name or not api_name:
            return self._empty_features()
        
        features = {
            # String similarity (universal)
            'fuzz_ratio': fuzz.ratio(ref_name, api_name) / 100.0,
            'fuzz_partial': fuzz.partial_ratio(ref_name, api_name) / 100.0,
            'fuzz_token_sort': fuzz.token_sort_ratio(ref_name, api_name) / 100.0,
            'fuzz_token_set': fuzz.token_set_ratio(ref_name, api_name) / 100.0,
            
            # Geographic (universal)
            'country_exact': ref_hotel['country_iso'] == api_hotel['country_iso'],
            'city_similarity': self._city_similarity(ref_hotel['city'], api_hotel['city_normalized']),
            
            # Brand detection (universal)
            'brand_from_name': self._brand_match_from_names(ref_hotel, api_hotel),
            'brand_chain_match': self._brand_chain_match(ref_hotel, api_hotel),
            
            # Additional universal features
            'soundex_match': self._soundex_match(ref_name, api_name),
            'word_intersection': self._word_intersection_ratio(ref_name, api_name),
            'premium_keywords': self._premium_keywords_overlap(ref_hotel['clean_hotel'], api_hotel['clean_name'])
        }
        
        return features
    
    def _empty_features(self) -> Dict:
        return {
            'fuzz_ratio': 0.0, 'fuzz_partial': 0.0, 'fuzz_token_sort': 0.0,
            'fuzz_token_set': 0.0, 'country_exact': False, 'city_similarity': 0.0,
            'brand_from_name': False, 'brand_chain_match': False, 'soundex_match': False,
            'word_intersection': 0.0, 'premium_keywords': 0.0
        }
    
    def _city_similarity(self, city1: str, city2: str) -> float:
        if not city1 or not city2:
            return 0.0
        return fuzz.ratio(city1.lower(), city2.lower()) / 100.0
    
    def _brand_match_from_names(self, ref_hotel: pd.Series, api_hotel: pd.Series) -> bool:
        """Brand matching from hotel names (universal)"""
        ref_brand = ref_hotel.get('brand')
        api_brand = api_hotel.get('brand_from_name')
        
        if not ref_brand or not api_brand:
            return False
        
        return fuzz.ratio(ref_brand, api_brand) / 100.0 > 0.85
    
    def _brand_chain_match(self, ref_hotel: pd.Series, api_hotel: pd.Series) -> bool:
        """Brand matching using chain field (if available)"""
        ref_brand = ref_hotel.get('brand')
        api_chain = api_hotel.get('clean_chain', '').lower()
        
        if not ref_brand or not api_chain:
            return False
        
        return ref_brand.lower() in api_chain or fuzz.ratio(ref_brand, api_chain) / 100.0 > 0.80
    
    def _soundex_match(self, name1: str, name2: str) -> bool:
        if not name1 or not name2:
            return False
        try:
            soundex1 = phonetics.soundex(name1.replace(' ', ''))
            soundex2 = phonetics.soundex(name2.replace(' ', ''))
            return soundex1 == soundex2
        except:
            return False
    
    def _word_intersection_ratio(self, name1: str, name2: str) -> float:
        if not name1 or not name2:
            return 0.0
        
        words1 = set(name1.lower().split())
        words2 = set(name2.lower().split())
        
        if not words1 or not words2:
            return 0.0
        
        intersection = len(words1 & words2)
        union = len(words1 | words2)
        
        return intersection / union if union > 0 else 0.0
    
    def _premium_keywords_overlap(self, name1: str, name2: str) -> float:
        name1_lower = name1.lower()
        name2_lower = name2.lower()
        
        name1_keywords = sum(1 for kw in self.premium_keywords if kw in name1_lower)
        name2_keywords = sum(1 for kw in self.premium_keywords if kw in name2_lower)
        
        if name1_keywords == 0 and name2_keywords == 0:
            return 0.0
        
        common_keywords = sum(1 for kw in self.premium_keywords if kw in name1_lower and kw in name2_lower)
        total_keywords = max(name1_keywords, name2_keywords)
        
        return common_keywords / total_keywords if total_keywords > 0 else 0.0
    
    def _universal_perfect_match_rules(self, features: Dict) -> Optional[Dict]:
        """Perfect match rules - highest priority (99% confidence)"""
        
        # EXACT NAME MATCH (normalized) + Same Country = Perfect Match
        if features['fuzz_ratio'] >= 0.98 and features['country_exact']:
            return {
                'match': True,
                'confidence': 0.99,
                'reason': f'Perfect Name Match + Same Country ({features["fuzz_ratio"]:.3f})'
            }
        
        # NEAR-PERFECT (97%+) with location confirmation
        if (features['fuzz_token_sort'] >= 0.97 and features['country_exact'] and 
            features['city_similarity'] > 0.8):
            return {
                'match': True,
                'confidence': 0.97,
                'reason': f'Near-Perfect Name + Location Confirmed ({features["fuzz_token_sort"]:.3f})'
            }
        
        # EXACT TOKEN SET (same words, different order) + Country
        if features['fuzz_token_set'] >= 0.98 and features['country_exact']:
            return {
                'match': True,
                'confidence': 0.96,
                'reason': f'Same Words Perfect + Same Country ({features["fuzz_token_set"]:.3f})'
            }
        
        return None

    def _universal_high_confidence_rules(self, features: Dict) -> Optional[Dict]:
        """High confidence rules using only universal features"""
        
        # Brand match + country + high name similarity
        if features['brand_chain_match'] and features['country_exact'] and features['fuzz_token_sort'] > 0.85:
            return {
                'match': True,
                'confidence': 0.95,
                'reason': 'Universal Brand + Location + High Name Similarity'
            }
        
        if features['brand_from_name'] and features['country_exact'] and features['city_similarity'] > 0.8:
            return {
                'match': True,
                'confidence': 0.93,
                'reason': 'Universal Brand Match + Perfect Location'
            }
        
        # Very high name similarity + country
        if features['fuzz_token_sort'] > 0.92 and features['country_exact']:
            return {
                'match': True,
                'confidence': 0.90,
                'reason': f'Universal Name Almost Identical + Same Country ({features["fuzz_token_sort"]:.2f})'
            }
        
        # Token set high + location
        if features['fuzz_token_set'] > 0.90 and features['country_exact'] and features['city_similarity'] > 0.7:
            return {
                'match': True,
                'confidence': 0.88,
                'reason': f'Universal Same Words Different Order + Location ({features["fuzz_token_set"]:.2f})'
            }
        
        return None
    
    def _universal_medium_confidence_rules(self, features: Dict) -> Optional[Dict]:
        """Medium confidence rules using universal features"""
        
        # Brand similarity + good name match
        if (features['brand_from_name'] or features['brand_chain_match']) and features['fuzz_token_sort'] > 0.75 and features['country_exact']:
            confidence = 0.75 + (features['fuzz_token_sort'] - 0.75) * 0.4
            return {
                'match': True,
                'confidence': min(confidence, 0.87),
                'reason': f'Universal Brand + Strong Name Similarity ({features["fuzz_token_sort"]:.2f})'
            }
        
        # High partial ratio + context + ENHANCED GEOGRAPHIC VALIDATION
        if (features['fuzz_partial'] > 0.85 and features['country_exact'] and 
            features['premium_keywords'] > 0.3 and features['city_similarity'] > 0.6):
            return {
                'match': True,
                'confidence': 0.77,
                'reason': f'Universal Name Contains + Geographic + Premium ({features["fuzz_partial"]:.2f})'
            }
        
        # Token set + location
        if features['fuzz_token_set'] > 0.80 and features['country_exact'] and features['city_similarity'] > 0.6:
            confidence = 0.65 + (features['fuzz_token_set'] - 0.80) * 0.5
            return {
                'match': True,
                'confidence': min(confidence, 0.83),
                'reason': f'Universal Word Shuffle + Location ({features["fuzz_token_set"]:.2f})'
            }
        
        return None
    
    def _universal_lower_confidence_rules(self, features: Dict) -> Optional[Dict]:
        """Lower confidence rules using universal features"""
        
        # Phonetic + partial match
        if features['soundex_match'] and features['fuzz_partial'] > 0.75 and features['country_exact']:
            return {
                'match': True,
                'confidence': 0.62,
                'reason': f'Universal Phonetic + Partial Match ({features["fuzz_partial"]:.2f})'
            }
        
        # Word intersection + premium + location
        if (features['word_intersection'] > 0.6 and features['premium_keywords'] > 0.4 and 
            features['country_exact'] and features['city_similarity'] > 0.5):
            return {
                'match': True,
                'confidence': 0.58,
                'reason': f'Universal Word Intersection + Premium + Location ({features["word_intersection"]:.2f})'
            }
        
        return None
    
    def _universal_matching_decision(self, ref_hotel: pd.Series, api_hotel: pd.Series) -> Optional[Dict]:
        """Core universal matching - works with any API"""
        features = self._calculate_universal_features(ref_hotel, api_hotel)
        
        # PERFECT MATCH - highest priority (99% confidence)
        result = self._universal_perfect_match_rules(features)
        if result:
            return result
        
        # Try high confidence rules
        result = self._universal_high_confidence_rules(features)
        if result:
            return result
        
        # Try medium confidence rules
        result = self._universal_medium_confidence_rules(features)
        if result:
            return result
        
        # Try lower confidence rules
        result = self._universal_lower_confidence_rules(features)
        if result:
            return result
        
        return None
    
    def _universal_name_only_decision(self, ref_hotel: pd.Series, api_hotel: pd.Series) -> Optional[Dict]:
        """Universal name-only matching for fallback"""
        ref_name = ref_hotel['normalized_name']
        api_name = api_hotel['normalized_name']
        
        if not ref_name or not api_name:
            return None
        
        # Check brand match first
        ref_brand = ref_hotel.get('brand')
        api_brand = api_hotel.get('brand_from_name')
        brand_match = ref_brand and api_brand and fuzz.ratio(ref_brand, api_brand) / 100.0 > 0.85
        
        if brand_match:
            name_sim = fuzz.token_sort_ratio(ref_name, api_name) / 100.0
            confidence = 0.70 + name_sim * 0.15
            return {
                'match': True,
                'confidence': min(confidence, 0.88),
                'reason': f'Universal Brand + Name Similarity (Fallback) ({name_sim:.2f})'
            }
        
        # Regular name-only matching
        fuzz_token_sort = fuzz.token_sort_ratio(ref_name, api_name) / 100.0
        fuzz_token_set = fuzz.token_set_ratio(ref_name, api_name) / 100.0
        
        if fuzz_token_sort > 0.88:
            return {
                'match': True,
                'confidence': 0.75,
                'reason': f'Universal Names Almost Identical (Fallback) ({fuzz_token_sort:.2f})'
            }
        
        if fuzz_token_set > 0.85:
            return {
                'match': True,
                'confidence': 0.72,
                'reason': f'Universal Same Words Different Order (Fallback) ({fuzz_token_set:.2f})'
            }
        
        return None
    
    def run_single_api_matching(self, api_name: str) -> List[Dict]:
        """Run matching for a single API against reference"""
        print(f"\n MATCHING: {api_name.upper()} → REFERENCE")
        print("="*60)
        
        if api_name not in self.api_hotels_dict:
            print(f" Error: {api_name} not loaded")
            return []
        
        # Set current API for processing
        self.api_hotels = self.api_hotels_dict[api_name]
        self.api_source = api_name
        
        matches = []
        api_by_country = self.api_hotels.groupby('country_iso')
        total_hotels = len(self.reference_hotels)
        
        print(f" Processing {total_hotels} reference hotels against {len(self.api_hotels):,} {api_name} hotels")
        
        total_comparisons = 0
        enhanced_matches = 0
        
        for idx, (_, ref_hotel) in enumerate(self.reference_hotels.iterrows(), 1):
            ref_iso = ref_hotel['country_iso']
            ref_name = ref_hotel['clean_hotel']
            
            if idx % 10 == 0:
                print(f"[{idx:2d}/{total_hotels}] Processing: {ref_name[:50]}...")
            
            best_match = None
            best_confidence = 0.0
            
            # Strategy 1: Universal ISO-based matching
            if ref_iso and ref_iso in api_by_country.groups:
                candidates = api_by_country.get_group(ref_iso)
                
                for _, api_hotel in candidates.iterrows():
                    total_comparisons += 1
                    
                    # Universal core matching
                    result = self._universal_matching_decision(ref_hotel, api_hotel)
                    
                    if result and result['confidence'] > best_confidence:
                        best_confidence = result['confidence']
                        best_match = {
                            'reference_id': ref_hotel['reference_id'],
                            'reference_name': ref_hotel['clean_hotel'],
                            'api_id': str(api_hotel.get('id', '')),
                            'api_name': api_hotel['clean_name'],
                            'api_chain': api_hotel['clean_chain'],
                            'api_city': api_hotel['clean_city'],
                            'api_address': api_hotel['clean_address'],
                            'api_latitude': api_hotel.get('lat'),
                            'api_longitude': api_hotel.get('lng'),
                            'confidence': result['confidence'],
                            'match_reason': result['reason'],
                            'match_strategy': 'Universal ISO-based',
                            'api_country_iso': api_hotel['country_iso']
                        }
                        
                        # Try smart API enhancements
                        if self.api_enhancements.get('smart_id_analysis', False):
                            enhanced_result = self._try_smart_api_enhancements(result, ref_hotel, api_hotel)
                            if enhanced_result['confidence'] > result['confidence']:
                                best_match['confidence'] = enhanced_result['confidence']
                                best_match['match_reason'] = enhanced_result['reason']
                                enhanced_matches += 1
            
            # Strategy 2: Universal name-only fallback
            if not best_match or best_confidence < 0.75:
                # Use top 3000 hotels by similarity for performance
                name_similarities = []
                for _, api_hotel in self.api_hotels.iterrows():
                    if api_hotel['normalized_name']:
                        sim = fuzz.ratio(ref_hotel['normalized_name'], api_hotel['normalized_name']) / 100.0
                        if sim > 0.4:
                            name_similarities.append((sim, api_hotel))
                
                name_similarities.sort(key=lambda x: x[0], reverse=True)
                top_candidates = name_similarities[:3000]
                
                for sim_score, api_hotel in top_candidates:
                    total_comparisons += 1
                    
                    result = self._universal_name_only_decision(ref_hotel, api_hotel)
                    
                    if result and result['confidence'] > best_confidence:
                        best_confidence = result['confidence']
                        best_match = {
                            'reference_id': ref_hotel['reference_id'],
                            'reference_name': ref_hotel['clean_hotel'],
                            'api_id': str(api_hotel.get('id', '')),
                            'api_name': api_hotel['clean_name'],
                            'api_chain': api_hotel['clean_chain'],
                            'api_city': api_hotel['clean_city'],
                            'api_address': api_hotel['clean_address'],
                            'api_latitude': api_hotel.get('lat'),
                            'api_longitude': api_hotel.get('lng'),
                            'confidence': result['confidence'],
                            'match_reason': result['reason'],
                            'match_strategy': 'Universal Name-only Fallback',
                            'api_country_iso': api_hotel['country_iso']
                        }
                        
                        # Try smart enhancements on fallback too
                        if self.api_enhancements.get('smart_id_analysis', False):
                            enhanced_result = self._try_smart_api_enhancements(result, ref_hotel, api_hotel)
                            if enhanced_result['confidence'] > result['confidence']:
                                best_match['confidence'] = enhanced_result['confidence']
                                best_match['match_reason'] = enhanced_result['reason']
                                enhanced_matches += 1
            
            # Record result
            if best_match and best_confidence >= 0.55:
                matches.append(best_match)
        
        print(f"\n {api_name.upper()} MATCHING COMPLETED")
        print(f"    Total matches: {len(matches)}")
        print(f"    Coverage: {len(matches)}/{total_hotels} ({len(matches)/total_hotels*100:.1f}%)")
        print(f"    Smart enhancements: {enhanced_matches}")
        print(f"    Total comparisons: {total_comparisons:,}")
        
        return matches
    
    def create_master_results_table(self, all_api_results: Dict) -> pd.DataFrame:
        """Create master table with all API results"""
        print("\n CREATING MASTER RESULTS TABLE")
        print("="*60)
        
        # Start with reference data
        master_df = self.reference_hotels[['reference_id', 'Hotel', 'Lokalizacja', 'country_iso', 'city', 'brand']].copy()
        master_df = master_df.rename(columns={
            'Hotel': 'ref_hotel_name',
            'Lokalizacja': 'ref_location',
            'country_iso': 'ref_country_iso',
            'city': 'ref_city',
            'brand': 'ref_brand'
        })
        
        # Add columns for each API
        api_names = list(all_api_results.keys())
        
        for api_name in api_names:
            matches = all_api_results[api_name]
            match_dict = {match['reference_id']: match for match in matches}
            
            # Add API-specific columns
            master_df[f'{api_name}_matched'] = master_df['reference_id'].map(
                lambda x: match_dict.get(x) is not None
            )
            master_df[f'{api_name}_hotel_id'] = master_df['reference_id'].map(
                lambda x: match_dict.get(x, {}).get('api_id', '')
            )
            master_df[f'{api_name}_hotel_name'] = master_df['reference_id'].map(
                lambda x: match_dict.get(x, {}).get('api_name', '')
            )
            master_df[f'{api_name}_city'] = master_df['reference_id'].map(
                lambda x: match_dict.get(x, {}).get('api_city', '')
            )
            master_df[f'{api_name}_country_iso'] = master_df['reference_id'].map(
                lambda x: match_dict.get(x, {}).get('api_country_iso', '')
            )
            master_df[f'{api_name}_chain'] = master_df['reference_id'].map(
                lambda x: match_dict.get(x, {}).get('api_chain', '')
            )
            master_df[f'{api_name}_confidence'] = master_df['reference_id'].map(
                lambda x: match_dict.get(x, {}).get('confidence', '')
            )
            master_df[f'{api_name}_match_quality'] = master_df['reference_id'].map(
                lambda x: match_dict.get(x, {}).get('match_strategy', '')
            )
            master_df[f'{api_name}_country_valid'] = master_df.apply(
                lambda row: row['ref_country_iso'] == row[f'{api_name}_country_iso'] 
                if row[f'{api_name}_country_iso'] else False, axis=1
            )
        
        # Add summary columns
        master_df['total_apis_matched'] = sum(master_df[f'{api}_matched'] for api in api_names)
        master_df['matched_in_all_apis'] = master_df['total_apis_matched'] == len(api_names)
        
        # Add cross-validation status
        def get_cross_validation_status(row):
            matched_apis = [api for api in api_names if row[f'{api}_matched']]
            if len(matched_apis) <= 1:
                return 'single_api' if len(matched_apis) == 1 else 'unmatched'
            
            # Check if hotel names are similar across APIs
            hotel_names = [row[f'{api}_hotel_name'] for api in matched_apis if row[f'{api}_hotel_name']]
            if len(hotel_names) >= 2:
                similarity = fuzz.ratio(hotel_names[0], hotel_names[1]) / 100.0
                return 'cross_validated' if similarity > 0.7 else 'conflicted'
            return 'partial'
        
        master_df['cross_validation_status'] = master_df.apply(get_cross_validation_status, axis=1)
        
        # Add country match validation for each API
        for api_name in api_names:
            master_df[f'{api_name}_country_match'] = master_df[f'{api_name}_country_valid']
        
        # Find best match across all APIs
        def get_best_match(row):
            best_api = ''
            best_confidence = 0.0
            best_name = ''
            
            for api in api_names:
                if row[f'{api}_matched'] and row[f'{api}_confidence']:
                    conf = float(row[f'{api}_confidence'])
                    if conf > best_confidence:
                        best_confidence = conf
                        best_api = api
                        best_name = row[f'{api}_hotel_name']
            
            return pd.Series([best_api, best_confidence, best_name])
        
        master_df[['best_match_api', 'best_match_confidence', 'best_match_hotel_name']] = master_df.apply(
            get_best_match, axis=1
        )
        
        print(f" Master table created with {len(master_df)} rows and {len(master_df.columns)} columns")
        print(f" APIs included: {', '.join(api_names)}")
        
        return master_df
    
    def save_master_results(self, all_api_results: Dict, output_file: str = "master_hotel_mapping_results.csv"):
        """Save master results with all APIs"""
        print("\n SAVING MASTER RESULTS")
        print("="*60)
        
        # Create master table
        master_df = self.create_master_results_table(all_api_results)
        
        # Save master file
        master_df.to_csv(output_file, index=False, encoding='utf-8-sig')
        print(f" Saved master results: {output_file}")
        
        # Create and save summary statistics
        api_names = list(all_api_results.keys())
        total_reference = len(master_df)
        
        summary_stats = []
        
        # General stats
        summary_stats.extend([
            {'metric': 'total_reference_hotels', 'value': total_reference},
            {'metric': 'total_apis_processed', 'value': len(api_names)}
        ])
        
        # Per-API stats
        for api_name in api_names:
            matches = master_df[f'{api_name}_matched'].sum()
            coverage = (matches / total_reference) * 100
            
            summary_stats.extend([
                {'metric': f'{api_name}_matches', 'value': matches},
                {'metric': f'{api_name}_coverage_pct', 'value': f'{coverage:.1f}%'}
            ])
            
            # Country validation stats
            if f'{api_name}_country_valid' in master_df.columns:
                country_valid = master_df[f'{api_name}_country_valid'].sum()
                country_validation_rate = (country_valid / matches * 100) if matches > 0 else 0
                summary_stats.append({
                    'metric': f'{api_name}_country_validation_pct',
                    'value': f'{country_validation_rate:.1f}%'
                })
        
        # Cross-API stats
        matched_in_all = master_df['matched_in_all_apis'].sum()
        matched_in_any = (master_df['total_apis_matched'] > 0).sum()
        
        summary_stats.extend([
            {'metric': 'matched_in_all_apis', 'value': matched_in_all},
            {'metric': 'matched_in_any_api', 'value': matched_in_any},
            {'metric': 'coverage_any_api_pct', 'value': f'{(matched_in_any/total_reference)*100:.1f}%'}
        ])
        
        summary_df = pd.DataFrame(summary_stats)
        summary_file = output_file.replace('.csv', '_summary.csv')
        summary_df.to_csv(summary_file, index=False, encoding='utf-8-sig')
        print(f" Saved summary statistics: {summary_file}")
        
        # Print summary
        print(f"\n MASTER RESULTS SUMMARY:")
        print(f"    Total reference hotels: {total_reference}")
        
        for api_name in api_names:
            matches = master_df[f'{api_name}_matched'].sum()
            coverage = (matches / total_reference) * 100
            print(f"    {api_name.upper()}: {matches}/{total_reference} ({coverage:.1f}%)")
        
        print(f"    Matched in ALL APIs: {matched_in_all}")
        print(f"    Matched in ANY API: {matched_in_any} ({(matched_in_any/total_reference)*100:.1f}%)")
        
        return {
            'master_file': output_file,
            'summary_file': summary_file,
            'total_reference': total_reference,
            'api_coverage': {api: (master_df[f'{api}_matched'].sum() / total_reference) * 100 for api in api_names},
            'matched_in_all': matched_in_all,
            'matched_in_any': matched_in_any
        }
    
    def run_multi_api_matching(self, reference_csv: str, api_csvs: Dict[str, str]) -> Dict:
        """Run complete multi-API matching (supports any number of APIs)"""
        api_count = len(api_csvs)
        api_type = "SINGLE" if api_count == 1 else "DUAL" if api_count == 2 else "TRIPLE" if api_count == 3 else "MULTI"
        
        print( "="*58 )
        print("DIRECT MATCHING ONLY")
        print("="*58)
        print(f"APIs: {list(api_csvs.keys())}")
        print("Architecture: Universal Core + API Enhancements (No Cross-API)")
        
        # Step 1: Load reference hotels
        self.load_reference_hotels(reference_csv)
        if self.reference_hotels is None:
            return {'error': 'Failed to load reference hotels'}
        
        # Step 2: Load all API hotels
        for api_name, csv_path in api_csvs.items():
            self.load_api_hotels(csv_path, api_name)
        
        # Step 3: Run direct matching for each API
        print(f"\n{'='*60}")
        print(" DIRECT MATCHING PHASE")
        print("="*60)
        
        all_api_results = {}
        
        for api_name in api_csvs.keys():
            matches = self.run_single_api_matching(api_name)
            all_api_results[api_name] = matches
        
        # Step 4: Create and save master results
        master_results = self.save_master_results(all_api_results)
        
        print(f"\n {api_type} API MATCHING COMPLETED!")
        print(f" Total APIs processed: {len(api_csvs)}")
        print(f" Master file: {master_results['master_file']}")
        
        return {
            'api_results': all_api_results,
            'master_results': master_results,
            'reference_hotels_count': len(self.reference_hotels)
        }

if __name__ == "__main__":
    # TRIPLE API MATCHING - Rate Hawk + GoGlobal + TBO
    print(" STARTING PRODUCTION TRIPLE API MATCHING ")
    
    # Initialize matcher
    matcher = ProductionHotelMatcher(api_source='universal')
    
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    
    # Define API sources with proper paths
    api_sources = {
        'rate_hawk': os.path.join(script_dir, "01_api_rate_hawk_hotels.csv"),
        'goglobal': os.path.join(script_dir, "02_api_goglobal_hotels.csv"),
        'tbo': os.path.join(script_dir, "03_api_tbo_hotels.csv")
    }
    
    # Run Triple API Matching
    results = matcher.run_multi_api_matching(
        reference_csv=os.path.join(script_dir, "00_api_ref_hotels1.csv"),
        api_csvs=api_sources
    )
    

    print(" PRODUCTION MATCHING COMPLETED! ")

    
    # Final summary
    master_results = results['master_results']
    api_count = len(api_sources)
    
    print(f"\n FINAL RESULTS:")
    print(f"    Reference hotels: {results['reference_hotels_count']}")
    
    for api_name, coverage in master_results['api_coverage'].items():
        print(f"    {api_name.upper()}: {coverage:.1f}% coverage")
    
    print(f"    Combined coverage: {(master_results['matched_in_any']/results['reference_hotels_count'])*100:.1f}%")
    
    if api_count == 2:
        print(f"    Both APIs matched: {master_results['matched_in_all']}")
    elif api_count == 3:
        print(f"    All three APIs matched: {master_results['matched_in_all']}")
    else:
        print(f"    All {api_count} APIs matched: {master_results['matched_in_all']}")
    
    print(f"\n Output files:")
    print(f"    {master_results['master_file']}")
    print(f"    {master_results['summary_file']}")