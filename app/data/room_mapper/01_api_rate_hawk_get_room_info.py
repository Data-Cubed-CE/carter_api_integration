import pandas as pd
import requests
import json
import time
import os
import base64
from datetime import datetime
from typing import List, Dict, Optional
import logging

# Import your existing config
import sys
sys.path.append('./app')
from config import Config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hotel_rooms_extraction.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class HotelRoomsExtractor:
    def __init__(self):
        # Get credentials from config
        self.config = Config.get_provider_config('rate_hawk')
        if not self.config:
            raise ValueError("RateHawk configuration not found")
            
        self.credentials = Config.get_provider_credentials('rate_hawk')
        if not self.credentials:
            raise ValueError("RateHawk credentials not found or incomplete")
            
        self.base_url = 'https://api.worldota.net/api/b2b/v3/hotel/info/'
        self.session = requests.Session()
        
        # Setup authentication based on config
        if self.credentials['auth_type'] == 'basic':
            # Create basic auth header
            username = self.credentials['username']
            password = self.credentials['password']
            auth_string = f"{username}:{password}"
            encoded_auth = base64.b64encode(auth_string.encode()).decode()
            
            self.session.headers.update({
                'Content-Type': 'application/json',
                'Authorization': f'Basic {encoded_auth}'
            })
        else:
            raise ValueError(f"Unsupported auth type: {self.credentials['auth_type']}")
    
    # Mapping functions according to RateHawk documentation
    def map_room_class(self, class_id: int) -> str:
        """Map room class ID to descriptive name"""
        class_mapping = {
            0: "run of house",
            1: "dorm", 
            2: "capsule",
            3: "room",
            4: "junior suite",
            5: "suite",
            6: "apartment",
            7: "studio",
            8: "villa",
            9: "cottage",
            17: "bungalow",
            18: "chalet",
            19: "camping",
            20: "tent"
        }
        return class_mapping.get(class_id, f"unknown_{class_id}")
    
    def map_room_quality(self, quality_id: int) -> str:
        """Map room quality ID to descriptive name"""
        quality_mapping = {
            0: "undefined",
            1: "economy",
            2: "standard", 
            3: "comfort",
            4: "business",
            5: "superior",
            6: "deluxe",
            7: "premier",
            8: "executive",
            9: "presidential",
            17: "premium",
            18: "classic",
            19: "ambassador",
            20: "grand",
            21: "luxury",
            22: "platinum",
            23: "prestige",
            24: "privilege",
            25: "royal"
        }
        return quality_mapping.get(quality_id, f"unknown_{quality_id}")
    
    def map_capacity(self, capacity_id: int) -> str:
        """Map capacity ID to descriptive name"""
        capacity_mapping = {
            0: "undefined",
            1: "single",
            2: "double",
            3: "triple", 
            4: "quadruple",
            5: "quintuple",
            6: "sextuple"
        }
        return capacity_mapping.get(capacity_id, f"unknown_{capacity_id}")
    
    def map_bathroom_type(self, bathroom_id: int) -> str:
        """Map bathroom type ID to descriptive name"""
        bathroom_mapping = {
            0: "undefined",
            1: "shared bathroom",
            2: "private bathroom",
            3: "external private bathroom"
        }
        return bathroom_mapping.get(bathroom_id, f"unknown_{bathroom_id}")
    
    def map_bedding_config(self, bedding_id: int) -> str:
        """Map bedding config ID to descriptive name"""
        bedding_mapping = {
            0: "undefined",
            1: "bunk bed",
            2: "single bed",
            3: "double",
            4: "twin",
            7: "multiple"
        }
        return bedding_mapping.get(bedding_id, f"unknown_{bedding_id}")
    
    def map_sex_restriction(self, sex_id: int) -> str:
        """Map sex restriction ID to descriptive name"""
        sex_mapping = {
            0: "undefined",
            1: "male",
            2: "female",
            3: "mixed"
        }
        return sex_mapping.get(sex_id, f"unknown_{sex_id}")
    
    def map_bedrooms_count(self, bedrooms_id: int) -> str:
        """Map bedrooms count ID to descriptive name"""
        bedrooms_mapping = {
            0: "undefined",
            1: "1 bedroom",
            2: "2 bedrooms",
            3: "3 bedrooms",
            4: "4 bedrooms",
            5: "5 bedrooms",
            6: "6 bedrooms"
        }
        return bedrooms_mapping.get(bedrooms_id, f"unknown_{bedrooms_id}")
    
    def map_room_view(self, view_id: int) -> str:
        """Map room view ID to descriptive name"""
        view_mapping = {
            0: "undefined", 1: "bay view", 2: "bosphorus view", 3: "burj-khalifa view",
            4: "canal view", 5: "city view", 6: "courtyard view", 7: "dubai-marina view",
            8: "garden view", 9: "golf view", 17: "harbour view", 18: "inland view",
            19: "kremlin view", 20: "lake view", 21: "land view", 22: "mountain view",
            23: "ocean view", 24: "panoramic view", 25: "park view", 26: "partial-ocean view",
            27: "partial-sea view", 28: "partial view", 29: "pool view", 30: "river view",
            31: "sea view", 32: "sheikh-zayed view", 33: "street view", 34: "sunrise view",
            35: "sunset view", 36: "water view", 37: "with view", 38: "beachfront",
            39: "ocean front", 40: "sea front"
        }
        return view_mapping.get(view_id, f"unknown_{view_id}")
    
    def map_room_floor(self, floor_id: int) -> str:
        """Map room floor ID to descriptive name"""
        floor_mapping = {
            0: "undefined",
            1: "penthouse floor",
            2: "duplex floor", 
            3: "basement floor",
            4: "attic floor"
        }
        return floor_mapping.get(floor_id, f"unknown_{floor_id}")
    
    def _safe_join_amenities(self, amenities) -> str:
        """Safely join amenities handling different data types"""
        try:
            if amenities is None:
                return ''
            elif isinstance(amenities, list):
                # Filter out None values and convert to strings
                clean_amenities = [str(item) for item in amenities if item is not None]
                return ', '.join(clean_amenities)
            elif isinstance(amenities, str):
                return amenities
            else:
                # Handle other types by converting to string
                return str(amenities) if amenities else ''
        except Exception as e:
            logger.warning(f"Error processing amenities {amenities}: {e}")
            return ''
    
    def _safe_count_amenities(self, amenities) -> int:
        """Safely count amenities handling different data types"""
        try:
            if amenities is None:
                return 0
            elif isinstance(amenities, list):
                # Count non-None items
                return len([item for item in amenities if item is not None])
            elif isinstance(amenities, str):
                return 1 if amenities else 0
            else:
                return 1 if amenities else 0
        except Exception as e:
            logger.warning(f"Error counting amenities {amenities}: {e}")
            return 0
    
    def load_hotel_mappings(self, csv_path: str) -> pd.DataFrame:
        """Load hotel mappings from CSV file"""
        try:
            # Ensure reference_id is always read as string to preserve leading zeros
            df = pd.read_csv(csv_path, dtype={'reference_id': 'str'})
            logger.info(f"Loaded {len(df)} hotel mappings from {csv_path}")
            return df
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            raise
    
    def filter_hotels_by_api(self, df: pd.DataFrame, api_source: str = 'rate_hawk') -> pd.DataFrame:
        """Filter hotels that have matches in specified API"""
        if api_source == 'rate_hawk':
            filtered = df[
                (df['rate_hawk_matched'] == True) & 
                (df['rate_hawk_hotel_id'].notna()) & 
                (df['rate_hawk_hotel_id'] != '')
            ].copy()
            logger.info(f"Found {len(filtered)} hotels matched in RateHawk")
        elif api_source == 'goglobal':
            filtered = df[
                (df['goglobal_matched'] == True) & 
                (df['goglobal_hotel_id'].notna()) & 
                (df['goglobal_hotel_id'] != '')
            ].copy()
            logger.info(f"Found {len(filtered)} hotels matched in GoGlobal")
        else:
            raise ValueError(f"Unsupported API source: {api_source}")
        
        return filtered
    
    def fetch_hotel_rooms(self, hotel_id: str) -> Optional[Dict]:
        """Fetch hotel room data from RateHawk API"""
        payload = {
            "id": hotel_id,
            "language": "en"
        }
        
        try:
            logger.info(f"Making request to {self.base_url} for hotel {hotel_id}")
            response = self.session.post(self.base_url, json=payload, timeout=30)
            
            logger.info(f"Response status: {response.status_code}")
            
            response.raise_for_status()
            
            data = response.json()
            
            # Debug: Log full response structure
            logger.info(f"API response structure for hotel {hotel_id}: status={data.get('status')}, has_data={bool(data.get('data'))}")
            
            if data.get('status') == 'ok' and data.get('data'):
                hotel_data = data['data']
                room_groups_count = len(hotel_data.get('room_groups', []))
                logger.info(f"Hotel {hotel_id}: Found {room_groups_count} room groups")
                return hotel_data
            else:
                error_msg = data.get('error', 'Unknown error')
                logger.warning(f"API returned error for hotel {hotel_id}: status={data.get('status')}, error={error_msg}")
                
                # Debug: Log full response when no data
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(f"Full API response for hotel {hotel_id}: {json.dumps(data, indent=2)}")
                
                return None
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error fetching data for hotel {hotel_id}: {e}")
            return None
        except json.JSONDecodeError as e:
            logger.error(f"Error parsing JSON for hotel {hotel_id}: {e}")
            return None
    
    def normalize_room_data(self, hotel_data: Dict, hotel_id: str, hotel_name: str, reference_id: str, ref_hotel_name: str) -> List[Dict]:
        """Normalize hotel room data to simplified structure with essential fields only"""
        rooms_data = []
        extracted_at = datetime.now().isoformat()
        
        room_groups = hotel_data.get('room_groups', [])
        
        for room in room_groups:
            # Skip if room is None or empty
            if not room or not isinstance(room, dict):
                logger.warning(f"Skipping invalid room data: {type(room)} - {room}")
                continue
                
            # Extract nested data safely
            name_struct = room.get('name_struct', {})
            rg_ext = room.get('rg_ext', {})
            
            # Get raw values for mapping
            class_id = rg_ext.get('class', 0)
            quality_id = rg_ext.get('quality', 0)
            capacity_id = rg_ext.get('capacity', 0)
            bathroom_id = rg_ext.get('bathroom', 0)
            bedding_id = rg_ext.get('bedding', 0)
            sex_id = rg_ext.get('sex', 0)
            bedrooms_id = rg_ext.get('bedrooms', 0)
            view_id = rg_ext.get('view', 0)
            floor_id = rg_ext.get('floor', 0)
            family_id = rg_ext.get('family', 0)
            club_id = rg_ext.get('club', 0)
            balcony_id = rg_ext.get('balcony', 0)
            
            room_data = {
                # Reference hotel data
                'reference_id': reference_id,
                'ref_hotel_name': ref_hotel_name,
                
                # API hotel data
                'hotel_id': hotel_id,
                'hotel_name': hotel_name,
                
                # Room basic data
                #'room_group_id': room.get('room_group_id', 0),
                'room_name': room.get('name', ''),
                'main_name': name_struct.get('main_name', ''),
                'bedding_type': name_struct.get('bedding_type', ''),
                'bathroom_info': name_struct.get('bathroom', ''),
                
                # Room classification with both ID and mapped values
                #'room_class_id': class_id,
                'room_class': self.map_room_class(class_id),
                #'room_quality_id': quality_id,
                'room_quality': self.map_room_quality(quality_id),
                'room_capacity': capacity_id,
                #'room_capacity': self.map_capacity(capacity_id),
                'bedrooms_count': bedrooms_id,
                #'bedrooms_count': self.map_bedrooms_count(bedrooms_id),
                #'bathroom_type_id': bathroom_id,
                'bathroom_type': self.map_bathroom_type(bathroom_id),
                #'bedding_config_id': bedding_id,
                'bedding_config': self.map_bedding_config(bedding_id),
                #'sex_restriction_id': sex_id,
                #'sex_restriction': self.map_sex_restriction(sex_id),
                'family_room': family_id,
                #'family_room': 'family' if family_id == 1 else 'not family',
                'club_room': club_id,
                #'club_room': 'club' if club_id == 1 else 'not club',
                'balcony': balcony_id,
                #'balcony': 'balcony' if balcony_id == 1 else 'no balcony',
                #'room_view_id': view_id,
                'room_view': self.map_room_view(view_id),
                #'room_floor_id': floor_id,
                #'room_floor': self.map_room_floor(floor_id),
                
                # Room amenities - handle different data types safely
                'room_amenities': self._safe_join_amenities(room.get('room_amenities', [])),
                'room_amenities_count': self._safe_count_amenities(room.get('room_amenities', [])),
                
                # Metadata
                'api_source': 'rate_hawk',
                'extracted_at': extracted_at
            }
            
            rooms_data.append(room_data)
        
        logger.info(f"Processed {len(rooms_data)} valid rooms out of {len(room_groups)} room groups for hotel {hotel_id}")
        return rooms_data
    
    def process_all_hotels(self, hotels_df: pd.DataFrame, batch_size: int = 5, delay: float = 2.0) -> List[Dict]:
        """Process ALL hotels iteratively with progress tracking and error recovery"""
        all_rooms_data = []
        total_hotels = len(hotels_df)
        processed_count = 0
        success_count = 0
        error_count = 0
        
        print(f"\n Starting extraction for {total_hotels} hotels...")
        print(f" Batch size: {batch_size}, Delay between batches: {delay}s")
        print("=" * 60)
        
        for i in range(0, total_hotels, batch_size):
            batch_end = min(i + batch_size, total_hotels)
            batch = hotels_df.iloc[i:batch_end]
            batch_num = i // batch_size + 1
            total_batches = (total_hotels - 1) // batch_size + 1
            
            print(f"\n Batch {batch_num}/{total_batches} (Hotels {i+1}-{batch_end})")
            logger.info(f"Processing batch {batch_num}/{total_batches} (hotels {i+1}-{batch_end} of {total_hotels})")
            
            batch_success = 0
            batch_errors = 0
            
            for idx, row in batch.iterrows():
                hotel_id = row['rate_hawk_hotel_id']
                hotel_name = row['rate_hawk_hotel_name']
                reference_id = row['reference_id']
                ref_hotel_name = row['ref_hotel_name']
                processed_count += 1
                
                print(f"   [{processed_count}/{total_hotels}] {reference_id} | {hotel_id} - {hotel_name[:50]}...")
                logger.info(f"Fetching rooms for hotel {hotel_id} ({hotel_name}) [Reference: {reference_id}]")
                
                try:
                    hotel_data = self.fetch_hotel_rooms(hotel_id)
                    if hotel_data and isinstance(hotel_data, dict):
                        rooms_data = self.normalize_room_data(hotel_data, hotel_id, hotel_name, reference_id, ref_hotel_name)
                        all_rooms_data.extend(rooms_data)
                        batch_success += 1
                        success_count += 1
                        print(f"     Extracted {len(rooms_data)} rooms")
                        logger.info(f"Extracted {len(rooms_data)} room types for hotel {hotel_id} [Reference: {reference_id}]")
                    else:
                        batch_errors += 1
                        error_count += 1
                        print(f"     No valid data retrieved (got: {type(hotel_data)})")
                        logger.warning(f"No valid data retrieved for hotel {hotel_id} [Reference: {reference_id}] - received: {type(hotel_data)}")
                
                except Exception as e:
                    batch_errors += 1
                    error_count += 1
                    print(f"     Error: {str(e)[:50]}...")
                    logger.error(f"Error processing hotel {hotel_id} [Reference: {reference_id}]: {e}")
                    import traceback
                    logger.error(f"Full traceback: {traceback.format_exc()}")
                
                # Short delay between individual requests
                time.sleep(5)
            
            # Batch summary
            print(f"   Batch {batch_num} complete:  {batch_success} success,  {batch_errors} errors")
            
            # Progress summary
            remaining = total_hotels - processed_count
            success_rate = (success_count / processed_count) * 100
            print(f"   Overall progress: {processed_count}/{total_hotels} ({success_rate:.1f}% success rate)")
            print(f"   Estimated remaining: {remaining} hotels")
            
            # Longer delay between batches (except for last batch)
            if batch_end < total_hotels:
                print(f"   Waiting {delay} seconds before next batch...")
                logger.info(f"Batch completed. Waiting {delay} seconds before next batch...")
                time.sleep(delay)
        
        # Final summary
        print("\n" + "=" * 60)
        print(f" EXTRACTION COMPLETE!")
        print(f" Final Statistics:")
        print(f"   • Total hotels processed: {processed_count}")
        print(f"   • Successful extractions: {success_count}")
        print(f"   • Failed extractions: {error_count}")
        print(f"   • Success rate: {(success_count/processed_count)*100:.1f}%")
        print(f"   • Total rooms extracted: {len(all_rooms_data)}")
        print("=" * 60)
        
        return all_rooms_data
    
    def save_to_csv(self, rooms_data: List[Dict], output_path: str):
        """Save extracted room data to CSV with enhanced summary"""
        if not rooms_data:
            logger.warning("No room data to save")
            return
        
        df = pd.DataFrame(rooms_data)
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Saved {len(rooms_data)} room records to {output_path}")
        
        # Enhanced summary statistics
        summary = {
            'total_rooms': len(rooms_data),
            'unique_hotels': df['hotel_id'].nunique(),
            'unique_reference_hotels': df['reference_id'].nunique(),
            'extraction_date': datetime.now().isoformat(),
            'api_source': 'rate_hawk',
            'rooms_per_hotel': {
                'min': int(df.groupby('hotel_id').size().min()),
                'max': int(df.groupby('hotel_id').size().max()),
                'avg': float(df.groupby('hotel_id').size().mean()),
                'median': float(df.groupby('hotel_id').size().median())
            },
            'room_class_distribution': df['room_class'].value_counts().to_dict(),
            'room_quality_distribution': df['room_quality'].value_counts().to_dict(),
            'room_capacity_distribution': df['room_capacity'].value_counts().to_dict(),
            'hotel_details': df.groupby('reference_id').agg({
                'ref_hotel_name': 'first',
                'hotel_id': 'first',
                'hotel_name': 'first',
                'room_name': 'count'
            }).rename(columns={'room_name': 'room_count'}).to_dict('index')
        }
        
        summary_path = output_path.replace('.csv', '_summary.json')
        with open(summary_path, 'w') as f:
            json.dump(summary, f, indent=2)
        
        logger.info(f"Summary: {summary}")
        
        # Print file locations
        print(f"\n Files saved:")
        print(f"   • Room data CSV: {os.path.abspath(output_path)}")
        print(f"   • Summary JSON: {os.path.abspath(summary_path)}")


def main():
    """Main execution function"""
    # Configuration
    CSV_INPUT_PATH = r'.\app\data\hotel_mappings.csv'
    CSV_OUTPUT_PATH = r'.\app\data\01_api_rate_hawk_rooms.csv'
    
    BATCH_SIZE = 5  # Process 5 hotels at a time
    BATCH_DELAY = 2.0  # 2 seconds between batches
    API_SOURCE = 'rate_hawk'
    
    # Debug: Check environment and paths
    print("=== INITIALIZATION ===")
    print(f"Current working directory: {os.getcwd()}")
    print(f"Input file exists: {os.path.exists(CSV_INPUT_PATH)}")
    print(f"Input file path: {os.path.abspath(CSV_INPUT_PATH)}")
    
    # Check environment variables
    print(f"RATE_HAWK_USERNAME set: {bool(os.getenv('RATE_HAWK_USERNAME'))}")
    print(f"RATE_HAWK_PASSWORD set: {bool(os.getenv('RATE_HAWK_PASSWORD'))}")
    print("=====================")
    
    try:
        # Initialize extractor
        extractor = HotelRoomsExtractor()
        print(" Extractor initialized successfully")
        
        # Load and filter hotel mappings
        print("\n Loading hotel mappings...")
        logger.info("Starting hotel rooms extraction process...")
        mappings_df = extractor.load_hotel_mappings(CSV_INPUT_PATH)
        filtered_hotels = extractor.filter_hotels_by_api(mappings_df, API_SOURCE)
        
        if filtered_hotels.empty:
            print(f" No hotels found for API source: {API_SOURCE}")
            logger.warning(f"No hotels found for API source: {API_SOURCE}")
            return
        
        print(f" Found {len(filtered_hotels)} hotels to process")
        
        # Ask for confirmation for large datasets
        if len(filtered_hotels) > 10:
            response = input(f"\n This will process {len(filtered_hotels)} hotels. Continue? (y/N): ")
            if response.lower() != 'y':
                print(" Process cancelled by user")
                return
        
        # Process ALL hotels
        logger.info(f"Starting extraction for {len(filtered_hotels)} hotels...")
        rooms_data = extractor.process_all_hotels(
            filtered_hotels, 
            batch_size=BATCH_SIZE, 
            delay=BATCH_DELAY
        )
        
        # Save results
        print(f"\n Saving results...")
        extractor.save_to_csv(rooms_data, CSV_OUTPUT_PATH)
        
        print("\n PROCESS COMPLETED SUCCESSFULLY!")
        logger.info("Extraction process completed successfully!")
        
    except KeyboardInterrupt:
        print("\n Process interrupted by user")
        logger.info("Process interrupted by user")
    except Exception as e:
        print(f"\n FATAL ERROR: {e}")
        logger.error(f"Fatal error during extraction: {e}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    main()