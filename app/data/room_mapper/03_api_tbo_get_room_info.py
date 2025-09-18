#!/usr/bin/env python3
"""
Simplified TBO Room Extractor - Only Essential Columns
Extracts: reference_id, ref_hotel_name, hotel_id, hotel_name, room_name
"""

import pandas as pd
import json
import time
import os
import asyncio
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Any
import logging
import sys
from pathlib import Path

# Add project root directory to path
sys.path.append(str(Path(__file__).parent.parent.parent))

from app.services.providers.tbo import TBOProvider
from app.config import config

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('tbo_rooms_extraction.log', encoding='utf-8'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TBORoomsExtractor:
    """
    Simplified TBO Room Extractor
    Only extracts essential columns for room mapping
    """
    
    def __init__(self):
        self.tbo_provider = TBOProvider()

    def create_multiple_search_params(self, hotel_code: str, nights: int = 4) -> List[Dict[str, Any]]:
        """Create search parameters for multiple date ranges - 1, 2, and 3 months from now"""
        search_params_list = []
        
        # Create 3 different date ranges: 1, 2, and 3 months from now
        for i in range(3):
            # Calculate check-in date: 1, 2, or 3 months from now
            months_ahead = i + 1  # 1, 2, 3 months
            check_in = datetime.now() + timedelta(days=30 * months_ahead)  # Approximate months
            check_out = check_in + timedelta(days=nights)
            
            search_params = {
                'check_in_date': check_in.strftime('%Y-%m-%d'),
                'check_out_date': check_out.strftime('%Y-%m-%d'),
                'hotel_codes': [hotel_code],
                'guest_nationality': 'PL',
                'rooms': [{'adults': 2, 'children': 0}]
            }
            search_params_list.append(search_params)
        
        return search_params_list

    async def search_hotel_rooms(self, hotel_code: str) -> Optional[Dict]:
        """Search for rooms using TBO API with multiple date ranges"""
        search_params_list = self.create_multiple_search_params(hotel_code)
        
        for i, search_params in enumerate(search_params_list):
            try:
                months_ahead = i + 1
                logger.debug(f"Trying search params {i+1}/3 for hotel {hotel_code} ({months_ahead} month{'s' if months_ahead > 1 else ''} ahead)")
                
                response = await self.tbo_provider.search(search_params)
                
                if response.get('success', False):
                    data = response.get('data', {})
                    hotel_results = data.get('HotelResult', [])
                    if hotel_results and hotel_results[0].get('Rooms'):
                        logger.info(f"Found rooms for hotel {hotel_code} with {months_ahead} month{'s' if months_ahead > 1 else ''} ahead search")
                        return hotel_results[0]  # Return first hotel data
                        
            except Exception as e:
                months_ahead = i + 1
                logger.warning(f"Error with search params {i+1}/3 ({months_ahead} month{'s' if months_ahead > 1 else ''} ahead) for hotel {hotel_code}: {e}")
                continue
        
        logger.warning(f"No rooms found for hotel {hotel_code} with any search params")
        return None

    def normalize_room_data(self, hotel_data: Dict, hotel_code: str, reference_id: str, ref_hotel_name: str) -> List[Dict]:
        """
        Simplified room data normalization - only essential columns
        Structure: reference_id, ref_hotel_name, hotel_id, hotel_name, room_name
        """
        rooms_data = []
        
        rooms = hotel_data.get('Rooms', [])
        if not rooms:
            logger.warning(f"No rooms found for hotel {hotel_code}")
            return rooms_data
        
        logger.info(f"Found {len(rooms)} room offers for hotel {hotel_code}")
        
        # Process each room offer
        for room_idx, room in enumerate(rooms):
            # Extract room name
            room_names = room.get('Name', ['Standard Room'])
            original_room_name = room_names[0] if isinstance(room_names, list) and room_names else 'Standard Room'
            
            # Build simplified room record - TYLKO POTRZEBNE KOLUMNY
            room_record = {
                'reference_id': reference_id,
                'ref_hotel_name': ref_hotel_name,
                'hotel_id': hotel_code,
                'hotel_name': ref_hotel_name,
                'room_name': original_room_name
            }
            
            rooms_data.append(room_record)
        
        logger.info(f"Normalized {len(rooms_data)} room records for hotel {reference_id}")
        return rooms_data

    def load_hotel_mappings(self, csv_path: str) -> pd.DataFrame:
        """Load hotel mappings CSV with TBO data"""
        try:
            df = pd.read_csv(csv_path)
            logger.info(f"Loaded {len(df)} hotel mappings")
            return df
        except Exception as e:
            logger.error(f"Error loading CSV: {e}")
            raise

    def filter_tbo_hotels(self, df: pd.DataFrame) -> pd.DataFrame:
        """Filter hotels with TBO matches"""
        # Filter hotels that have TBO matches (tbo_matched = TRUE) and valid TBO hotel IDs
        filtered = df[
            (df['tbo_matched'] == True) &
            (df['tbo_hotel_id'].notna()) & 
            (df['tbo_hotel_id'] != '') &
            (df['tbo_hotel_id'] != 0)
        ].copy()
        
        # Convert to string and remove .0 from floats
        filtered['tbo_hotel_id'] = filtered['tbo_hotel_id'].astype(str).str.replace('.0', '', regex=False)
        
        logger.info(f"Found {len(filtered)} TBO hotels to process (from {len(df)} total hotels)")
        return filtered

    async def process_all_hotels(self, hotels_df: pd.DataFrame, batch_size: int = 2, delay: float = 10.0) -> List[Dict]:
        """Process ALL TBO hotels with simplified data extraction and rate limiting"""
        all_rooms_data = []
        total_hotels = len(hotels_df)
        processed_count = 0
        success_count = 0
        error_count = 0
        
        print(f"\n🚀 Starting Simplified TBO extraction for {total_hotels} hotels...")
        print(f"📊 Batch size: {batch_size}, Delay between requests: {delay}s")
        print(f"📅 Using 3 date ranges: 1, 2, and 3 months from now")
        print("=" * 70)
        
        for i in range(0, total_hotels, batch_size):
            batch_end = min(i + batch_size, total_hotels)
            batch = hotels_df.iloc[i:batch_end]
            batch_num = i // batch_size + 1
            total_batches = (total_hotels - 1) // batch_size + 1
            
            print(f"\n📦 Batch {batch_num}/{total_batches} (Hotels {i+1}-{batch_end})")
            logger.info(f"Processing batch {batch_num}/{total_batches} (hotels {i+1}-{batch_end} of {total_hotels})")
            
            batch_success = 0
            batch_errors = 0
            
            for idx, row in batch.iterrows():
                hotel_id = str(row['tbo_hotel_id'])
                reference_id = row['reference_id']
                ref_hotel_name = row['ref_hotel_name']
                processed_count += 1
                
                print(f"   🏨 [{processed_count}/{total_hotels}] {reference_id} | TBO:{hotel_id}")
                print(f"      📛 {ref_hotel_name[:60]}...")
                logger.info(f"Fetching rooms for TBO hotel {hotel_id} [Reference: {reference_id}]")
                
                try:
                    # Search for rooms
                    hotel_data = await self.search_hotel_rooms(hotel_id)
                    
                    if hotel_data and hotel_data.get('Rooms'):
                        # Normalize room data
                        rooms_data = self.normalize_room_data(
                            hotel_data, hotel_id, reference_id, ref_hotel_name
                        )
                        
                        if rooms_data:
                            all_rooms_data.extend(rooms_data)
                            batch_success += 1
                            success_count += 1
                            print(f"      ✅ Found {len(rooms_data)} rooms")
                        else:
                            print(f"      ⚠️  No valid rooms extracted")
                    else:
                        print(f"      ❌ No rooms found")
                        error_count += 1
                        batch_errors += 1
                        
                except Exception as e:
                    logger.error(f"Error processing hotel {hotel_id}: {e}")
                    print(f"      💥 Error: {str(e)[:50]}...")
                    error_count += 1
                    batch_errors += 1
                
                # Rate limiting
                if processed_count < total_hotels:
                    print(f"      ⏳ Waiting {delay}s...")
                    await asyncio.sleep(delay)
            
            # Batch summary
            print(f"   📊 Batch {batch_num} complete: {batch_success} success, {batch_errors} errors")
            
            # Additional delay between batches
            if batch_end < total_hotels:
                print(f"   ⏳ Batch delay: {delay * 2}s...")
                await asyncio.sleep(delay * 2)
        
        # Final summary
        print(f"\n🎉 Extraction Complete!")
        print(f"📊 Final Statistics:")
        print(f"   • Total hotels processed: {processed_count}")
        print(f"   • Successful extractions: {success_count}")
        print(f"   • Failed extractions: {error_count}")
        print(f"   • Total rooms extracted: {len(all_rooms_data)}")
        
        return all_rooms_data

    def save_to_csv(self, rooms_data: List[Dict], output_path: str):
        """Save extracted room data to CSV - simplified version"""
        if not rooms_data:
            logger.warning("No room data to save")
            return
        
        df = pd.DataFrame(rooms_data)
        
        # Ensure output directory exists
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        
        # Save to CSV
        df.to_csv(output_path, index=False, encoding='utf-8')
        logger.info(f"Saved {len(rooms_data)} room records to {output_path}")
        
        # Simple summary
        summary = {
            'total_rooms': len(rooms_data),
            'unique_hotels': df['hotel_id'].nunique(),
            'unique_reference_hotels': df['reference_id'].nunique(),
            'extraction_date': datetime.now().isoformat(),
            'api_source': 'tbo',
            'multiple_date_ranges_used': True,
            'rooms_per_hotel': {
                'min': int(df.groupby('hotel_id').size().min()),
                'max': int(df.groupby('hotel_id').size().max()),
                'avg': float(df.groupby('hotel_id').size().mean()),
                'median': float(df.groupby('hotel_id').size().median())
            }
        }
        
        # Save simple summary
        summary_path = output_path.replace('.csv', '_summary.json')
        with open(summary_path, 'w', encoding='utf-8') as f:
            json.dump(summary, f, indent=2, ensure_ascii=False)
        
        logger.info(f"Simple summary: {summary}")
        
        # Print file locations and key statistics
        print(f"\n📁 Files saved:")
        print(f"   • Room data CSV: {os.path.abspath(output_path)}")
        print(f"   • Summary JSON: {os.path.abspath(summary_path)}")
        print(f"\n📊 Key Statistics:")
        print(f"   • Total rooms extracted: {len(rooms_data)}")
        print(f"   • Hotels with rooms found: {df['hotel_id'].nunique()}")
        print(f"   • Average rooms per hotel: {float(df.groupby('hotel_id').size().mean()):.1f}")

    async def test_connection(self) -> bool:
        """Test TBO API connection with sample hotel - simplified version"""
        print("\n🔧 Testing TBO API connection with 1, 2, and 3 months ahead...")
        
        # Test with a sample hotel ID
        test_hotel_id = "1144457"
        
        print(f"   Testing TBO hotel ID: {test_hotel_id}")
        
        try:
            hotel_data = await self.search_hotel_rooms(test_hotel_id)
            
            if hotel_data and hotel_data.get('Rooms'):
                rooms_count = len(hotel_data.get('Rooms', []))
                print(f"   ✅ Connection successful! Found {rooms_count} room offers")
                
                # Show first room name as example
                if rooms_count > 0:
                    sample_room = hotel_data['Rooms'][0]
                    room_names = sample_room.get('Name', ['Standard Room'])
                    sample_room_name = room_names[0] if isinstance(room_names, list) and room_names else 'Standard Room'
                    
                    print(f"   📝 Sample room: {sample_room_name}")
                
                return True
            else:
                print(f"   ❌ No rooms found for test hotel {test_hotel_id}")
                return False
                
        except Exception as e:
            print(f"   💥 Connection test failed: {e}")
            return False

async def main():
    """Main execution function - simplified version"""
    print("🚀 Starting Simplified TBO Room Extractor")
    print("=" * 50)
    
    # Configuration
    input_csv = r'.\app\data\hotel_mappings.csv'
    output_csv = 'app/data/03_api_tbo_rooms.csv'
    
    # Check if input file exists
    if not os.path.exists(input_csv):
        print(f"❌ Input file not found: {input_csv}")
        print("Please ensure hotel_mappings.csv exists with TBO hotel mappings.")
        return
    
    extractor = TBORoomsExtractor()
    
    # Test connection first
    print("\n🔧 Testing TBO API connection...")
    if not await extractor.test_connection():
        print("❌ TBO API connection test failed. Exiting.")
        return
    
    print("\n✅ TBO API connection successful!")
    
    # Load and filter hotels
    print(f"\n📂 Loading hotel mappings from: {input_csv}")
    hotels_df = extractor.load_hotel_mappings(input_csv)
    print(f"📊 Loaded {len(hotels_df)} total hotels from mappings file")
    
    filtered_hotels = extractor.filter_tbo_hotels(hotels_df)
    
    if filtered_hotels.empty:
        print("❌ No TBO hotels found in the input file.")
        print("   Make sure hotel_mappings.csv contains hotels with tbo_matched=TRUE and valid tbo_hotel_id")
        return
    
    # Show sample of hotels to be processed
    print(f"\n📋 Sample TBO hotels to process:")
    for i, (_, row) in enumerate(filtered_hotels.head(5).iterrows()):
        print(f"   {i+1}. {row['reference_id']} | TBO:{row['tbo_hotel_id']} - {row['ref_hotel_name']}")
    if len(filtered_hotels) > 5:
        print(f"   ... and {len(filtered_hotels) - 5} more")
    
    # Process all hotels
    print(f"\n🏨 Processing {len(filtered_hotels)} TBO hotels...")
    all_rooms_data = await extractor.process_all_hotels(
        filtered_hotels, 
        batch_size=2, 
        delay=2.0
    )
    
    # Save results
    if all_rooms_data:
        print(f"\n💾 Saving {len(all_rooms_data)} room records...")
        extractor.save_to_csv(all_rooms_data, output_csv)
        print(f"✅ Extraction complete! Results saved to: {output_csv}")
    else:
        print("❌ No room data extracted.")

if __name__ == "__main__":
    asyncio.run(main())
