#!/usr/bin/env python3
"""
Process goglobal room data - complete standardization
"""

import pandas as pd
from pathlib import Path
import sys
import os

# Add the parent directory to Python path to import universal_room_parser
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from universal_room_parser import RoomDataParser

def process_goglobal_complete():
    """Process goglobal room data through all standardization steps"""
    
    print("=" * 80)
    print(f"PROCESSING GOGLOBAL ROOM DATA - COMPLETE STANDARDIZATION")
    print("=" * 80)
    
    # Initialize parser with provider
    parser = RoomDataParser(provider='goglobal')
    
    # File paths
    input_file = Path('app/data/02_api_goglobal_rooms.csv')
    output_file = Path('app/data/02_api_goglobal_rooms_STANDARDIZED.csv')
    
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")
    
    # Check if input file exists
    if not input_file.exists():
        print(f"Error: Input file not found: {input_file}")
        return
    
    # Load data
    print("Loading data...")
    df = pd.read_csv(input_file)
    print(f"Loaded {len(df)} room records")
    
    # Process all standardization steps
    print("\nProcessing all standardization steps...")
    
    # Special handling for main_name based on provider
    if 'goglobal' == 'ratehawk':
        print("1. Using existing main_name from source file (RateHawk specific)...")
        # Keep existing main_name column from source file
        if 'main_name' not in df.columns:
            print("   Warning: main_name column not found in RateHawk source file!")
            df['main_name'] = df['room_name'].apply(parser.parse_main_name)
    else:
        print("1. Parsing main_name...")
        df['main_name'] = df['room_name'].apply(parser.parse_main_name)
    
    print("2. Parsing bedrooms_count...")
    df['bedrooms_count'] = df['room_name'].apply(parser.parse_bedrooms_count)
    
    print("3. Parsing room_capacity...")
    df['room_capacity'] = df['room_name'].apply(parser.parse_room_capacity)
    
    print("4. Parsing room_area...")
    area_results = df['room_name'].apply(parser.parse_room_area)
    df['room_area_m2'] = [result[0] for result in area_results]
    df['room_area_sqft'] = [result[1] for result in area_results]
    
    print("5. Parsing room_class...")
    df['room_class'] = df['room_name'].apply(parser.parse_room_class)
    
    print("6. Parsing room_quality...")
    df['room_quality'] = df['room_name'].apply(parser.parse_room_quality)
    
    print("7. Parsing room_quality_category...")
    df['room_quality_category'] = df['room_name'].apply(parser.parse_room_quality_category)
    
    print("8. Parsing bedding_config...")
    df['bedding_config'] = df['room_name'].apply(parser.parse_bedding_config)
    
    print("9. Parsing bedding_type...")
    df['bedding_type'] = df['room_name'].apply(parser.parse_bedding_type)
    
    print("10. Parsing room_view...")
    df['room_view'] = df['room_name'].apply(parser.parse_room_view)
    
    print("11. Parsing balcony...")
    df['balcony'] = df['room_name'].apply(parser.parse_balcony)
    
    print("12. Parsing family_room...")
    df['family_room'] = df['room_name'].apply(parser.parse_family_room)
    
    print("13. Parsing club_room...")
    df['club_room'] = df['room_name'].apply(parser.parse_club_room)
    
    # Save output
    print(f"\nSaving complete standardized file to {output_file}...")
    
    # Select only the standardized columns in the correct order
    standardized_columns = [
        'reference_id', 'ref_hotel_name', 'hotel_id', 'hotel_name', 'room_name', 
        'main_name', 'bedrooms_count', 'room_capacity', 'room_quality', 
        'room_quality_category', 'room_class', 'bedding_type', 'bedding_config', 
        'balcony', 'family_room', 'club_room', 'room_view', 'room_area_m2', 'room_area_sqft'
    ]
    
    # Create standardized DataFrame with only required columns
    df_standardized = df[standardized_columns].copy()
    
    df_standardized.to_csv(output_file, index=False)
    print(f"Complete standardization completed successfully!")
    print(f"Output saved to: {output_file}")
    
    # Show summary
    print(f"\nSUMMARY:")
    print(f"DataFrame shape: {df_standardized.shape}")
    print(f"Total records: {len(df_standardized)}")
    print(f"Standardized columns: {list(df_standardized.columns)}")
    
    # Show sample standardization results
    print(f"\nSAMPLE RESULTS:")
    if len(df_standardized) > 0:
        sample_row = df_standardized.iloc[0]
        print(f"Original room_name: '{sample_row['room_name']}'")
        print(f"Standardized:")
        print(f"  - main_name: {sample_row['main_name']}")
        print(f"  - room_class: {sample_row['room_class']}")
        print(f"  - room_view: {sample_row['room_view']}")
        print(f"  - balcony: {sample_row['balcony']}")
        print(f"  - family_room: {sample_row['family_room']}")
        print(f"  - club_room: {sample_row['club_room']}")
        print(f"  - room_area_m2: {sample_row['room_area_m2']}")
        print(f"  - room_area_sqft: {sample_row['room_area_sqft']}")

if __name__ == "__main__":
    process_goglobal_complete()
