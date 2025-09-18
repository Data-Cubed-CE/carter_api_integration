#!/usr/bin/env python3
"""
Process all 3 room data files through all 13 standardization steps
"""

import subprocess
import sys
from pathlib import Path
import time

def run_command(command):
    """Run a command and return the result"""
    print(f"\n>>> Running: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)
    
    if result.stdout:
        print("STDOUT:")
        print(result.stdout)
    
    if result.stderr:
        print("STDERR:")
        print(result.stderr)
    
    if result.returncode != 0:
        print(f"Command failed with return code: {result.returncode}")
        return False
    
    return True

def process_provider_file(provider_name, source_file, target_prefix):
    """Process a single provider file through all standardization steps"""
    
    print("=" * 100)
    print(f"PROCESSING {provider_name.upper()} - FILE: {source_file}")
    print("=" * 100)
    
    # Create and run complete parser for each provider
    create_complete_parser(provider_name, source_file, target_prefix)
    
    # Run the complete processing
    command = f"python process_{provider_name}_complete.py"
    success = run_command(command)
    
    if not success:
        print(f"Failed to process {provider_name}")
        return False
    
    print(f"\nSuccessfully completed all standardization for {provider_name}")
    return True

def create_complete_parser(provider_name, source_file, target_prefix):
    """Create a complete parser that processes all 13 standardization steps at once"""
    
    parser_code = f'''#!/usr/bin/env python3
"""
Process {provider_name} room data - complete standardization
"""

import pandas as pd
from pathlib import Path
import sys
import os

# Add the parent directory to Python path to import universal_room_parser
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from universal_room_parser import RoomDataParser

def process_{provider_name}_complete():
    """Process {provider_name} room data through all standardization steps"""
    
    print("=" * 80)
    print(f"PROCESSING {provider_name.upper()} ROOM DATA - COMPLETE STANDARDIZATION")
    print("=" * 80)
    
    # Initialize parser with provider
    parser = RoomDataParser(provider='{provider_name}')
    
    # File paths
    input_file = Path('app/data/{source_file}')
    output_file = Path('app/data/{target_prefix}_STANDARDIZED.csv')
    
    print(f"Input file: {{input_file}}")
    print(f"Output file: {{output_file}}")
    
    # Check if input file exists
    if not input_file.exists():
        print(f"Error: Input file not found: {{input_file}}")
        return
    
    # Load data
    print("Loading data...")
    df = pd.read_csv(input_file)
    print(f"Loaded {{len(df)}} room records")
    
    # Process all standardization steps
    print("\\nProcessing all standardization steps...")
    
    # Special handling for main_name based on provider
    if '{provider_name}' == 'ratehawk':
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
    print(f"\\nSaving complete standardized file to {{output_file}}...")
    
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
    print(f"Output saved to: {{output_file}}")
    
    # Show summary
    print(f"\\nSUMMARY:")
    print(f"DataFrame shape: {{df_standardized.shape}}")
    print(f"Total records: {{len(df_standardized)}}")
    print(f"Standardized columns: {{list(df_standardized.columns)}}")
    
    # Show sample standardization results
    print(f"\\nSAMPLE RESULTS:")
    if len(df_standardized) > 0:
        sample_row = df_standardized.iloc[0]
        print(f"Original room_name: '{{sample_row['room_name']}}'")
        print(f"Standardized:")
        print(f"  - main_name: {{sample_row['main_name']}}")
        print(f"  - room_class: {{sample_row['room_class']}}")
        print(f"  - room_view: {{sample_row['room_view']}}")
        print(f"  - balcony: {{sample_row['balcony']}}")
        print(f"  - family_room: {{sample_row['family_room']}}")
        print(f"  - club_room: {{sample_row['club_room']}}")
        print(f"  - room_area_m2: {{sample_row['room_area_m2']}}")
        print(f"  - room_area_sqft: {{sample_row['room_area_sqft']}}")

if __name__ == "__main__":
    process_{provider_name}_complete()
'''
    
    # Write the parser file
    parser_filename = f"process_{provider_name}_complete.py"
    with open(parser_filename, 'w', encoding='utf-8') as f:
        f.write(parser_code)
    
    print(f"Created {parser_filename}")

def main():
    """Main processing function"""
    
    print("Starting complete room data standardization")
    print("Processing 3 provider files - generating complete standardized files")
    print("=" * 100)
    
    # Define the providers and their files
    providers = [
        {
            'name': 'ratehawk',
            'source_file': '01_api_rate_hawk_rooms.csv',
            'target_prefix': '01_api_rate_hawk_rooms'
        },
        {
            'name': 'goglobal', 
            'source_file': '02_api_goglobal_rooms.csv',
            'target_prefix': '02_api_goglobal_rooms'
        },
        {
            'name': 'tbo',
            'source_file': '03_api_tbo_rooms.csv', 
            'target_prefix': '03_api_tbo_rooms'
        }
    ]
    
    success_count = 0
    
    # Process each provider
    for provider in providers:
        success = process_provider_file(
            provider['name'],
            provider['source_file'], 
            provider['target_prefix']
        )
        
        if success:
            success_count += 1
        else:
            print(f"Failed to process {provider['name']}")
    
    # Final summary
    print("\\n" + "=" * 100)
    print("FINAL SUMMARY")
    print("=" * 100)
    print(f"Successfully processed: {success_count}/3 providers")
    print(f"Generated standardized files:")
    
    if success_count == 3:
        print("\\nALL PROVIDERS PROCESSED SUCCESSFULLY!")
        print("\\nGenerated complete standardized files:")
        print("   - RateHawk: 01_api_rate_hawk_rooms_STANDARDIZED.csv")  
        print("   - GoGlobal: 02_api_goglobal_rooms_STANDARDIZED.csv")
        print("   - TBO: 03_api_tbo_rooms_STANDARDIZED.csv")
        print("\\nEach file contains exactly 19 standardized columns:")
        print("   reference_id, ref_hotel_name, hotel_id, hotel_name, room_name,")
        print("   main_name, bedrooms_count, room_capacity, room_quality, room_quality_category,")
        print("   room_class, bedding_type, bedding_config, balcony, family_room,")
        print("   club_room, room_view, room_area_m2, room_area_sqft")
    else:
        print(f"\\n{3 - success_count} provider(s) failed processing")

if __name__ == "__main__":
    main()
