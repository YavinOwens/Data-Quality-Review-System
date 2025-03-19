#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
File Organization Script
This script organizes CSV files in the oracle_tables folder by:
1. Converting _info.csv files to text and moving them to a 'text information' folder
2. Moving other CSV files to a 'tables' folder
"""

import os
import shutil
import csv
import pandas as pd

# Directory paths
oracle_tables_dir = '/Users/yavinowens/Documents/Python_offline/Python_offline/oracle_tables'
text_info_dir = os.path.join(oracle_tables_dir, 'text information')
tables_dir = os.path.join(oracle_tables_dir, 'tables')

# Create directories if they don't exist
os.makedirs(text_info_dir, exist_ok=True)
os.makedirs(tables_dir, exist_ok=True)

print(f"Created directories: '{text_info_dir}' and '{tables_dir}'")

# Process files
file_count = {'info_converted': 0, 'tables_moved': 0}

for filename in os.listdir(oracle_tables_dir):
    file_path = os.path.join(oracle_tables_dir, filename)
    
    # Skip directories
    if os.path.isdir(file_path):
        continue
    
    # Process _info.csv files
    if filename.endswith('_info.csv'):
        # Read the CSV file
        try:
            df = pd.read_csv(file_path)
            
            # Create text file with the same name but .txt extension
            txt_filename = filename.replace('.csv', '.txt')
            txt_file_path = os.path.join(text_info_dir, txt_filename)
            
            # Convert DataFrame to text format
            with open(txt_file_path, 'w') as txt_file:
                # Write header
                txt_file.write(f"Information for {filename.split('_info.csv')[0]}:\n")
                txt_file.write("=" * 50 + "\n\n")
                
                # Write each row as key-value pairs
                for _, row in df.iterrows():
                    for column in df.columns:
                        txt_file.write(f"{column}: {row[column]}\n")
                    txt_file.write("\n")
            
            file_count['info_converted'] += 1
            print(f"Converted {filename} to {txt_filename}")
            
        except Exception as e:
            print(f"Error converting {filename}: {e}")
    
    # Process regular CSV files (not ending with _info.csv)
    elif filename.endswith('.csv'):
        # Move to tables directory
        dest_path = os.path.join(tables_dir, filename)
        try:
            shutil.move(file_path, dest_path)
            file_count['tables_moved'] += 1
            print(f"Moved {filename} to tables folder")
        except Exception as e:
            print(f"Error moving {filename}: {e}")

print(f"\nSummary:")
print(f"- Converted {file_count['info_converted']} info files to text")
print(f"- Moved {file_count['tables_moved']} CSV files to tables folder")
print("File organization complete!")
