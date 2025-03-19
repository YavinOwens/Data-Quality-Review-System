#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate Bespoke ERD Diagrams Using GraphViz Directly

This script generates Entity Relationship Diagrams (ERDs) for the Oracle 
table schemas using GraphViz directly without SQLAlchemy.
"""

import os
import glob
import csv
import re
import codecs
from typing import List, Dict, Set
import pandas as pd

# Directory paths
TABLES_DIR = 'oracle_tables/tables'
TEXT_INFO_DIR = 'oracle_tables/text information'
ERD_DIR = 'erds'
OUTPUT_DIR = os.path.join(ERD_DIR, 'bespoke erds')

# Create output directory if it doesn't exist
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Regular expressions to identify potential foreign keys
foreign_key_patterns = [
    r'.*_ID$',  # Columns ending with _ID
    r'^.*_FK$'  # Columns ending with _FK
]

def get_distinct_tables():
    """
    Get a list of distinct table names from the tables directory.
    
    Returns:
        List[str]: List of distinct table names
    """
    # Use all_tables_*.csv file to get a list of distinct table names
    all_tables_files = glob.glob(os.path.join(TABLES_DIR, 'all_tables_*.csv'))
    if not all_tables_files:
        return []
    
    most_recent_file = max(all_tables_files, key=os.path.getmtime)
    
    # Read the CSV file and extract unique table names
    tables = set()
    with open(most_recent_file, 'r') as f:
        reader = csv.reader(f)
        next(reader)  # Skip header
        for row in reader:
            if row and len(row) > 0:
                tables.add(row[0])
    
    return list(tables)

def get_table_columns(table_name):
    """
    Get columns for a specific table from the most recent version of the table schema.
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        List[Dict]: List of column information dictionaries
    """
    # Try to find a specific CSV file for this table
    table_files = glob.glob(os.path.join(TABLES_DIR, f'{table_name}_*.csv'))
    
    # If no specific file is found, try to extract from all_tables file
    if not table_files:
        all_tables_files = glob.glob(os.path.join(TABLES_DIR, 'all_tables_*.csv'))
        if not all_tables_files:
            return []
        
        most_recent_file = max(all_tables_files, key=os.path.getmtime)
        
        # Read the all_tables CSV and extract columns for this table
        columns = []
        with open(most_recent_file, 'r') as f:
            reader = csv.reader(f)
            next(reader)  # Skip header
            for row in reader:
                if row and len(row) > 0 and row[0] == table_name:
                    column_name, data_type, nullable = row[1], row[2], row[3] if len(row) > 3 else 'Y'
                    columns.append({
                        'name': column_name,
                        'type': data_type,
                        'nullable': nullable == 'Y'
                    })
        
        return columns
    
    # Use the most recent specific table file
    most_recent_file = max(table_files, key=os.path.getmtime)
    
    columns = []
    with open(most_recent_file, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        
        for row in reader:
            if len(row) >= 3:
                column_name, data_type, nullable = row[0], row[1], row[2]
                columns.append({
                    'name': column_name,
                    'type': data_type,
                    'nullable': nullable == 'Y'
                })
    
    return columns

def get_table_info(table_name):
    """
    Get additional information about a table from its info text file.
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        Dict: Table information dictionary
    """
    info_files = glob.glob(os.path.join(TEXT_INFO_DIR, f'{table_name}_*_info.txt'))
    if not info_files:
        return {'description': f'No description available for {table_name}'}
    
    most_recent_file = max(info_files, key=os.path.getmtime)
    
    info = {'description': f'No description available for {table_name}'}
    current_property = None
    current_value = []
    
    with open(most_recent_file, 'r') as f:
        lines = f.readlines()
        
        for line in lines:
            line = line.strip()
            
            # Skip header lines
            if line.startswith('==') or f'Information for {table_name}' in line:
                continue
                
            # Extract property and value
            if line.startswith('property:'):
                # Save the previous property if it exists
                if current_property and current_value:
                    info[current_property] = '\n'.join(current_value)
                    current_value = []
                
                current_property = line.replace('property:', '').strip()
            elif line.startswith('value:'):
                value = line.replace('value:', '').strip()
                if value != 'nan':
                    current_value.append(value)
    
    # Save the last property
    if current_property and current_value:
        info[current_property] = '\n'.join(current_value)
    
    return info

def detect_relationships(tables, columns_by_table):
    """
    Detect potential foreign key relationships between tables.
    
    Args:
        tables (List[str]): List of table names
        columns_by_table (Dict): Dictionary mapping table names to their columns
        
    Returns:
        List[Dict]: List of relationship dictionaries
    """
    relationships = []
    
    # Create a dictionary of primary keys for each table
    primary_keys = {}
    for table in tables:
        for column in columns_by_table[table]:
            # Assume columns named <table>_ID or ID are primary keys
            if column['name'] == 'ID' or column['name'] == f"{table}_ID":
                primary_keys[table] = column['name']
                break
        
        # If no primary key found, use the first column
        if table not in primary_keys and columns_by_table[table]:
            primary_keys[table] = columns_by_table[table][0]['name']
    
    # Find person-related tables that might have a person_id
    person_related_tables = [table for table in tables if 'PERSON' in table or 'PER_' in table]
    person_table = None
    person_id_column = None
    
    # Find the main person table and its ID column
    for table in person_related_tables:
        for column in columns_by_table[table]:
            if column['name'] == 'PERSON_ID':
                person_table = table
                person_id_column = 'PERSON_ID'
                break
    
    # If we have a person table, add relationships for any tables with PERSON_ID
    if person_table and person_id_column:
        for table in tables:
            if table == person_table:
                continue  # Skip the person table itself
                
            for column in columns_by_table[table]:
                if column['name'] == 'PERSON_ID' or column['name'] == 'PER_ID':
                    relationships.append({
                        'source_table': table,
                        'source_column': column['name'],
                        'target_table': person_table,
                        'target_column': person_id_column
                    })
    
    # Detect foreign keys by column naming patterns
    for table in tables:
        for column in columns_by_table[table]:
            # Skip primary keys as foreign keys
            if table in primary_keys and column['name'] == primary_keys[table]:
                continue
                
            # Check if column matches foreign key patterns
            is_foreign_key = False
            referenced_table = None
            
            # Check if column name matches foreign key pattern
            for pattern in foreign_key_patterns:
                if re.match(pattern, column['name']):
                    is_foreign_key = True
                    
                    # Try to derive referenced table from column name
                    # For example, PERSON_ID likely references the PERSON table
                    match = re.match(r'([A-Z_]+)_ID$', column['name'])
                    if match:
                        potential_table = match.group(1)
                        # Check if the potential table exists in our list
                        for t in tables:
                            if t == potential_table or (
                                potential_table in t and 
                                # Avoid matching substring tables (like PER_ALL_PEOPLE_F matching PER_PEOPLE)
                                (t.startswith(potential_table + '_') or t == potential_table)
                            ):
                                referenced_table = t
                                break
            
            if is_foreign_key and referenced_table:
                relationships.append({
                    'source_table': table,
                    'source_column': column['name'],
                    'target_table': referenced_table,
                    'target_column': primary_keys.get(referenced_table, 'ID')  # Assume ID if no primary key known
                })
                
    # Add explicit relationships for PERSON_ID columns if not already detected
    for table in tables:
        # Skip the main person table if we found one
        if person_table and table == person_table:
            continue
            
        # Check each column for PERSON_ID
        for column in columns_by_table[table]:
            if 'PERSON_ID' in column['name'] or 'PER_ID' in column['name'] or 'PEOPLE_ID' in column['name']:
                # Check if this relationship already exists
                existing_rel = any(rel['source_table'] == table and 
                                  rel['source_column'] == column['name'] 
                                  for rel in relationships)
                if not existing_rel:
                    # Find the target table (prefer PER_ALL_PEOPLE_F if available)
                    target_table = None
                    for t in tables:
                        if 'PER_ALL_PEOPLE' in t:
                            target_table = t
                            break
                    
                    if not target_table:
                        for t in tables:
                            if 'PERSON' in t or 'PER_' in t:
                                target_table = t
                                break
                    
                    if target_table:
                        relationships.append({
                            'source_table': table,
                            'source_column': column['name'],
                            'target_table': target_table,
                            'target_column': primary_keys.get(target_table, 'ID')
                        })
    
    return relationships

def format_column_for_dot(column, is_primary=False, is_foreign=False):
    """
    Format a column for the DOT file.
    
    Args:
        column (Dict): Column information dictionary
        is_primary (bool): Whether this is a primary key
        is_foreign (bool): Whether this is a foreign key
        
    Returns:
        str: Formatted column string for DOT file
    """
    null_str = "" if column['nullable'] else " NOT NULL"
    pk_str = " PK" if is_primary else ""
    fk_str = " FK" if is_foreign else ""
    
    return f"<tr><td port=\"{column['name']}\">{column['name']}</td><td>{column['type']}{null_str}{pk_str}{fk_str}</td></tr>"

def generate_dot_file(tables, columns_by_table, relationships, primary_keys, output_filename):
    """
    Generate a DOT file for GraphViz with professional styling and crow's feet notation.
    
    Args:
        tables (List[str]): List of table names
        columns_by_table (Dict): Dictionary mapping table names to their columns
        relationships (List[Dict]): List of relationship dictionaries
        primary_keys (Dict): Dictionary mapping table names to their primary key column names
        output_filename (str): Base name for the output files
    """
    # Create the DOT file content with improved styling
    dot_content = [
        'digraph "Oracle HR Entity Relationship Diagram" {',
        '  graph [rankdir=LR, splines=ortho, nodesep=0.8, ranksep=1.2, fontname="Arial", fontsize=12, bgcolor="#FFFFFF"];',
        '  node [shape=plaintext, fontname="Arial", fontsize=11];',
        '  edge [fontname="Arial", fontsize=10, dir="both", arrowsize=0.9];',
        ''
    ]
    
    # Add tables with improved styling
    for table in tables:
        table_info = get_table_info(table)
        description = table_info.get('description', 'No description')
        # Truncate description to avoid excessively large diagrams
        short_description = description[:100] + '...' if len(description) > 100 else description
        short_description = short_description.replace('"', '\"').replace('\n', ' ')
        
        dot_content.append(f'  "{table}" [label=<')
        dot_content.append(f'    <table border="0" cellborder="1" cellspacing="0" cellpadding="4">')
        
        # Table header with better styling
        dot_content.append(f'      <tr><td colspan="3" bgcolor="#4169E1" align="center"><font color="white" point-size="12"><b>{table}</b></font></td></tr>')
        
        # Column headers
        dot_content.append(f'      <tr bgcolor="#D3D3D3"><td align="center"><b>Column</b></td><td align="center"><b>Type</b></td><td align="center"><b>Key</b></td></tr>')
        
        # Add columns with improved styling
        fk_columns = {rel['source_column'] for rel in relationships if rel['source_table'] == table}
        
        for column in columns_by_table[table]:
            is_primary = table in primary_keys and column['name'] == primary_keys[table]
            is_foreign = column['name'] in fk_columns
            
            # Style based on key type
            bg_color = ''
            key_content = ''
            
            if is_primary and is_foreign:
                bg_color = ' bgcolor="#F0E6FF"'  # Light purple for PK/FK
                key_content = '<font color="purple">PK/FK</font>'
            elif is_primary:
                bg_color = ' bgcolor="#FFE6E6"'  # Light red for PK
                key_content = '<font color="darkred">PK</font>'
            elif is_foreign:
                bg_color = ' bgcolor="#E6F0FF"'  # Light blue for FK
                key_content = '<font color="blue">FK</font>'
            
            null_str = "" if column.get('nullable', True) else " NOT NULL"
            
            # Add the column with styling
            dot_content.append(f'      <tr{bg_color}><td port="{column["name"]}" align="left">{column["name"]}</td>'
                             f'<td align="left">{column.get("type", "VARCHAR2")}{null_str}</td>'
                             f'<td align="center">{key_content}</td></tr>')
        
        dot_content.append('    </table>')
        dot_content.append('  >, style="filled,rounded", fillcolor="#FFFFFF", color="#333333"];')
        dot_content.append('')
    
    # Add relationships with proper crow's feet notation
    for rel in relationships:
        source_table = rel['source_table']
        target_table = rel['target_table']
        source_column = rel['source_column']
        target_column = rel['target_column']
        
        # Using improved crow's feet notation (many-to-one relationship)
        dot_content.append(f'  "{source_table}":"{source_column}" -> "{target_table}":"{target_column}" ['
                        f'arrowhead=none, arrowtail=crow, headlabel="1", taillabel="n", '
                        f'color="#0066CC", penwidth=1.2, fontcolor="#333333"];')
    
    dot_content.append('}')
    
    # Write to file
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    dot_filename = os.path.join(OUTPUT_DIR, f"{output_filename}.dot")
    
    with codecs.open(dot_filename, 'w', encoding='utf-8') as f:
        f.write('\n'.join(dot_content))
    
    return dot_filename

def generate_diagrams_by_subject_area(tables, columns_by_table, relationships, primary_keys):
    """
    Group tables by subject area and generate separate ERDs for each group.
    
    Args:
        tables (List[str]): List of table names
        columns_by_table (Dict): Dictionary mapping table names to their columns
        relationships (List[Dict]): List of relationship dictionaries
        primary_keys (Dict): Dictionary mapping table names to their primary key column names
    """
    # Group tables by common prefixes (e.g., PER_, HR_, etc.)
    subject_areas = {}
    
    for table in tables:
        # Extract the prefix
        match = re.match(r'([A-Z]+)_.*', table)
        if match:
            prefix = match.group(1)
            if prefix not in subject_areas:
                subject_areas[prefix] = []
            subject_areas[prefix].append(table)
        else:
            # Tables without a clear prefix go into a "Misc" group
            if 'MISC' not in subject_areas:
                subject_areas['MISC'] = []
            subject_areas['MISC'].append(table)
    
    # Generate a diagram for each subject area
    for prefix, tables_in_area in subject_areas.items():
        # Skip if there's only one table in this area
        if len(tables_in_area) <= 1:
            continue
            
        # Filter relationships to only include those between tables in this area
        area_relationships = [
            rel for rel in relationships 
            if rel['source_table'] in tables_in_area and rel['target_table'] in tables_in_area
        ]
        
        # Generate DOT file
        dot_filename = generate_dot_file(
            tables_in_area,
            {t: columns_by_table[t] for t in tables_in_area},
            area_relationships,
            primary_keys,
            f"{prefix}_tables"
        )
        
        # Generate diagrams from dot file
        png_filename = dot_filename.replace('.dot', '.png')
        pdf_filename = dot_filename.replace('.dot', '.pdf')
        
        # Run graphviz commands
        try:
            os.system(f"dot -Tpng \"{dot_filename}\" -o \"{png_filename}\"")
            os.system(f"dot -Tpdf \"{dot_filename}\" -o \"{pdf_filename}\"")
            print(f"Generated ERD for {prefix} subject area: {pdf_filename}")
        except Exception as e:
            print(f"Error generating diagram for {prefix} subject area: {e}")

def generate_related_tables_diagram(tables, columns_by_table, relationships, primary_keys):
    """
    Generate a diagram for related tables that are connected by relationships.
    
    Args:
        tables (List[str]): List of table names
        columns_by_table (Dict): Dictionary mapping table names to their columns
        relationships (List[Dict]): List of relationship dictionaries
        primary_keys (Dict): Dictionary mapping table names to their primary key column names
    """
    # Create a graph of connected tables
    connected_tables = set()
    
    for rel in relationships:
        connected_tables.add(rel['source_table'])
        connected_tables.add(rel['target_table'])
    
    # Only proceed if we have connected tables
    if not connected_tables:
        return
    
    # Generate DOT file for connected tables
    dot_filename = generate_dot_file(
        list(connected_tables),
        {t: columns_by_table[t] for t in connected_tables},
        relationships,
        primary_keys,
        "related_tables"
    )
    
    # Generate diagrams from dot file
    png_filename = dot_filename.replace('.dot', '.png')
    pdf_filename = dot_filename.replace('.dot', '.pdf')
    
    # Run graphviz commands
    try:
        os.system(f"dot -Tpng \"{dot_filename}\" -o \"{png_filename}\"")
        os.system(f"dot -Tpdf \"{dot_filename}\" -o \"{pdf_filename}\"")
        print(f"Generated ERD for related tables: {pdf_filename}")
    except Exception as e:
        print(f"Error generating related tables diagram: {e}")

def generate_complete_diagram(tables, columns_by_table, relationships, primary_keys):
    """
    Generate a complete diagram with all tables.
    
    Args:
        tables (List[str]): List of table names
        columns_by_table (Dict): Dictionary mapping table names to their columns
        relationships (List[Dict]): List of relationship dictionaries
        primary_keys (Dict): Dictionary mapping table names to their primary key column names
    """
    # Generate DOT file for all tables
    dot_filename = generate_dot_file(
        tables,
        columns_by_table,
        relationships,
        primary_keys,
        "complete_oracle_hr"
    )
    
    # Generate diagrams from dot file
    png_filename = dot_filename.replace('.dot', '.png')
    pdf_filename = dot_filename.replace('.dot', '.pdf')
    
    # Run graphviz commands
    try:
        os.system(f"dot -Tpng \"{dot_filename}\" -o \"{png_filename}\"")
        os.system(f"dot -Tpdf \"{dot_filename}\" -o \"{pdf_filename}\"")
        print(f"Generated complete ERD: {pdf_filename}")
    except Exception as e:
        print(f"Error generating complete diagram: {e}")

def main():
    """Main function to generate the ERD diagrams."""
    print("Generating Bespoke ERD Diagrams...")
    
    # Get distinct tables
    tables = get_distinct_tables()
    print(f"Found {len(tables)} distinct tables")
    
    # Get columns for each table
    columns_by_table = {table: get_table_columns(table) for table in tables}
    
    # Create a dictionary of primary keys for each table
    primary_keys = {}
    for table in tables:
        for column in columns_by_table[table]:
            # Assume columns named <table>_ID or ID are primary keys
            if column['name'] == 'ID' or column['name'] == f"{table}_ID":
                primary_keys[table] = column['name']
                break
        
        # If no primary key found, use the first column
        if table not in primary_keys and columns_by_table[table]:
            primary_keys[table] = columns_by_table[table][0]['name']
    
    # Detect relationships between tables
    relationships = detect_relationships(tables, columns_by_table)
    print(f"Detected {len(relationships)} potential relationships between tables")
    
    # Generate diagrams by subject area
    generate_diagrams_by_subject_area(tables, columns_by_table, relationships, primary_keys)
    
    # Generate diagram for related tables
    generate_related_tables_diagram(tables, columns_by_table, relationships, primary_keys)
    
    # Generate complete diagram with all tables
    generate_complete_diagram(tables, columns_by_table, relationships, primary_keys)
    
    print("ERD generation complete")

if __name__ == "__main__":
    main()
