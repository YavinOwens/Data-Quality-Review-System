#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generate Bespoke ERD Diagrams

This script generates Entity Relationship Diagrams (ERDs) for the Oracle table schemas
using sadisplay, which requires creating SQLAlchemy models from the CSV data.
"""

import os
import glob
import csv
import re
import sadisplay
import codecs
from sqlalchemy import Column, String, ForeignKey, Integer, MetaData, Table, Text, DateTime, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, declared_attr

# Directory paths
TABLES_DIR = 'oracle_tables/tables'
TEXT_INFO_DIR = 'oracle_tables/text information'
OUTPUT_DIR = 'erds/bespoke erds'

# Define SQLAlchemy base
Base = declarative_base()
metadata = MetaData()

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
    table_files = glob.glob(os.path.join(TABLES_DIR, '*.csv'))
    distinct_tables = set()
    
    for file_path in table_files:
        if 'all_tables' in file_path:
            continue
        
        # Extract table name from filename (remove timestamp)
        filename = os.path.basename(file_path)
        table_name = re.sub(r'_\d{8}_\d{6}\.csv$', '', filename)
        distinct_tables.add(table_name)
    
    return list(distinct_tables)

def get_table_columns(table_name):
    """
    Get columns for a specific table from the most recent version of the table schema.
    
    Args:
        table_name (str): Name of the table
        
    Returns:
        List[Dict]: List of column information dictionaries
    """
    # Find the most recent file for this table
    table_files = glob.glob(os.path.join(TABLES_DIR, f'{table_name}_*.csv'))
    if not table_files:
        return []
    
    most_recent_file = max(table_files, key=os.path.getmtime)
    
    columns = []
    with open(most_recent_file, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)  # Skip header
        
        for row in reader:
            column_name, data_type, nullable = row
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
    
    return relationships

def create_sqlalchemy_models(tables, columns_by_table, relationships):
    """
    Dynamically create SQLAlchemy models for the tables.
    
    Args:
        tables (List[str]): List of table names
        columns_by_table (Dict): Dictionary mapping table names to their columns
        relationships (List[Dict]): List of relationship dictionaries
        
    Returns:
        Dict: Dictionary mapping table names to their SQLAlchemy model classes
    """
    models = {}
    
    # Identify primary key columns for each table
    primary_keys = {}
    for table in tables:
        # Look for ID, TABLE_ID, or first column as potential primary key
        pk_candidates = ['ID', f"{table}_ID"]
        
        # Find the best primary key candidate
        for column in columns_by_table[table]:
            if column['name'] in pk_candidates:
                primary_keys[table] = column['name']
                break
        
        # If no primary key found, use the first column
        if table not in primary_keys and columns_by_table[table]:
            primary_keys[table] = columns_by_table[table][0]['name']
    
    # First pass - create model classes
    for table in tables:
        # Convert Oracle table name to Python class name
        class_name = ''.join(word.capitalize() for word in table.split('_'))
        
        # Create a new model class
        models[table] = type(
            class_name,
            (Base,),
            {
                '__tablename__': table,
                '__table_args__': {'extend_existing': True},
                '__doc__': get_table_info(table).get('description', f"Table {table}")
            }
        )
    
    # Second pass - add columns and foreign keys
    for table in tables:
        model = models[table]
        
        for column in columns_by_table[table]:
            column_name = column['name']
            column_type = column['type']
            nullable = column['nullable']
            
            # Map Oracle data types to SQLAlchemy types
            if column_type == 'VARCHAR2':
                sa_type = String(255)
            elif column_type == 'NUMBER':
                sa_type = Integer
            elif column_type == 'DATE':
                sa_type = DateTime
            elif column_type == 'CLOB':
                sa_type = Text
            elif column_type == 'FLOAT':
                sa_type = Float
            elif column_type == 'CHAR':
                sa_type = String(1)
            elif column_type == 'BOOLEAN':
                sa_type = Boolean
            else:
                sa_type = String(255)  # Default to String for unknown types
            
            # Check if this column is a primary key
            is_primary_key = table in primary_keys and column_name == primary_keys[table]
            
            # Check if this column is a foreign key
            is_foreign_key = False
            for rel in relationships:
                if rel['source_table'] == table and rel['source_column'] == column_name:
                    setattr(
                        model,
                        column_name,
                        Column(
                            column_name,
                            sa_type,
                            ForeignKey(f"{rel['target_table']}.{rel['target_column']}"),
                            primary_key=is_primary_key,
                            nullable=nullable
                        )
                    )
                    
                    # Add relationship attribute
                    target_model = models[rel['target_table']]
                    backref_name = f"{table.lower()}_set"
                    setattr(
                        model,
                        rel['target_table'].lower(),
                        relationship(target_model.__name__, backref=backref_name)
                    )
                    
                    is_foreign_key = True
                    break
            
            # If not a foreign key, add as a regular column
            if not is_foreign_key:
                setattr(model, column_name, Column(
                    column_name, 
                    sa_type, 
                    primary_key=is_primary_key,
                    nullable=nullable
                ))
    
    return models

def generate_diagram(models, output_filename, diagram_type='er'):
    """
    Generate a diagram for the given models.
    
    Args:
        models (Dict): Dictionary of SQLAlchemy models
        output_filename (str): Base name for the output files
        diagram_type (str): Type of diagram ('er' or 'uml')
    """
    model_classes = list(models.values())
    
    # Generate dot file from models
    if diagram_type == 'er':
        dot = sadisplay.dot(model_classes, show_methods=False, show_datatypes=True, show_properties=True)
    else:  # UML diagram
        dot = sadisplay.dot(model_classes, show_methods=False, show_datatypes=True, 
                         show_properties=True, show_relationships=True)
    
    # Write dot file
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    dot_filename = os.path.join(OUTPUT_DIR, f"{output_filename}.dot")
    png_filename = os.path.join(OUTPUT_DIR, f"{output_filename}.png")
    pdf_filename = os.path.join(OUTPUT_DIR, f"{output_filename}.pdf")
    
    with codecs.open(dot_filename, 'w', encoding='utf-8') as f:
        f.write(dot)
    
    # Generate diagrams from dot file
    os.system(f"dot -Tpng {dot_filename} -o {png_filename}")
    os.system(f"dot -Tpdf {dot_filename} -o {pdf_filename}")
    
    print(f"Generated {diagram_type.upper()} diagram: {pdf_filename}")

def main():
    """Main function to generate the ERD diagrams."""
    print("Generating Bespoke ERD Diagrams...")
    
    # Get distinct tables
    tables = get_distinct_tables()
    print(f"Found {len(tables)} distinct tables")
    
    # Get columns for each table
    columns_by_table = {table: get_table_columns(table) for table in tables}
    
    # Detect relationships between tables
    relationships = detect_relationships(tables, columns_by_table)
    print(f"Detected {len(relationships)} potential relationships between tables")
    
    # Create SQLAlchemy models
    models = create_sqlalchemy_models(tables, columns_by_table, relationships)
    
    # Generate diagrams
    generate_diagram(models, "oracle_hr_erd", diagram_type='er')
    generate_diagram(models, "oracle_hr_uml", diagram_type='uml')
    
    # Generate individual diagrams for each table
    for table in tables:
        table_models = {table: models[table]}
        generate_diagram(table_models, f"{table}_diagram", diagram_type='er')
    
    print("ERD generation complete")

if __name__ == "__main__":
    main()
