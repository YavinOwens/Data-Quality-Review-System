"""
Module for generating Entity-Relationship Diagrams (ERDs) from database schema.
Uses Graphviz DOT format for visualization.
"""

import os
from typing import List, Dict, Any
from helpers.db_connection import DatabaseConnection
from helpers.sql_queries import get_table_schema

def generate_erd_dot(conn: DatabaseConnection, output_dir: str = "static/images") -> str:
    """
    Generate an ERD diagram in DOT format for the database schema.
    
    Args:
        conn: DatabaseConnection object
        output_dir: Directory to save the generated files
        
    Returns:
        str: Path to the generated DOT file
    """
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all tables and their schemas
    tables_info = {}
    foreign_keys = []
    
    # Query to get table information including foreign keys
    table_query = """
    SELECT 
        tc.table_schema, 
        tc.table_name, 
        kcu.column_name,
        ccu.table_schema AS foreign_table_schema,
        ccu.table_name AS foreign_table_name,
        ccu.column_name AS foreign_column_name
    FROM 
        information_schema.table_constraints AS tc 
        JOIN information_schema.key_column_usage AS kcu
            ON tc.constraint_name = kcu.constraint_name
            AND tc.table_schema = kcu.table_schema
        JOIN information_schema.constraint_column_usage AS ccu
            ON ccu.constraint_name = tc.constraint_name
            AND ccu.table_schema = tc.table_schema
    WHERE tc.constraint_type = 'FOREIGN KEY';
    """
    
    fk_results = conn.execute_query(table_query)
    
    # Generate DOT file content
    dot_content = [
        'digraph G {',
        '    rankdir=LR;',
        '    node [shape=record, style=filled, fillcolor=lightblue];',
        '    edge [arrowhead=crow, arrowtail=none];'
    ]
    
    # Add tables
    for table_name in conn.get_table_names():
        schema = get_table_schema(conn, table_name)
        tables_info[table_name] = schema
        
        # Format table columns for display
        columns = [f"{col['name']} : {col['type']}" for col in schema]
        table_label = f"{table_name}|{{'|'.join(columns)}}"
        
        dot_content.append(f'    "{table_name}" [label="{{{table_label}}}"];')
    
    # Add relationships from foreign keys
    for fk in fk_results:
        source_table = fk['table_name']
        target_table = fk['foreign_table_name']
        dot_content.append(f'    "{source_table}" -> "{target_table}";')
    
    dot_content.append('}')
    
    # Write DOT file
    dot_file_path = os.path.join(output_dir, "schema_erd.dot")
    with open(dot_file_path, 'w') as f:
        f.write('\n'.join(dot_content))
    
    return dot_file_path

def generate_erd_image(dot_file_path: str) -> str:
    """
    Generate a PNG image from the DOT file using Graphviz.
    
    Args:
        dot_file_path: Path to the DOT file
        
    Returns:
        str: Path to the generated PNG file
    """
    try:
        import graphviz
        
        # Read the DOT file
        with open(dot_file_path, 'r') as f:
            dot_content = f.read()
        
        # Create Graphviz object
        graph = graphviz.Source(dot_content)
        
        # Generate PNG file
        output_path = dot_file_path.replace('.dot', '.png')
        graph.render(output_path, format='png', cleanup=True)
        
        return output_path + '.png'
    except ImportError:
        print("Please install graphviz: pip install graphviz")
        return None 