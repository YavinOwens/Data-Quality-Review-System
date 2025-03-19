#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Database HR Table Schema Web Viewer

This script provides a web interface to view the scraped Database HR table schemas.
"""

import os
import json
import glob
import re
import tempfile
import subprocess
from typing import List, Dict, Optional

import pandas as pd
import pydot
from reportlab.pdfgen import canvas
from reportlab.lib.pagesizes import A3, A2, A1, A0, landscape
from flask import Flask, render_template, request, jsonify, send_from_directory, redirect, flash, url_for, send_file, Response

app = Flask(__name__, static_folder='static')

# Create static directory if it doesn't exist
os.makedirs(os.path.join(os.path.dirname(__file__), 'static'), exist_ok=True)
os.makedirs(os.path.join(os.path.dirname(__file__), 'static', 'pdf_cache'), exist_ok=True)

# Set a secret key for session management and flash messages
app.secret_key = 'oracle_tables_viewer_secret_key'

# Directory where the scraped data is stored
BASE_DATA_DIR = 'oracle_tables'
TABLES_DIR = os.path.join(BASE_DATA_DIR, 'tables')
TEXT_INFO_DIR = os.path.join(BASE_DATA_DIR, 'text information')

# File storing ERD diagram URLs
ERD_FILE = 'oracle_hr_erd.csv'

# Directory where ERD diagrams are stored
ERD_DIR = 'erds'
BESPOKE_ERD_DIR = os.path.join(ERD_DIR, 'bespoke erds')


def get_available_tables() -> List[Dict]:
    """
    Get a list of available tables from the data directory.
    Shows only distinct tables based on analyzing table structure files.
    
    Returns:
        List[Dict]: List of table information dictionaries
    """
    tables = []
    table_mapping = {}
    
    # Check if directories exist
    if not os.path.exists(TABLES_DIR):
        return tables
    
    # First try to extract distinct table names from all_tables file if it exists
    distinct_tables = set()
    all_tables_files = sorted(glob.glob(os.path.join(TABLES_DIR, 'all_tables_*.csv')), reverse=True)
    
    if all_tables_files:
        # Use the most recent all_tables file
        try:
            all_tables_df = pd.read_csv(all_tables_files[0])
            if 'table_name' in all_tables_df.columns:
                distinct_tables = set(all_tables_df['table_name'].unique())
        except Exception as e:
            print(f"Error reading all_tables file: {e}")
    
    # Pattern to match table definition CSV files in the tables directory
    pattern = os.path.join(TABLES_DIR, '*.csv')
    
    # Exclude consolidated files
    files = [f for f in glob.glob(pattern) if 'all_tables_' not in f]
    
    # Map files to their corresponding tables
    for file_path in files:
        filename = os.path.basename(file_path)
        
        # Extract table name - need to handle tables with underscores in their names
        # First check if this is a known table from all_tables
        matched_table = None
        for table in distinct_tables:
            if filename.startswith(table + '_'):
                matched_table = table
                break
        
        # If no match found, use the traditional method (but this may not be accurate for tables with underscores)
        if matched_table is None:
            parts = filename.split('_')
            # Check if we have at least a table name and timestamp
            if len(parts) >= 2:
                # This is a heuristic - assuming the timestamp portion always starts with a date format
                # Try to find where the timestamp portion begins
                timestamp_start_idx = None
                for i, part in enumerate(parts):
                    if len(part) == 8 and part.isdigit():
                        timestamp_start_idx = i
                        break
                
                if timestamp_start_idx is not None:
                    # Table name is everything before the timestamp
                    matched_table = '_'.join(parts[:timestamp_start_idx])
                else:
                    # Fallback to first part if timestamp pattern not found
                    matched_table = parts[0]
        
        # Get file modification time
        mod_time = os.path.getmtime(file_path)
        
        # Check if we already have this table
        if matched_table in table_mapping:
            # Keep the most recent file for each table
            if mod_time > table_mapping[matched_table]['mod_time']:
                table_mapping[matched_table] = {
                    'file_path': file_path,
                    'mod_time': mod_time,
                    'filename': filename
                }
        else:
            table_mapping[matched_table] = {
                'file_path': file_path,
                'mod_time': mod_time,
                'filename': filename
            }
    
    # Now create table entries from the mapping
    for table_name, info in table_mapping.items():
        file_path = info['file_path']
        filename = info['filename']
        
        # Look for the info text file in the text information directory
        info_filename = filename.replace('.csv', '_info.txt')
        info_file = os.path.join(TEXT_INFO_DIR, info_filename)
        description = ""
        
        if os.path.exists(info_file):
            try:
                # Use the get_table_info function to properly parse the text file
                table_info = get_table_info(file_path)
                
                # Extract description from the parsed info
                if 'description' in table_info:
                    description = table_info['description']
                    
                # Clean up the description if necessary
                if description and description.lower() == 'nan':
                    description = ""
            except Exception as e:
                print(f"Error reading info file {info_file}: {e}")
        
        # Extract timestamp portion
        timestamp = filename.replace(table_name + '_', '').replace('.csv', '')
        
        tables.append({
            'name': table_name,
            'file_path': file_path,
            'timestamp': timestamp,
            'description': description,
            'mod_time': info['mod_time']
        })
    
    # Sort tables by modification time (newest first)
    tables.sort(key=lambda x: x['mod_time'], reverse=True)
    
    return tables


def get_table_schema(file_path: str) -> Optional[pd.DataFrame]:
    """
    Load the table schema from a CSV file.
    
    Args:
        file_path (str): Path to the CSV file
    
    Returns:
        Optional[pd.DataFrame]: DataFrame with the table schema or None if error
    """
    try:
        df = pd.read_csv(file_path)
        return df
    except Exception:
        return None


def get_table_info(file_path: str) -> Dict:
    """
    Load the table information from the info text file.
    
    Args:
        file_path (str): Path to the table CSV file
    
    Returns:
        Dict: Dictionary with table information
    """
    info = {}
    
    # Extract filename and construct path to the corresponding text info file
    filename = os.path.basename(file_path)
    info_filename = filename.replace('.csv', '_info.txt')
    info_file = os.path.join(TEXT_INFO_DIR, info_filename)
    
    if os.path.exists(info_file):
        try:
            # Read the text file content
            with open(info_file, 'r') as f:
                content = f.read()
            
            # Split the content into sections
            sections = []
            current_section = []
            
            for line in content.split('\n'):
                line = line.strip()
                # Skip empty lines, headers and separator lines
                if not line or line.startswith('=') or line.startswith('Information for'):
                    continue
                
                # Check if this line starts a new property section
                if line.lower().startswith('property:'):
                    if current_section:
                        sections.append(current_section)
                        current_section = []
                    current_section.append(line)
                else:
                    current_section.append(line)
            
            # Add the last section if any
            if current_section:
                sections.append(current_section)
            
            # Process each section
            for section in sections:
                if not section:
                    continue
                
                # First line should be the property name
                property_line = section[0]
                if ':' in property_line:
                    property_name = property_line.split(':', 1)[0].strip().lower()
                    
                    # Find the value line(s)
                    value_lines = []
                    for i, line in enumerate(section):
                        if i == 0:  # Skip property line
                            continue
                            
                        if line.lower().startswith('value:'):
                            # Extract the value part
                            value_lines.append(line.split(':', 1)[1].strip())
                        else:
                            # This is additional text, likely part of a multiline value
                            value_lines.append(line)
                    
                    # Combine value lines
                    value = ' '.join(value_lines).strip()
                    
                    # Clean up value
                    if value.startswith('[') and value.endswith(']'):
                        # Remove brackets for bracketed values
                        value = value[1:-1].strip()
                    
                    # Store property and value
                    info[property_name] = value
                
        except Exception as e:
            print(f"Error parsing info file {info_file}: {e}")
    
    return info


def get_local_erd_files() -> List[Dict]:
    """
    Get a list of available local ERD files from the ERD directory and bespoke ERD directory.
    
    Returns:
        List[Dict]: List of ERD diagram information dictionaries
    """
    diagrams = []
    
    # Check if the ERD directory exists
    if not os.path.exists(ERD_DIR):
        return diagrams
    
    # Get all PDF files in the main ERD directory
    pdf_files = glob.glob(os.path.join(ERD_DIR, '*.pdf'))
    
    # Add files from the bespoke ERD directory if it exists
    if os.path.exists(BESPOKE_ERD_DIR):
        bespoke_pdf_files = glob.glob(os.path.join(BESPOKE_ERD_DIR, '*.pdf'))
        pdf_files.extend(bespoke_pdf_files)
    
    for pdf_file in pdf_files:
        filename = os.path.basename(pdf_file)
        # Generate a slug for the URL based on the filename
        slug = os.path.splitext(filename)[0].replace(' ', '_').lower()
        # Generate a more readable name from the filename
        name = os.path.splitext(filename)[0].replace('_', ' ')
        
        # Add a prefix for bespoke ERDs to distinguish them
        is_bespoke = BESPOKE_ERD_DIR in pdf_file
        display_name = f"[Bespoke] {name}" if is_bespoke else name
        
        # Check if corresponding DOT file exists
        file_base = os.path.splitext(pdf_file)[0]
        has_dot_file = os.path.exists(f"{file_base}.dot")
        
        diagrams.append({
            'name': display_name,
            'slug': slug,
            'file_path': pdf_file,
            'description': f"{'Bespoke ' if is_bespoke else ''}ERD Diagram for {name}",
            'has_dot_file': has_dot_file,
            'dot_file_path': f"{file_base}.dot" if has_dot_file else None
        })
    
    return diagrams


def get_erd_diagrams() -> List[Dict]:
    """
    Get a list of available ERD diagrams - combining local files and ERD file.
    
    Returns:
        List[Dict]: List of ERD diagram information dictionaries
    """
    diagrams = get_local_erd_files()
    
    # Check if the ERD file exists
    if not os.path.exists(ERD_FILE):
        return diagrams
    
    try:
        df = pd.read_csv(ERD_FILE)
        
        # Convert DataFrame to list of dictionaries
        for _, row in df.iterrows():
            diagrams.append({
                'name': row['name'],
                'url': row['url'],
                'description': row['description'] if 'description' in row else '',
                'is_external': True
            })
            
        return diagrams
    except Exception as e:
        print(f"Error loading ERD diagrams: {e}")
        return diagrams  # Still return local diagrams even if the CSV loading fails


def map_tables_to_erds(tables: List[Dict], diagrams: List[Dict]) -> Dict[str, str]:
    """
    Create a mapping between table names and ERD diagrams.
    
    Args:
        tables (List[Dict]): List of table information dictionaries
        diagrams (List[Dict]): List of ERD diagram information dictionaries
    
    Returns:
        Dict[str, str]: Dictionary mapping table names to ERD diagram slugs
    """
    mapping = {}
    
    # Get local ERD files
    local_erds = get_local_erd_files()
    
    # PER table mapping for local ERD files
    per_mapping_patterns = {
        'PER_PEOPLE': 'r11i10_us_federal_human_resorces_erd62d3',
        'PER_ALL_PEOPLE': 'r11i10_us_federal_human_resorces_erd62d3',  # Added for PER_ALL_PEOPLE pattern
        'PER_ALL_ASSIGNMENTS': 'r11i10_us_federal_human_resorces_erd62d3',
        'PER_JOBS': 'r11i10_us_federal_human_resorces_erd62d3',
        'PER_POSITIONS': 'r11i10_us_federal_human_resorces_erd62d3',
        'PER_ABSENCE': 'r11i10_human_resources_absence_erda34d',
        'PER_PERFORMANCE': 'r12.1_performance_managementcd0d',
        'PER_RECRUIT': 'irecruitment_r12.1e0aa'
    }
    
    # Define specific table-to-ERD mappings
    specific_mappings = {
        'PER_PEOPLE_F': 'r11i10_us_federal_human_resorces_erd62d3',
        'PER_ALL_PEOPLE_F': 'r11i10_us_federal_human_resorces_erd62d3',  # Corrected mapping
        'PER_ALL_ASSIGNMENTS_F': 'r11i10_us_federal_human_resorces_erd62d3'
    }
    
    # Federal tables mapping
    federal_tables = ['GHR_', 'PQH_', 'FED_']
    for table in tables:
        table_name = table['name']
        for prefix in federal_tables:
            if table_name.startswith(prefix):
                specific_mappings[table_name] = 'r11i10_us_federal_human_resorces_erd62d3'
    
    # Create a list of available ERD slugs
    available_erd_slugs = [d.get('slug') for d in local_erds]
    
    # Add external ERDs by name for backward compatibility
    for diagram in diagrams:
        if diagram.get('is_external', False):
            available_erd_slugs.append(diagram['name'])
    
    for table in tables:
        table_name = table['name']
        
        # Check for specific mappings first
        if table_name in specific_mappings:
            erd_slug = specific_mappings[table_name]
            if erd_slug in available_erd_slugs:
                mapping[table_name] = erd_slug
            continue
        
        # For PER tables, use the pattern mapping
        if 'PER' in table_name:
            for pattern, erd_slug in per_mapping_patterns.items():
                if pattern in table_name and erd_slug in available_erd_slugs:
                    mapping[table_name] = erd_slug
                    break
        
        # Check if there's a local ERD that matches the table name
        if table_name not in mapping:
            for erd in local_erds:
                if table_name.lower() in erd['name'].lower():
                    mapping[table_name] = erd['slug']
                    break
    
    return mapping


# Define routes
def get_distinct_text_info():
    """
    Get a list of distinct text information files.
    
    Returns:
        List[Dict]: List of text information dictionaries
    """
    text_info_files = []
    
    # Check if directory exists
    if not os.path.exists(TEXT_INFO_DIR):
        return text_info_files
    
    # Pattern to match text info files
    pattern = os.path.join(TEXT_INFO_DIR, '*.txt')
    
    # Get all text info files
    files = glob.glob(pattern)
    
    # Group files by table name
    table_files = {}
    for file_path in files:
        filename = os.path.basename(file_path)
        
        # Extract table name - need to handle tables with underscores in their names
        parts = filename.split('_')
        
        # This is a heuristic - assuming the timestamp portion always starts with a date format
        # and info files typically end with _info.txt
        timestamp_start_idx = None
        for i, part in enumerate(parts):
            if len(part) == 8 and part.isdigit():
                timestamp_start_idx = i
                break
        
        if timestamp_start_idx is not None:
            # Table name is everything before the timestamp
            table_name = '_'.join(parts[:timestamp_start_idx])
        else:
            # Fallback to remove _info.txt suffix
            table_name = filename.replace('_info.txt', '')
        
        # Get file modification time
        mod_time = os.path.getmtime(file_path)
        
        # Check if we already have this table
        if table_name in table_files:
            # Keep the most recent file for each table
            if mod_time > table_files[table_name]['mod_time']:
                table_files[table_name] = {
                    'file_path': file_path,
                    'mod_time': mod_time,
                    'filename': filename
                }
        else:
            table_files[table_name] = {
                'file_path': file_path,
                'mod_time': mod_time,
                'filename': filename
            }
    
    # Now create entries for distinct text info files
    for table_name, info in table_files.items():
        file_path = info['file_path']
        
        # Read the content of the file to get a preview
        preview = ""
        try:
            with open(file_path, 'r') as f:
                # Just read the first few lines for preview
                for _ in range(5):
                    line = f.readline().strip()
                    if line and not line.startswith('=='):
                        preview += line + "\n"
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
        
        text_info_files.append({
            'name': table_name,
            'file_path': file_path,
            'preview': preview,
            'mod_time': info['mod_time']
        })
    
    # Sort files by name
    text_info_files.sort(key=lambda x: x['name'])
    
    return text_info_files


@app.route('/')
def index():
    """Render the index page with the list of available tables and ERD diagrams."""
    tables = get_available_tables()
    diagrams = get_erd_diagrams()
    
    # Create mapping between tables and ERDs
    table_erd_mapping = map_tables_to_erds(tables, diagrams)
    
    return render_template('index.html', tables=tables, diagrams=diagrams, table_erd_mapping=table_erd_mapping)


@app.route('/text-info')
def text_info():
    """Render the page with distinct text information files."""
    text_info_files = get_distinct_text_info()
    return render_template('text_info.html', text_info_files=text_info_files)


@app.route('/text-info/<table_name>')
def view_text_info(table_name):
    """
    Render the page with the full content of a specific text information file.
    
    Args:
        table_name (str): Name of the table to view text information for
    """
    # Find the most recent text information file for this table
    text_info_files = get_distinct_text_info()
    target_file = None
    
    for file in text_info_files:
        if file['name'] == table_name:
            target_file = file
            break
    
    if not target_file:
        flash(f"No text information file found for table {table_name}", "error")
        return redirect(url_for('text_info'))
    
    # Read the full content of the file
    try:
        with open(target_file['file_path'], 'r') as f:
            content = f.read()
    except Exception as e:
        flash(f"Error reading file: {e}", "error")
        return redirect(url_for('text_info'))
    
    return render_template('view_text_info.html', table_name=table_name, content=content)


@app.route('/erd/<diagram_slug>')
def view_erd(diagram_slug):
    """
    Serve or redirect to an ERD diagram.
    
    Args:
        diagram_slug (str): Slug of the diagram to view
    """
    diagrams = get_erd_diagrams()
    
    # Find the diagram in the list by slug or name
    diagram = next((d for d in diagrams if d.get('slug') == diagram_slug or d.get('name') == diagram_slug), None)
    
    if not diagram:
        return "ERD diagram not found", 404
    
    # Check if it's an external URL or local file
    if diagram.get('is_external', False):
        # Redirect to external URL
        return redirect(diagram['url'])
    else:
        # First check if there's a corresponding .dot file
        dot_file_path = None
        # Get base filepath without extension
        file_base = os.path.splitext(diagram['file_path'])[0]
        potential_dot_file = f"{file_base}.dot"
        
        if os.path.exists(potential_dot_file):
            dot_file_path = potential_dot_file
            dot_file_name = os.path.basename(dot_file_path)
            
            # Get file size for any metadata display
            file_size = os.path.getsize(dot_file_path) if os.path.exists(dot_file_path) else 0
            is_bespoke = 'bespoke' in dot_file_path.lower()
            
            # Check if user explicitly wants a specific view mode
            view_mode = request.args.get('mode', 'pdf').lower()
            
            # For full rendering mode with Graphviz, use the standard dot viewer
            if view_mode == 'graphviz' or (view_mode == 'full' and not request.args.get('pdf', False)):
                return render_template('dot_viewer.html',
                               diagram=diagram,
                               dot_path=f"/dot/{dot_file_name}",
                               show_simplified_option=True,
                               show_full_option=False,
                               file_size_mb=round(file_size/1024/1024, 1),
                               bespoke_diagram=is_bespoke)
            
            # For simplified mode, extract table names and relationships
            elif view_mode == 'simplified':
                try:
                    # Read the DOT file to extract table names and relationships
                    with open(dot_file_path, 'r') as f:
                        dot_content = f.read()
                    
                    # Extract table names using regex pattern
                    table_pattern = r'"([^"]+)"\s*\['
                    tables = sorted(set(re.findall(table_pattern, dot_content)))
                    
                    # Extract relationships
                    rel_pattern = r'"([^"]+)"\s*->\s*"([^"]+)"'
                    relationships = list(set(re.findall(rel_pattern, dot_content)))
                    
                    return render_template('simplified_erd.html',
                                       diagram=diagram,
                                       tables=tables,
                                       relationships=relationships,
                                       dot_path=f"/dot/{dot_file_name}")
                except Exception as e:
                    app.logger.error(f"Error extracting table info from DOT: {str(e)}")
                    # Fall back to PDF if simplified extraction fails
                    view_mode = 'pdf'
            
            # Default to PDF view (pre-rendered)
            if view_mode == 'pdf':
                # Create pdf_cache directory if it doesn't exist
                output_pdf_dir = os.path.join(app.static_folder, 'pdf_cache')
                os.makedirs(output_pdf_dir, exist_ok=True)
                
                # Use standard size for PDFs (A2)
                paper_size = request.args.get('size', 'A2').upper()
                dpi = int(request.args.get('dpi', '300'))
                
                pdf_filename = f"{os.path.splitext(os.path.basename(dot_file_path))[0]}_{paper_size}.pdf"
                pdf_path = os.path.join(output_pdf_dir, pdf_filename)
                
                # Generate PDF if it doesn't exist or if DOT file is newer
                if not os.path.exists(pdf_path) or os.path.getmtime(pdf_path) < os.path.getmtime(dot_file_path):
                    try:
                        # Map of available paper sizes
                        paper_sizes = {
                            'A4': '8.27x11.69',
                            'A3': '11.69x16.54',
                            'A2': '16.54x23.39',  # Default
                            'A1': '23.39x33.11',
                            'A0': '33.11x46.81'
                        }
                        
                        size_str = paper_sizes.get(paper_size, paper_sizes['A2'])
                        
                        # Generate the PDF using graphviz
                        graphviz_cmd = [
                            'dot', 
                            '-Tpdf', 
                            f'-Gsize={size_str}!', 
                            f'-Gdpi={dpi}',
                            '-Gmargin=0.5',
                            '-o', pdf_path, 
                            dot_file_path
                        ]
                        
                        # Try using system graphviz command
                        subprocess.run(graphviz_cmd, check=True)
                        app.logger.info(f"Generated PDF for direct viewing: {pdf_path}")
                    except Exception as e:
                        app.logger.error(f"Error generating PDF: {str(e)}")
                        # If PDF generation fails, fall back to graphviz view
                        return render_template('dot_viewer.html',
                                           diagram=diagram,
                                           dot_path=f"/dot/{dot_file_name}",
                                           show_simplified_option=True,
                                           show_full_option=False,
                                           file_size_mb=round(file_size/1024/1024, 1),
                                           bespoke_diagram=is_bespoke)
                
                # Serve the PDF using the pdf_viewer template
                pdf_url = f"/static/pdf_cache/{pdf_filename}"
                return render_template('pdf_viewer.html',
                                   diagram=diagram,
                                   pdf_path=pdf_url)
        
        # For local PDF files, render in our PDF viewer template
        elif diagram['file_path'].lower().endswith('.pdf'):
            return render_template('pdf_viewer.html', 
                                 diagram=diagram, 
                                 pdf_path=f"/pdf/{os.path.basename(diagram['file_path'])}")
        else:
            # Serve the local file directly
            return send_from_directory(os.path.dirname(diagram['file_path']), 
                                    os.path.basename(diagram['file_path']))


@app.route('/pdf/<filename>')
def serve_pdf(filename):
    """
    Serve the PDF file directly from either the main ERD directory or bespoke ERD directory.
    
    Args:
        filename (str): Name of the PDF file
    """
    # First, check if the file exists in the main ERD directory
    main_erd_path = os.path.join(ERD_DIR, filename)
    if os.path.exists(main_erd_path):
        return send_from_directory(ERD_DIR, filename)
    
    # If not found in the main directory, check the bespoke ERD directory
    bespoke_erd_path = os.path.join(BESPOKE_ERD_DIR, filename)
    if os.path.exists(bespoke_erd_path):
        return send_from_directory(BESPOKE_ERD_DIR, filename)
    
    # If file is not found in either directory, return a 404 error
    return f"ERD file '{filename}' not found", 404


@app.route('/export-pdf/<path:diagram_slug>')
def export_pdf(diagram_slug):
    """
    Export an ERD diagram as a high-quality PDF.
    
    Args:
        diagram_slug (str): Slug of the diagram to export as PDF.
    """
    # Get all available diagrams
    diagrams = get_erd_diagrams()
    
    # Find the requested diagram by slug
    diagram = next((d for d in diagrams if d.get('slug') == diagram_slug), None)
    
    if not diagram:
        return f"ERD diagram '{diagram_slug}' not found", 404
    
    # Get the DOT file path for the diagram
    dot_file_path = None
    file_base = os.path.splitext(diagram['file_path'])[0]
    potential_dot_file = f"{file_base}.dot"
    
    if os.path.exists(potential_dot_file):
        dot_file_path = potential_dot_file
    else:
        return f"DOT file for diagram '{diagram_slug}' not found", 404
    
    try:
        # Create high-quality PDF from the DOT file
        # Get desired paper size from request parameter (default to A2)
        paper_size = request.args.get('size', 'A2').upper()
        dpi = int(request.args.get('dpi', '300'))  # High DPI for quality
        
        # Map of available paper sizes
        paper_sizes = {
            'A4': '8.27x11.69',
            'A3': '11.69x16.54',
            'A2': '16.54x23.39',  # Default
            'A1': '23.39x33.11',
            'A0': '33.11x46.81'
        }
        
        size_str = paper_sizes.get(paper_size, paper_sizes['A2'])
        
        # Create a temporary file for the PDF
        with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as temp_pdf:
            pdf_path = temp_pdf.name
        
        # Generate the PDF using graphviz (better quality than pydot for complex diagrams)
        graphviz_cmd = [
            'dot', 
            '-Tpdf', 
            f'-Gsize={size_str}!', 
            f'-Gdpi={dpi}',
            '-Gmargin=0.5',
            '-o', pdf_path, 
            dot_file_path
        ]
        
        # Try using system graphviz command first
        try:
            subprocess.run(graphviz_cmd, check=True)
            app.logger.info(f"Generated PDF using system graphviz: {pdf_path}")
        except (subprocess.SubprocessError, FileNotFoundError):
            # Fall back to pydot if system graphviz is not available
            app.logger.info("Falling back to pydot for PDF generation")
            graph = pydot.graph_from_dot_file(dot_file_path)[0]
            graph.write_pdf(pdf_path)
        
        # Generate a nice filename for the PDF
        output_filename = os.path.splitext(os.path.basename(dot_file_path))[0] + f"_{paper_size}.pdf"
        
        # Set appropriate headers for PDF download
        return send_file(
            pdf_path,
            as_attachment=True,
            download_name=output_filename,
            mimetype='application/pdf'
        )
    except Exception as e:
        app.logger.error(f"Error creating PDF: {str(e)}")
        return f"Error creating PDF: {str(e)}", 500


# Dictionary to cache optimized DOT files
dot_file_cache = {}

@app.route('/dot/<filename>')
def serve_dot(filename):
    """
    Serve the DOT file directly from either the main ERD directory or bespoke ERD directory.
    Includes optimization for large DOT files to improve rendering performance.
    
    Args:
        filename (str): Name of the DOT file
    """
    # Check if we have a cached version
    if filename in dot_file_cache:
        app.logger.info(f"Serving cached version of {filename}")
        return dot_file_cache[filename]
    
    dot_file_path = None
    
    # First, check if the file exists in the main ERD directory
    main_erd_path = os.path.join(ERD_DIR, filename)
    if os.path.exists(main_erd_path):
        dot_file_path = main_erd_path
    
    # If not found in the main directory, check the bespoke ERD directory
    if not dot_file_path:
        bespoke_erd_path = os.path.join(BESPOKE_ERD_DIR, filename)
        if os.path.exists(bespoke_erd_path):
            dot_file_path = bespoke_erd_path
    
    # If file is not found in either directory, return a 404 error
    if not dot_file_path:
        return f"DOT file '{filename}' not found", 404
    
    try:
        # Get file stats
        file_stats = os.stat(dot_file_path)
        file_size = file_stats.st_size
        
        # Read the file content
        with open(dot_file_path, 'r') as f:
            content = f.read()
        
        # For large DOT files, perform optimizations
        if file_size > 1000000:  # Over 1MB
            app.logger.info(f"Optimizing large DOT file: {filename} ({file_size/1024/1024:.2f} MB)")
            
            # Start with basic whitespace optimization
            content = re.sub(r'\s+', ' ', content)
            
            # For extremely large files (usually bespoke tables)
            if file_size > 5000000 and 'bespoke' in dot_file_path:  # Over 5MB and from bespoke folder
                app.logger.info(f"Applying special optimization for very large bespoke diagram: {filename}")
                try:
                    # Attempt to simplify the diagram structure for faster rendering
                    # 1. Reduce unnecessary attributes
                    content = re.sub(r'fontname="Arial", fontsize=\d+', 'fontname="Arial"', content)
                    
                    # 2. Simplify cell padding/spacing (can greatly reduce SVG complexity)
                    content = re.sub(r'cellpadding="4"', 'cellpadding="2"', content)
                    content = re.sub(r'cellspacing="0"', 'cellspacing="0"', content)
                    
                    # 3. Add a special flag that the client-side can detect for further optimizations
                    if '  graph [' in content:
                        content = content.replace('  graph [', '  graph [bespoke_large_diagram=true, ')
                    
                    app.logger.info(f"Successfully optimized very large bespoke diagram: {filename}")
                except Exception as e:
                    app.logger.error(f"Error optimizing very large bespoke diagram: {str(e)}")
                    # Continue with the unoptimized version if optimization fails
        
        # Cache the file content with appropriate headers
        response = app.response_class(
            response=content,
            status=200,
            mimetype='text/vnd.graphviz'
        )
        
        # Set cache headers
        response.headers['Cache-Control'] = 'public, max-age=3600'  # Cache for 1 hour
        
        # Store in cache
        dot_file_cache[filename] = response
        
        return response
        
    except Exception as e:
        app.logger.error(f"Error serving DOT file {filename}: {str(e)}")
        return f"Error reading DOT file: {str(e)}", 500


@app.route('/erds')
def list_erds():
    """
    Render a page with all available ERD diagrams.
    """
    diagrams = get_erd_diagrams()
    return render_template('erds.html', diagrams=diagrams, with_search=True)


@app.route('/schemas')
def list_schemas():
    """
    Render a page with all table schemas.
    """
    tables = get_available_tables()
    return render_template('schemas.html', tables=tables)


@app.route('/table/<table_name>/erd')
def view_table_erd(table_name):
    """
    Redirect to the appropriate ERD for a specific table.
    
    Args:
        table_name (str): Name of the table to find ERD for
    """
    tables = get_available_tables()
    diagrams = get_erd_diagrams()
    local_diagrams = get_local_erd_files()
    
    # First, try to find a local ERD file that matches the table name
    for diagram in local_diagrams:
        if table_name.lower() in diagram['name'].lower():
            # Found a matching local ERD
            return redirect(f"/erd/{diagram['slug']}")
    
    # If no local match, use the mapping
    # Find the table in the list
    table = next((t for t in tables if t['name'] == table_name), None)
    
    if not table:
        return "Table not found", 404
    
    # Get the mapping between tables and ERDs
    table_erd_mapping = map_tables_to_erds(tables, diagrams)
    
    # Get the ERD for this table
    erd_name = table_erd_mapping.get(table_name)
    
    if not erd_name:
        return "No ERD available for this table", 404
    
    # Redirect to the ERD view
    return redirect(f"/erd/{erd_name}")


@app.route('/table/<table_name>')
def view_table(table_name):
    """
    Render the table view page for a specific table.
    
    Args:
        table_name (str): Name of the table to view
    """
    tables = get_available_tables()
    diagrams = get_erd_diagrams()
    
    # Find the table in the list
    table = next((t for t in tables if t['name'] == table_name), None)
    
    if not table:
        return "Table not found", 404
    
    schema = get_table_schema(table['file_path'])
    info = get_table_info(table['file_path'])
    
    if schema is None:
        return "Error loading table schema", 500
    
    # Check if an ERD is available for this table
    table_erd_mapping = map_tables_to_erds(tables, diagrams)
    erd_available = table_name in table_erd_mapping and table_erd_mapping[table_name] is not None
    
    # Convert schema to list of dictionaries for the template
    schema_dict = schema.to_dict('records')
    
    return render_template('table.html', 
                          table=table,
                          schema=schema_dict,
                          info=info,
                          erd_available=erd_available)


@app.route('/api/tables')
def api_tables():
    """API endpoint to get the list of available tables as JSON."""
    tables = get_available_tables()
    return jsonify(tables)


@app.route('/api/table/<table_name>')
def api_table(table_name):
    """
    API endpoint to get the schema of a table as JSON.
    
    Args:
        table_name (str): Name of the table to get
    """
    tables = get_available_tables()
    
    # Find the table in the list
    table = next((t for t in tables if t['name'] == table_name), None)
    
    if not table:
        return jsonify({"error": "Table not found"}), 404
    
    schema = get_table_schema(table['file_path'])
    info = get_table_info(table['file_path'])
    
    if schema is None:
        return jsonify({"error": "Error loading table schema"}), 500
    
    # Convert schema to list of dictionaries for JSON
    schema_dict = schema.to_dict('records')
    
    return jsonify({
        "table": table,
        "schema": schema_dict,
        "info": info
    })


# Create templates directory and templates
def create_templates():
    """Create the templates directory and template files if they don't exist."""
    templates_dir = os.path.join(os.path.dirname(__file__), 'templates')
    
    if not os.path.exists(templates_dir):
        os.makedirs(templates_dir)
    
    # Create index.html template
    index_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Database HR Table Schemas</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            .table-container {
                margin-top: 20px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="d-flex justify-content-between align-items-center mt-4 mb-4">
                <h1>Database HR Table Schemas</h1>
                <a href="/erds" class="btn btn-primary">View All ERD Diagrams</a>
            </div>
            
            <!-- ERD Diagrams Section -->
            {% if diagrams %}
            <div class="card mb-4">
                <div class="card-header bg-primary text-white">
                    <h4 class="mb-0">Entity Relationship Diagrams</h4>
                </div>
                <div class="card-body">
                    <div class="list-group">
                        {% for diagram in diagrams %}
                        <a href="/erd/{{ diagram.name }}" target="_blank" class="list-group-item list-group-item-action d-flex justify-content-between align-items-center">
                            <div>
                                <h5 class="mb-1">{{ diagram.name }}</h5>
                                <p class="mb-1">{{ diagram.description }}</p>
                            </div>
                            <span class="badge bg-primary rounded-pill">View ERD</span>
                        </a>
                        {% endfor %}
                    </div>
                </div>
            </div>
            {% endif %}
            
            <!-- Tables Section -->
            <div class="card">
                <div class="card-header bg-success text-white">
                    <h4 class="mb-0">Table Schemas</h4>
                </div>
                <div class="card-body">
                    <table class="table table-striped">
                        <thead>
                            <tr>
                                <th>Table Name</th>
                                <th>Description</th>
                                <th>Actions</th>
                            </tr>
                        </thead>
                        <tbody>
                            {% for table in tables %}
                            <tr>
                                <td>{{ table.name }}</td>
                                <td>{{ table.description }}</td>
                                <td>
                                    <div class="d-flex gap-2">
                                        <a href="/table/{{ table.name }}" class="btn btn-primary">View Schema</a>
                                        {% if table_erd_mapping and table_erd_mapping[table.name] %}
                                            <a href="/erd/{{ table_erd_mapping[table.name] }}" target="_blank" class="btn btn-success">View ERD</a>
                                        {% else %}
                                            <button class="btn btn-secondary" disabled>No ERD</button>
                                        {% endif %}
                                    </div>
                                </td>
                            </tr>
                            {% endfor %}
                        </tbody>
                    </table>
                </div>
            </div>
        </div>
        
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
    </body>
    </html>
    """
    
    index_template_path = os.path.join(templates_dir, 'index.html')
    if not os.path.exists(index_template_path):
        with open(index_template_path, 'w') as f:
            f.write(index_html)
    
    # Create table.html template
    table_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{{ table.name }} - Database HR Table Schema</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            .schema-container {
                margin-top: 20px;
            }
            .table-info {
                margin-bottom: 20px;
                padding: 15px;
                background-color: #f8f9fa;
                border-radius: 5px;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <h1 class="mt-4 mb-4">{{ table.name }} Schema</h1>
            
            <div class="table-info">
                <h4>Table Information</h4>
                <dl class="row">
                    {% if info.description %}
                    <dt class="col-sm-3">Description</dt>
                    <dd class="col-sm-9">{{ info.description }}</dd>
                    {% endif %}
                    
                    {% if info.owner %}
                    <dt class="col-sm-3">Owner/Schema</dt>
                    <dd class="col-sm-9">{{ info.owner }}</dd>
                    {% endif %}
                    
                    {% if info.scrape_date %}
                    <dt class="col-sm-3">Scraped On</dt>
                    <dd class="col-sm-9">{{ info.scrape_date }}</dd>
                    {% endif %}
                </dl>
            </div>
            
            <div class="d-flex gap-2 mb-3">
                <a href="/" class="btn btn-secondary">Back to Tables</a>
                {% if erd_available %}
                <a href="/table/{{ table.name }}/erd" target="_blank" class="btn btn-success">View ERD Diagram</a>
                {% else %}
                <button class="btn btn-secondary" disabled>No ERD Available</button>
                {% endif %}
            </div>
            
            <div class="schema-container">
                <h4>Column Definitions</h4>
                <table class="table table-striped">
                    <thead>
                        <tr>
                            <th>Column Name</th>
                            <th>Data Type</th>
                            <th>Nullable</th>
                            {% if schema and schema[0].pk is defined %}
                            <th>PK</th>
                            {% endif %}
                            {% if schema and schema[0].description is defined %}
                            <th>Description</th>
                            {% endif %}
                        </tr>
                    </thead>
                    <tbody>
                        {% for column in schema %}
                        <tr>
                            <td>{{ column.column_name }}</td>
                            <td>{{ column.data_type }}</td>
                            <td>{{ column.nullable }}</td>
                            {% if schema[0].pk is defined %}
                            <td>{{ column.pk }}</td>
                            {% endif %}
                            {% if schema[0].description is defined %}
                            <td>{{ column.description }}</td>
                            {% endif %}
                        </tr>
                        {% endfor %}
                    </tbody>
                </table>
            </div>
        </div>
        
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
    </body>
    </html>
    """
    
    table_template_path = os.path.join(templates_dir, 'table.html')
    if not os.path.exists(table_template_path):
        with open(table_template_path, 'w') as f:
            f.write(table_html)
            
    # Create erds.html template
    erds_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Database HR ERD Diagrams</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            .erd-card {
                margin-bottom: 20px;
                transition: transform 0.3s;
            }
            .erd-card:hover {
                transform: translateY(-5px);
                box-shadow: 0 10px 20px rgba(0,0,0,0.1);
            }
            .card-img-top {
                height: 200px;
                object-fit: cover;
                background-color: #f8f9fa;
                display: flex;
                align-items: center;
                justify-content: center;
            }
            .pdf-icon {
                font-size: 5rem;
                color: #dc3545;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="d-flex justify-content-between align-items-center mt-4 mb-4">
                <h1>Database HR ERD Diagrams</h1>
                <a href="/" class="btn btn-secondary">Back to Tables</a>
            </div>
            
            <div class="row">
                {% for diagram in diagrams %}
                <div class="col-md-4">
                    <div class="card erd-card">
                        <div class="card-img-top bg-light">
                            <div class="pdf-icon">
                                <i class="bi bi-file-earmark-pdf"></i>
                                <svg xmlns="http://www.w3.org/2000/svg" width="80" height="80" fill="currentColor" class="bi bi-file-earmark-pdf" viewBox="0 0 16 16">
                                    <path d="M14 14V4.5L9.5 0H4a2 2 0 0 0-2 2v12a2 2 0 0 0 2 2h8a2 2 0 0 0 2-2zM9.5 3A1.5 1.5 0 0 0 11 4.5h2V14a1 1 0 0 1-1 1H4a1 1 0 0 1-1-1V2a1 1 0 0 1 1-1h5.5v2z"/>
                                    <path d="M4.603 14.087a.81.81 0 0 1-.438-.42c-.195-.388-.13-.776.08-1.102.198-.307.526-.568.897-.787a7.68 7.68 0 0 1 1.482-.645 19.697 19.697 0 0 0 1.062-2.227 7.269 7.269 0 0 1-.43-1.295c-.086-.4-.119-.796-.046-1.136.075-.354.274-.672.65-.823.192-.077.4-.12.602-.077a.7.7 0 0 1 .477.365c.088.164.12.356.127.538.007.188-.012.396-.047.614-.084.51-.27 1.134-.52 1.794a10.954 10.954 0 0 0 .98 1.686 5.753 5.753 0 0 1 1.334.05c.364.066.734.195.96.465.12.144.193.32.2.518.007.192-.047.382-.138.563a1.04 1.04 0 0 1-.354.416.856.856 0 0 1-.51.138c-.331-.014-.654-.196-.933-.417a5.712 5.712 0 0 1-.911-.95 11.651 11.651 0 0 0-1.997.406 11.307 11.307 0 0 1-1.02 1.51c-.292.35-.609.656-.927.787a.793.793 0 0 1-.58.029zm1.379-1.901c-.166.076-.32.156-.459.238-.328.194-.541.383-.647.547-.094.145-.096.25-.04.361.01.022.02.036.026.044a.266.266 0 0 0 .035-.012c.137-.056.355-.235.635-.572a8.18 8.18 0 0 0 .45-.606zm1.64-1.33a12.71 12.71 0 0 1 1.01-.193 11.744 11.744 0 0 1-.51-.858 20.801 20.801 0 0 1-.5 1.05zm2.446.45c.15.163.296.3.435.41.24.19.407.253.498.256a.107.107 0 0 0 .07-.015.307.307 0 0 0 .094-.125.436.436 0 0 0 .059-.2.095.095 0 0 0-.026-.063c-.052-.062-.2-.152-.518-.209a3.876 3.876 0 0 0-.612-.053zM8.078 7.8a6.7 6.7 0 0 0 .2-.828c.031-.188.043-.343.038-.465a.613.613 0 0 0-.032-.198.517.517 0 0 0-.145.04c-.087.035-.158.106-.196.283-.04.192-.03.469.046.822.024.111.054.227.09.346z"/>
                                </svg>
                            </div>
                        </div>
                        <div class="card-body">
                            <h5 class="card-title">{{ diagram.name }}</h5>
                            <p class="card-text">{{ diagram.description }}</p>
                            {% if diagram.get('is_external', False) %}
                            <a href="{{ diagram.url }}" target="_blank" class="btn btn-primary w-100">View External ERD</a>
                            {% else %}
                            <a href="/erd/{{ diagram.slug }}" target="_blank" class="btn btn-primary w-100">View ERD</a>
                            {% endif %}
                        </div>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
    </body>
    </html>
    """
    
    erds_template_path = os.path.join(templates_dir, 'erds.html')
    if not os.path.exists(erds_template_path):
        with open(erds_template_path, 'w') as f:
            f.write(erds_html)
            
    # Create pdf_viewer.html template
    pdf_viewer_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>{{ diagram.name }} - ERD Viewer</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            body, html {
                height: 100%;
                margin: 0;
                padding: 0;
                overflow: hidden;
            }
            .header {
                background-color: #f8f9fa;
                padding: 10px 20px;
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            .pdf-container {
                height: calc(100vh - 60px);
                width: 100%;
                overflow: hidden;
            }
            iframe {
                border: none;
                width: 100%;
                height: 100%;
            }
            .controls {
                display: flex;
                gap: 10px;
            }
        </style>
    </head>
    <body>
        <div class="header">
            <div>
                <h4 class="mb-0">{{ diagram.name }}</h4>
                <p class="text-muted mb-0">{{ diagram.description }}</p>
            </div>
            <div class="controls">
                <a href="/erds" class="btn btn-secondary">Back to ERD List</a>
                <a href="/" class="btn btn-primary">Back to Tables</a>
            </div>
        </div>
        
        <div class="pdf-container">
            <iframe src="{{ pdf_path }}" type="application/pdf" width="100%" height="100%">
                <p>It appears your browser doesn't support embedded PDFs. <a href="{{ pdf_path }}">Click here to download the PDF</a>.</p>
            </iframe>
        </div>
        
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
    </body>
    </html>
    """
    
    pdf_viewer_template_path = os.path.join(templates_dir, 'pdf_viewer.html')
    if not os.path.exists(pdf_viewer_template_path):
        with open(pdf_viewer_template_path, 'w') as f:
            f.write(pdf_viewer_html)
            
    # Create schemas.html template
    schemas_html = """
    <!DOCTYPE html>
    <html lang="en">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <title>Database HR Table Schemas</title>
        <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/css/bootstrap.min.css" rel="stylesheet">
        <style>
            .schema-container {
                margin-top: 20px;
            }
            .table-schema {
                margin-bottom: 30px;
                border: 1px solid #dee2e6;
                border-radius: 8px;
                overflow: hidden;
            }
            .schema-header {
                background-color: #f8f9fa;
                padding: 15px;
                border-bottom: 1px solid #dee2e6;
            }
            .schema-body {
                padding: 15px;
            }
            .column-name {
                font-weight: bold;
            }
        </style>
    </head>
    <body>
        <div class="container">
            <div class="d-flex justify-content-between align-items-center mt-4 mb-4">
                <h1>All Table Schemas</h1>
                <div class="d-flex gap-2">
                    <a href="/" class="btn btn-secondary">Back to Home</a>
                    <a href="/erds" class="btn btn-primary">View All ERD Diagrams</a>
                </div>
            </div>
            
            <div id="tableFilter" class="mb-4">
                <div class="input-group">
                    <span class="input-group-text">Filter Tables</span>
                    <input type="text" id="filterInput" class="form-control" placeholder="Enter table name...">
                </div>
            </div>
            
            <div class="row">
                {% for table in tables %}
                <div class="col-12 table-item" data-table-name="{{ table.name }}">
                    <div class="table-schema">
                        <div class="schema-header d-flex justify-content-between align-items-center">
                            <div>
                                <h3>{{ table.name }}</h3>
                                <p class="text-muted mb-0">{{ table.description }}</p>
                            </div>
                            <a href="/table/{{ table.name }}" class="btn btn-primary">View Details</a>
                        </div>
                    </div>
                </div>
                {% endfor %}
            </div>
        </div>
        
        <script src="https://cdn.jsdelivr.net/npm/bootstrap@5.3.0-alpha1/dist/js/bootstrap.bundle.min.js"></script>
        <script>
            // Simple filter functionality
            document.getElementById('filterInput').addEventListener('keyup', function() {
                const filterValue = this.value.toLowerCase();
                const tableItems = document.querySelectorAll('.table-item');
                
                tableItems.forEach(item => {
                    const tableName = item.getAttribute('data-table-name').toLowerCase();
                    if (tableName.includes(filterValue)) {
                        item.style.display = '';
                    } else {
                        item.style.display = 'none';
                    }
                });
            });
        </script>
    </body>
    </html>
    """
    
    schemas_template_path = os.path.join(templates_dir, 'schemas.html')
    if not os.path.exists(schemas_template_path):
        with open(schemas_template_path, 'w') as f:
            f.write(schemas_html)


if __name__ == '__main__':
    import argparse
    parser = argparse.ArgumentParser(description='Database HR Table Schema Web Viewer')
    parser.add_argument('--port', type=int, default=5001, help='Port number to run the server on')
    args = parser.parse_args()
    
    # Create the data directory if it doesn't exist
    if not os.path.exists(BASE_DATA_DIR):
        os.makedirs(BASE_DATA_DIR)
    if not os.path.exists(TABLES_DIR):
        os.makedirs(TABLES_DIR)
    if not os.path.exists(TEXT_INFO_DIR):
        os.makedirs(TEXT_INFO_DIR)
    
    # Create the ERD directory and bespoke ERD directory if they don't exist
    if not os.path.exists(ERD_DIR):
        os.makedirs(ERD_DIR)
    if not os.path.exists(BESPOKE_ERD_DIR):
        os.makedirs(BESPOKE_ERD_DIR)
    
    # Create templates
    create_templates()
    
    port = args.port
    print(f"Starting server on port {port}")
    app.run(debug=True, port=port)
