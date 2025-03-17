"""
Test script for HR Core validation using pandera and ydata-profiling
"""

import os
import pandas as pd
import pandera as pa
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime
import glob
import re
from sqlalchemy import create_engine, MetaData, Table, Column, String, Date, ForeignKey
import sadisplay
import numpy as np
from ydata_profiling import ProfileReport
from typing import Dict, Any, List
from pathlib import Path
import ydata_profiling
from jinja2 import Environment, FileSystemLoader
from ..utils.profiling import generate_profile_report, validate_data_for_profiling
import jinja2

def validate_dataframe(df: pd.DataFrame, table_name: str) -> List[Dict[str, Any]]:
    """
    Validate a dataframe against predefined schemas and checks.
    
    Args:
        df (pd.DataFrame): The dataframe to validate
        table_name (str): The name of the table being validated
        
    Returns:
        List[Dict[str, Any]]: List of validation results
    """
    validation_results = []
    
    # Common schema for all tables
    common_schema = {
        "UNIQUE_ID": pa.Column(str, name="UNIQUE_ID", nullable=False, unique=True)
    }
    
    # Table-specific schemas
    table_schemas = {
        'workers': {
            **common_schema,
            "FIRST_NAME": pa.Column(str, name="FIRST_NAME", nullable=False),
            "LAST_NAME": pa.Column(str, name="LAST_NAME", nullable=False),
            "EMAIL": pa.Column(str, name="EMAIL", nullable=False, checks=pa.Check.str_matches(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$')),
            "PHONE": pa.Column(str, name="PHONE", nullable=True, checks=pa.Check.str_matches(r'^\+?1?\d{9,15}$')),
            "HIRE_DATE": pa.Column(str, name="HIRE_DATE", nullable=False, checks=pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$')),
            "TERMINATION_DATE": pa.Column(str, name="TERMINATION_DATE", nullable=True, checks=pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$')),
            "DEPARTMENT": pa.Column(str, name="DEPARTMENT", nullable=False),
            "POSITION": pa.Column(str, name="POSITION", nullable=False)
        },
        'addresses': {
            **common_schema,
            "WORKER_UNIQUE_ID": pa.Column(str, name="WORKER_UNIQUE_ID", nullable=False),
            "ADDRESS_TYPE": pa.Column(str, name="ADDRESS_TYPE", nullable=False, checks=pa.Check.isin(['HOME', 'WORK', 'OTHER'])),
            "ADDRESS_LINE1": pa.Column(str, name="ADDRESS_LINE1", nullable=False),
            "ADDRESS_LINE2": pa.Column(str, name="ADDRESS_LINE2", nullable=True),
            "CITY": pa.Column(str, name="CITY", nullable=False),
            "STATE": pa.Column(str, name="STATE", nullable=False),
            "POSTAL_CODE": pa.Column(str, name="POSTAL_CODE", nullable=False, checks=pa.Check.str_matches(r'^\d{5}(-\d{4})?$')),
            "EFFECTIVE_FROM": pa.Column(str, name="EFFECTIVE_FROM", nullable=False, checks=pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$')),
            "EFFECTIVE_TO": pa.Column(str, name="EFFECTIVE_TO", nullable=True, checks=pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$'))
        },
        'communications': {
            **common_schema,
            "WORKER_UNIQUE_ID": pa.Column(str, name="WORKER_UNIQUE_ID", nullable=False),
            "CONTACT_TYPE": pa.Column(str, name="CONTACT_TYPE", nullable=False, checks=pa.Check.isin(['EMAIL', 'PHONE', 'SMS'])),
            "CONTACT_VALUE": pa.Column(str, name="CONTACT_VALUE", nullable=False),
            "PRIMARY_FLAG": pa.Column(bool, name="PRIMARY_FLAG", nullable=False),
            "EFFECTIVE_FROM": pa.Column(str, name="EFFECTIVE_FROM", nullable=False, checks=pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$')),
            "EFFECTIVE_TO": pa.Column(str, name="EFFECTIVE_TO", nullable=True, checks=pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$'))
        },
        'assignments': {
            **common_schema,
            "WORKER_UNIQUE_ID": pa.Column(str, name="WORKER_UNIQUE_ID", nullable=False),
            "ASSIGNMENT_TYPE": pa.Column(str, name="ASSIGNMENT_TYPE", nullable=False, checks=pa.Check.isin(['REGULAR', 'TEMPORARY', 'CONTRACT'])),
            "START_DATE": pa.Column(str, name="START_DATE", nullable=False, checks=pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$')),
            "END_DATE": pa.Column(str, name="END_DATE", nullable=True, checks=pa.Check.str_matches(r'^\d{4}-\d{2}-\d{2}$')),
            "DEPARTMENT": pa.Column(str, name="DEPARTMENT", nullable=False),
            "POSITION": pa.Column(str, name="POSITION", nullable=False),
            "LOCATION": pa.Column(str, name="LOCATION", nullable=False)
        }
    }
    
    if table_name not in table_schemas:
        validation_results.append({
            "type": "schema",
            "success": False,
            "message": f"No schema defined for table {table_name}"
        })
        return validation_results
    
    schema = pa.DataFrameSchema(table_schemas[table_name])
    
    try:
        # Validate the entire schema
        schema.validate(df)
        validation_results.append({
            "type": "schema",
            "success": True,
            "message": "All schema validations passed"
        })
    except pa.errors.SchemaError as e:
        validation_results.append({
            "type": "schema",
            "success": False,
            "message": str(e)
        })
    
    # Validate each column individually
    for col_name, col_schema in table_schemas[table_name].items():
        try:
            if col_name in df.columns:
                # Create a single-column schema for validation
                single_col_schema = pa.DataFrameSchema({
                    col_name: col_schema
                })
                single_col_schema.validate(df[[col_name]])
                validation_results.append({
                    "type": "column",
                    "column": col_name,
                    "success": True,
                    "message": "All validations passed"
                })
            else:
                validation_results.append({
                    "type": "column",
                    "column": col_name,
                    "success": False,
                    "message": f"Column {col_name} not found in dataframe"
                })
        except pa.errors.SchemaError as e:
            validation_results.append({
                "type": "column",
                "column": col_name,
                "success": False,
                "message": str(e)
            })
    
    return validation_results

def create_relationship_graphs(validation_results: dict) -> str:
    """
    Create ER diagram using sadisplay
    
    Args:
        validation_results: Dictionary of validation results by table
    
    Returns:
        Path to the generated ER diagram
    """
    # Create SQLAlchemy metadata
    metadata = MetaData()
    
    # Define Workers table
    workers = Table('Workers', metadata,
        Column('UNIQUE_ID', String, primary_key=True),
        Column('PERSON_ID', String),
        Column('FIRST_NAME', String),
        Column('LAST_NAME', String),
        Column('BIRTH_DATE', Date),
        Column('NATIONALITY', String),
        Column('EFFECTIVE_FROM', Date),
        Column('EFFECTIVE_TO', Date)
    )
    
    # Define Assignments table
    assignments = Table('Assignments', metadata,
        Column('UNIQUE_ID', String, primary_key=True),
        Column('WORKER_UNIQUE_ID', String, ForeignKey('Workers.UNIQUE_ID')),
        Column('POSITION', String),
        Column('DEPARTMENT', String),
        Column('EFFECTIVE_FROM', Date),
        Column('EFFECTIVE_TO', Date)
    )
    
    # Define Addresses table
    addresses = Table('Addresses', metadata,
        Column('UNIQUE_ID', String, primary_key=True),
        Column('WORKER_UNIQUE_ID', String, ForeignKey('Workers.UNIQUE_ID')),
        Column('ADDRESS_TYPE', String),
        Column('ADDRESS_LINE1', String),
        Column('ADDRESS_LINE2', String),
        Column('CITY', String),
        Column('STATE', String),
        Column('POSTAL_CODE', String),
        Column('EFFECTIVE_FROM', Date),
        Column('EFFECTIVE_TO', Date)
    )
    
    # Define Communications table
    communications = Table('Communications', metadata,
        Column('UNIQUE_ID', String, primary_key=True),
        Column('WORKER_UNIQUE_ID', String, ForeignKey('Workers.UNIQUE_ID')),
        Column('CONTACT_TYPE', String),
        Column('CONTACT_VALUE', String),
        Column('EFFECTIVE_FROM', Date),
        Column('EFFECTIVE_TO', Date)
    )
    
    # Create the ER diagram description
    desc = sadisplay.describe([
        workers,
        assignments,
        addresses,
        communications
    ])
    
    # Save the ER diagram to a file
    diagram_path = 'hr_core_validation/documentation/er_diagram.dot'
    os.makedirs(os.path.dirname(diagram_path), exist_ok=True)
    
    with open(diagram_path, 'w') as f:
        f.write(sadisplay.dot(desc))
    
    # Convert dot file to PNG using graphviz
    os.system(f'dot -Tpng {diagram_path} -o hr_core_validation/documentation/er_diagram.png')
    
    return 'er_diagram.png'

def generate_profile_report(df: pd.DataFrame, table_name: str, output_dir: str) -> str:
    """Generate a profile report for a DataFrame using ydata-profiling."""
    try:
        profile = ProfileReport(df, title=f"{table_name} Profile Report")
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_path = os.path.join(output_dir, f'{table_name}_profile_{timestamp}.html')
        profile.to_file(report_path)
        return os.path.basename(report_path)
    except Exception as e:
        print(f"Warning: Could not generate profile report for {table_name}: {str(e)}")
        return "Profile report generation failed"

def generate_html_report(validation_results: Dict[str, List[Dict[str, Any]]], output_dir: str) -> str:
    """
    Generate an HTML report from validation results.
    
    Args:
        validation_results (Dict[str, List[Dict[str, Any]]]): Dictionary of validation results by table
        output_dir (str): Directory to save the report
        
    Returns:
        str: Path to the generated report
    """
    # Load the template
    template_loader = jinja2.FileSystemLoader(searchpath=os.path.join(os.path.dirname(__file__), '..', 'templates'))
    template_env = jinja2.Environment(loader=template_loader)
    template = template_env.get_template('validation_report.html')
    
    # Calculate validation statistics
    total_tables = len(validation_results)
    passed_tables = sum(1 for table_results in validation_results.values() 
                       if all(r['success'] for r in table_results if r['type'] == 'schema'))
    failed_tables = total_tables - passed_tables
    
    # Get profile report paths
    profile_reports = {}
    for table_name in validation_results.keys():
        profile_path = os.path.join(output_dir, f"{table_name}_profile_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
        if os.path.exists(profile_path):
            profile_reports[table_name] = os.path.basename(profile_path)
    
    # Prepare template variables
    template_vars = {
        'validation_results': validation_results,
        'total_tables': total_tables,
        'passed_tables': passed_tables,
        'failed_tables': failed_tables,
        'profile_reports': profile_reports,
        'er_diagram_path': 'er_diagram.png' if os.path.exists(os.path.join(output_dir, 'er_diagram.png')) else None,
        'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    }
    
    # Generate the report
    report_path = os.path.join(output_dir, f"validation_summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.html")
    with open(report_path, 'w') as f:
        f.write(template.render(**template_vars))
    
    return report_path

def run_validation(input_dir: str = 'hr_core_validation/data_sources', output_dir: str = '.') -> Dict[str, Any]:
    """
    Run validation on all CSV files in the input directory
    
    Args:
        input_dir: Directory containing CSV files (defaults to hr_core_validation/data_sources)
        output_dir: Directory to save validation results
        
    Returns:
        Dictionary containing validation results
    """
    validation_results = {}
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    # Create documentation directory if it doesn't exist
    docs_dir = os.path.join(output_dir, 'hr_core_validation', 'documentation')
    os.makedirs(docs_dir, exist_ok=True)
    
    # Get list of CSV files
    csv_files = [f for f in os.listdir(input_dir) if f.endswith('.csv')]
    
    if not csv_files:
        print(f"No CSV files found in {input_dir}")
        return validation_results
    
    # Process each CSV file
    for csv_file in csv_files:
        table_name = csv_file.replace('.csv', '')
        print(f"✓ Validating {table_name}")
        
        # Read CSV file
        df = pd.read_csv(os.path.join(input_dir, csv_file))
        
        # Validate DataFrame
        validation_results[table_name] = validate_dataframe(df, table_name)
        
        # Generate profile report if data is valid
        if validate_data_for_profiling(df):
            profile_path = generate_profile_report(df, table_name, docs_dir)
            validation_results[table_name].append({
                'check': 'profile_report',
                'success': profile_path is not None,
                'details': profile_path if profile_path else "Profile report generation failed"
            })
        else:
            validation_results[table_name].append({
                'check': 'profile_report',
                'success': False,
                'details': "Data validation failed - cannot generate profile report"
            })
        
        print(f"✓ {table_name} validation completed")
    
    # Create relationship graphs
    create_relationship_graphs(validation_results)
    
    # Generate HTML report
    generate_html_report(validation_results, docs_dir)
    
    return validation_results

if __name__ == '__main__':
    run_validation() 