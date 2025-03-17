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

def validate_dataframe(df: pd.DataFrame, table_name: str) -> list:
    """
    Validate a DataFrame using pandera
    
    Args:
        df: DataFrame to validate
        table_name: Name of the table
        
    Returns:
        List of validation results
    """
    validation_results = []
    
    # Define common schema
    common_schema = pa.DataFrameSchema({
        "UNIQUE_ID": pa.Column(str, nullable=False, unique=True),
    })
    
    # Define table-specific schemas
    if table_name == 'workers':
        schema = pa.DataFrameSchema({
            "UNIQUE_ID": pa.Column(str, nullable=False, unique=True),
            "PERSON_ID": pa.Column(str, nullable=True),
            "FIRST_NAME": pa.Column(str, nullable=False),
            "LAST_NAME": pa.Column(str, nullable=False),
            "BIRTH_DATE": pa.Column(str, nullable=True, checks=pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")),
            "NATIONALITY": pa.Column(str, nullable=True, checks=pa.Check.str_length(2, 3)),
            "EFFECTIVE_FROM": pa.Column(str, nullable=True, checks=pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")),
            "EFFECTIVE_TO": pa.Column(str, nullable=True, checks=pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")),
        })
    elif table_name == 'addresses':
        schema = pa.DataFrameSchema({
            "UNIQUE_ID": pa.Column(str, nullable=False, unique=True),
            "WORKER_UNIQUE_ID": pa.Column(str, nullable=False),
            "ADDRESS_TYPE": pa.Column(str, nullable=False, checks=pa.Check.isin(["HOME", "WORK", "OTHER"])),
            "ADDRESS_LINE1": pa.Column(str, nullable=True),
            "ADDRESS_LINE2": pa.Column(str, nullable=True),
            "CITY": pa.Column(str, nullable=True),
            "STATE": pa.Column(str, nullable=True),
            "POSTAL_CODE": pa.Column(pa.String, nullable=False, checks=pa.Check.str_length(5, 10)),
            "EFFECTIVE_FROM": pa.Column(str, nullable=True, checks=pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")),
            "EFFECTIVE_TO": pa.Column(str, nullable=True, checks=pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")),
        })
    elif table_name == 'communications':
        schema = pa.DataFrameSchema({
            "UNIQUE_ID": pa.Column(str, nullable=False, unique=True),
            "WORKER_UNIQUE_ID": pa.Column(str, nullable=False),
            "CONTACT_TYPE": pa.Column(str, nullable=False, checks=pa.Check.isin(["EMAIL", "PHONE"])),
            "CONTACT_VALUE": pa.Column(str, nullable=False),
            "EFFECTIVE_FROM": pa.Column(str, nullable=True, checks=pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")),
            "EFFECTIVE_TO": pa.Column(str, nullable=True, checks=pa.Check.str_matches(r"^\d{4}-\d{2}-\d{2}$")),
        })
    else:
        schema = common_schema
    
    try:
        # Convert postal codes to strings if they exist
        if 'POSTAL_CODE' in df.columns:
            df['POSTAL_CODE'] = df['POSTAL_CODE'].astype(str)
        
        # Validate the DataFrame
        schema.validate(df)
        validation_results.append({
            'check': 'schema_validation',
            'success': True,
            'details': 'All schema validations passed'
        })
    except pa.errors.SchemaError as e:
        validation_results.append({
            'check': 'schema_validation',
            'success': False,
            'details': f"Schema validation failed: {str(e)}"
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

def generate_html_report(validation_results: dict, output_dir: str) -> None:
    """
    Generate HTML validation report with enhanced visualizations using Jinja2 template
    
    Args:
        validation_results: Dictionary of validation results by table
        output_dir: Directory to save the report
    """
    # Create a summary report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_path = os.path.join(output_dir, f'validation_summary_{timestamp}.html')
    
    # Calculate overall statistics
    total_checks = sum(len(results) for results in validation_results.values())
    passed_checks = sum(
        sum(1 for r in results if r.get('success', False))
        for results in validation_results.values()
    )
    
    success_rate = (passed_checks / total_checks * 100) if total_checks > 0 else 0
    
    # Create success rate gauge
    fig_gauge = go.Figure(go.Indicator(
        mode="gauge+number",
        value=success_rate,
        domain={'x': [0, 1], 'y': [0, 1]},
        title={'text': "Overall Success Rate"},
        gauge={
            'axis': {'range': [0, 100]},
            'bar': {'color': "darkblue"},
            'steps': [
                {'range': [0, 60], 'color': "red"},
                {'range': [60, 80], 'color': "yellow"},
                {'range': [80, 100], 'color': "green"}
            ],
            'threshold': {
                'line': {'color': "black", 'width': 4},
                'thickness': 0.75,
                'value': success_rate
            }
        }
    ))
    
    # Create validation results by table
    tables = list(validation_results.keys())
    passed_by_table = [
        sum(1 for r in validation_results[t] if r.get('success', False)) / len(validation_results[t]) * 100
        for t in tables
    ]
    
    fig_bars = go.Figure(data=[
        go.Bar(
            name='Success Rate',
            x=tables,
            y=passed_by_table,
            marker_color=['green' if v >= 80 else 'yellow' if v >= 60 else 'red' for v in passed_by_table]
        )
    ])
    fig_bars.update_layout(
        title='Validation Results by Table',
        yaxis_title='Success Rate (%)',
        showlegend=False
    )
    
    # Set up Jinja2 environment
    env = Environment(
        loader=FileSystemLoader('hr_core_validation/templates'),
        autoescape=True
    )
    template = env.get_template('validation_report.html')
    
    # Prepare template variables
    template_vars = {
        'generation_time': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        'total_checks': total_checks,
        'passed_checks': passed_checks,
        'success_rate': success_rate,
        'validation_results': validation_results,
        'plotly_gauge': f"var gauge_data = {fig_gauge.to_json()};",
        'plotly_bars': f"var bars_data = {fig_bars.to_json()};"
    }
    
    # Render template and save report
    with open(report_path, 'w') as f:
        f.write(template.render(**template_vars))

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
        
        # Generate profile report
        profile_path = generate_profile_report(df, table_name, docs_dir)
        validation_results[table_name].append({
            'check_type': 'profile_report',
            'status': 'success' if profile_path != "Profile report generation failed" else 'failure',
            'details': f"Profile report: {profile_path}"
        })
        
        print(f"✓ {table_name} validation completed")
    
    # Create relationship graphs
    create_relationship_graphs(validation_results)
    
    # Generate HTML report
    generate_html_report(validation_results, docs_dir)
    
    return validation_results

if __name__ == '__main__':
    run_validation() 