#!/usr/bin/env python3
"""
Employee Data Transformation Script
This script transforms employee data from the HR database into various formats
for analysis and reporting purposes.
"""

import os
import pandas as pd
import cx_Oracle
from pathlib import Path
from dotenv import load_dotenv
import logging
from datetime import datetime

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('transform_employee_data.log'),
        logging.StreamHandler()
    ]
)

def get_db_connection():
    """Create a connection to the Oracle database."""
    load_dotenv()
    
    username = os.getenv('ORACLE_USER')
    password = os.getenv('ORACLE_PASSWORD')
    host = os.getenv('ORACLE_HOST', 'localhost')
    port = os.getenv('ORACLE_PORT', '1521')
    service_name = os.getenv('ORACLE_SERVICE', 'XEPDB1')
    
    connection_string = f"{host}:{port}/{service_name}"
    
    try:
        connection = cx_Oracle.connect(
            user=username,
            password=password,
            dsn=connection_string
        )
        return connection
    except Exception as e:
        logging.error(f"Error connecting to Oracle Database: {e}")
        raise

def extract_employee_data(connection):
    """Extract employee data from the database."""
    query = """
    SELECT 
        e.employee_id,
        e.first_name,
        e.last_name,
        e.email,
        e.phone_number,
        e.hire_date,
        j.job_title,
        d.department_name,
        e.salary,
        e.commission_pct
    FROM 
        hr.employees e
        LEFT JOIN hr.jobs j ON e.job_id = j.job_id
        LEFT JOIN hr.departments d ON e.department_id = d.department_id
    """
    
    try:
        df = pd.read_sql(query, connection)
        logging.info(f"Successfully extracted {len(df)} employee records")
        return df
    except Exception as e:
        logging.error(f"Error extracting employee data: {e}")
        raise

def transform_employee_data(df):
    """Transform the employee data."""
    # Create full name column
    df['full_name'] = df['first_name'] + ' ' + df['last_name']
    
    # Convert hire_date to datetime if it's not already
    df['hire_date'] = pd.to_datetime(df['hire_date'])
    
    # Calculate years of service
    df['years_of_service'] = (datetime.now() - df['hire_date']).dt.days / 365.25
    
    # Calculate total compensation
    df['total_compensation'] = df['salary'] * (1 + df['commission_pct'].fillna(0))
    
    # Create salary ranges
    df['salary_range'] = pd.cut(
        df['salary'],
        bins=[0, 30000, 60000, 90000, float('inf')],
        labels=['0-30K', '30K-60K', '60K-90K', '90K+']
    )
    
    logging.info("Successfully transformed employee data")
    return df

def save_transformed_data(df, output_dir):
    """Save transformed data to various formats."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Save as CSV
    csv_path = output_dir / f'employee_data_{timestamp}.csv'
    df.to_csv(csv_path, index=False)
    logging.info(f"Saved data to {csv_path}")
    
    # Save as Excel
    excel_path = output_dir / f'employee_data_{timestamp}.xlsx'
    df.to_excel(excel_path, index=False)
    logging.info(f"Saved data to {excel_path}")
    
    # Save as Parquet
    parquet_path = output_dir / f'employee_data_{timestamp}.parquet'
    df.to_parquet(parquet_path, index=False)
    logging.info(f"Saved data to {parquet_path}")

def main():
    """Main function to run the transformation process."""
    try:
        # Create database connection
        connection = get_db_connection()
        
        # Extract data
        df = extract_employee_data(connection)
        
        # Transform data
        df_transformed = transform_employee_data(df)
        
        # Save transformed data
        save_transformed_data(df_transformed, 'output/employee_data')
        
        connection.close()
        logging.info("Transformation process completed successfully")
        
    except Exception as e:
        logging.error(f"Error in transformation process: {e}")
        raise

if __name__ == "__main__":
    main() 