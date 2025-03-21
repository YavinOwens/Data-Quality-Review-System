#!/usr/bin/env python3
import os
import sys
import cx_Oracle
from pathlib import Path
from dotenv import load_dotenv

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
        print(f"Error connecting to Oracle Database: {e}")
        sys.exit(1)

def run_validation_script(connection, script_path):
    """Execute a PL-SQL validation script."""
    try:
        with open(script_path, 'r') as file:
            script_content = file.read()
        
        cursor = connection.cursor()
        cursor.execute(script_content)
        connection.commit()
        
        print(f"Successfully executed {script_path}")
        return True
    except Exception as e:
        print(f"Error executing {script_path}: {e}")
        return False

def main():
    """Main function to run all validation scripts."""
    connection = get_db_connection()
    validation_dir = Path(__file__).parent.parent / 'validations'
    
    # List of validation scripts in order of execution
    validation_scripts = [
        'validations.sql',
        'data_quality_review.sql',
        'address_validation.sql',
        'date_of_birth_validation.sql',
        'email_validation.sql',
        'job_history_validation.sql',
        'name_field_validation.sql',
        'national_insurance_validation.sql'
    ]
    
    success = True
    for script in validation_scripts:
        script_path = validation_dir / script
        if script_path.exists():
            if not run_validation_script(connection, script_path):
                success = False
                break
        else:
            print(f"Warning: {script} not found")
    
    connection.close()
    
    if success:
        print("\nAll validation scripts executed successfully!")
    else:
        print("\nValidation process failed. Please check the errors above.")
        sys.exit(1)

if __name__ == "__main__":
    main() 