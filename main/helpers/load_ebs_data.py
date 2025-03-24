import pandas as pd
from db_connection import DatabaseConnection
import os
from datetime import datetime

def create_tables():
    """Create the necessary tables in the database."""
    try:
        db = DatabaseConnection()
        
        # Read the SQL script
        with open('create_ebs_tables.sql', 'r') as f:
            sql_commands = f.read()
        
        # Split the commands and execute each one
        for command in sql_commands.split(';'):
            if command.strip():
                try:
                    db.execute(command)
                except Exception as e:
                    print(f"Error executing SQL command: {str(e)}")
                    print(f"Command: {command}")
        
        print("Tables created successfully")
        
    except Exception as e:
        print(f"Error creating tables: {str(e)}")

def load_addresses():
    """Load addresses data from CSV file."""
    try:
        # Read addresses.csv
        addresses_df = pd.read_csv('../data_sources/addresses.csv')
        
        # Convert date columns to datetime
        date_columns = ['EFFECTIVE_FROM', 'EFFECTIVE_TO']
        for col in date_columns:
            addresses_df[col] = pd.to_datetime(addresses_df[col])
        
        # Rename columns to match Oracle table
        addresses_df.columns = [col.upper() for col in addresses_df.columns]
        
        # Insert data into Oracle
        db = DatabaseConnection()
        db.insert_dataframe(addresses_df, 'ADDRESSES')
        print(f"Loaded {len(addresses_df)} addresses")
        
    except Exception as e:
        print(f"Error loading addresses: {str(e)}")

def load_assignments():
    """Load assignments data from CSV file."""
    try:
        # Read assignments.csv
        assignments_df = pd.read_csv('../data_sources/assignments.csv')
        
        if not assignments_df.empty:
            # Convert date columns to datetime
            date_columns = ['EFFECTIVE_FROM', 'EFFECTIVE_TO']
            for col in date_columns:
                assignments_df[col] = pd.to_datetime(assignments_df[col])
            
            # Rename columns to match Oracle table
            assignments_df.columns = [col.upper() for col in assignments_df.columns]
            
            # Insert data into Oracle
            db = DatabaseConnection()
            db.insert_dataframe(assignments_df, 'ASSIGNMENTS')
            print(f"Loaded {len(assignments_df)} assignments")
        else:
            print("No assignments data to load")
            
    except Exception as e:
        print(f"Error loading assignments: {str(e)}")

def load_communications():
    """Load communications data from CSV file."""
    try:
        # Read communications.csv
        communications_df = pd.read_csv('../data_sources/communications.csv')
        
        # Convert date columns to datetime
        date_columns = ['EFFECTIVE_FROM', 'EFFECTIVE_TO']
        for col in date_columns:
            communications_df[col] = pd.to_datetime(communications_df[col])
        
        # Rename columns to match Oracle table
        communications_df.columns = [col.upper() for col in communications_df.columns]
        
        # Insert data into Oracle
        db = DatabaseConnection()
        db.insert_dataframe(communications_df, 'COMMUNICATIONS')
        print(f"Loaded {len(communications_df)} communications")
        
    except Exception as e:
        print(f"Error loading communications: {str(e)}")

def load_workers():
    """Load workers data from CSV file."""
    try:
        # Read workers.csv
        workers_df = pd.read_csv('../data_sources/workers.csv')
        
        # Convert date columns to datetime
        date_columns = ['BIRTH_DATE', 'EFFECTIVE_FROM', 'EFFECTIVE_TO']
        for col in date_columns:
            workers_df[col] = pd.to_datetime(workers_df[col])
        
        # Rename columns to match Oracle table
        workers_df.columns = [col.upper() for col in workers_df.columns]
        
        # Insert data into Oracle
        db = DatabaseConnection()
        db.insert_dataframe(workers_df, 'WORKERS')
        print(f"Loaded {len(workers_df)} workers")
        
    except Exception as e:
        print(f"Error loading workers: {str(e)}")

def main():
    """Main function to load all data."""
    print("Starting data load process...")
    
    # Create tables first
    create_tables()
    
    # Load data for each table
    load_addresses()
    load_assignments()
    load_communications()
    load_workers()
    
    print("Data load process completed.")

if __name__ == "__main__":
    main() 