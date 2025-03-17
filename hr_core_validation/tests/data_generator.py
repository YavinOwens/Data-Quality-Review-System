"""
Data generator for HR Core test data
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import uuid

class HRCoreDataGenerator:
    def __init__(self, output_dir: str = 'test_data'):
        """
        Initialize data generator
        
        Args:
            output_dir: Directory to save generated data
        """
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)
        
        # Constants for data generation
        self.departments = ['HR', 'IT', 'Finance', 'Sales', 'Marketing', 'Operations', 'Legal']
        self.job_titles = ['Manager', 'Director', 'Analyst', 'Specialist', 'Coordinator', 'Associate']
        self.countries = ['US', 'UK', 'Canada', 'Australia', 'Germany', 'France', 'Japan']
        self.contact_types = ['EMAIL', 'PHONE', 'MOBILE']
        self.address_types = ['HOME', 'WORK', 'MAILING']
        
    def generate_workers(self, num_workers: int = 100) -> pd.DataFrame:
        """Generate worker data"""
        data = {
            'PERSON_ID': range(1, num_workers + 1),
            'UNIQUE_ID': [str(uuid.uuid4()) for _ in range(num_workers)],  # Add unique identifier
            'EMPLOYEE_NUMBER': [f'EMP{str(i).zfill(5)}' for i in range(1, num_workers + 1)],
            'FIRST_NAME': [f'First{i}' for i in range(1, num_workers + 1)],
            'LAST_NAME': [f'Last{i}' for i in range(1, num_workers + 1)],
            'SEX': np.random.choice(['M', 'F', 'X'], num_workers),
            'DATE_OF_BIRTH': [
                (datetime.now() - timedelta(days=random.randint(7300, 21900))).strftime('%Y-%m-%d')
                for _ in range(num_workers)
            ],
            'NATIONAL_IDENTIFIER': [
                f'SSN{str(random.randint(100000000, 999999999))}'
                for _ in range(num_workers)
            ],
            'CURRENT_EMPLOYEE_FLAG': ['Y'] * num_workers,
            'EFFECTIVE_START_DATE': [
                (datetime.now() - timedelta(days=random.randint(0, 3650))).strftime('%Y-%m-%d')
                for _ in range(num_workers)
            ],
            'EFFECTIVE_END_DATE': [
                (datetime.now() + timedelta(days=random.randint(365, 3650))).strftime('%Y-%m-%d')
                for _ in range(num_workers)
            ]
        }
        
        # Add some invalid data for testing
        invalid_indices = random.sample(range(num_workers), 5)
        for idx in invalid_indices:
            if random.random() < 0.5:
                data['EMPLOYEE_NUMBER'][idx] = 'INVALID'  # Invalid employee number
            else:
                data['SEX'][idx] = 'Z'  # Invalid sex code
        
        return pd.DataFrame(data)
    
    def generate_assignments(self, workers_df: pd.DataFrame) -> pd.DataFrame:
        """Generate assignment data"""
        assignments = []
        for _, worker in workers_df.iterrows():
            # Generate 1-3 assignments per worker
            num_assignments = random.randint(1, 3)
            for i in range(num_assignments):
                start_date = datetime.strptime(worker['EFFECTIVE_START_DATE'], '%Y-%m-%d')
                end_date = datetime.strptime(worker['EFFECTIVE_END_DATE'], '%Y-%m-%d')
                
                # Generate random dates within worker's effective period
                assignment_start = start_date + timedelta(days=random.randint(0, 365))
                assignment_end = min(
                    end_date,
                    assignment_start + timedelta(days=random.randint(365, 1825))
                )
                
                assignments.append({
                    'ASSIGNMENT_ID': len(assignments) + 1,
                    'UNIQUE_ID': str(uuid.uuid4()),  # Add unique identifier
                    'PERSON_ID': worker['PERSON_ID'],
                    'WORKER_UNIQUE_ID': worker['UNIQUE_ID'],  # Link to worker's unique ID
                    'EFFECTIVE_START_DATE': assignment_start.strftime('%Y-%m-%d'),
                    'EFFECTIVE_END_DATE': assignment_end.strftime('%Y-%m-%d'),
                    'ASSIGNMENT_TYPE': random.choice(['FULL_TIME', 'PART_TIME', 'CONTRACT']),
                    'ASSIGNMENT_STATUS_TYPE_ID': 1,  # Active
                    'BUSINESS_GROUP_ID': random.randint(1, 5),
                    'ORGANIZATION_ID': random.randint(1, 10),
                    'JOB_ID': random.randint(1, 20),
                    'POSITION_ID': random.randint(1, 30),
                    'GRADE_ID': random.randint(1, 5)
                })
        
        # Add some invalid data for testing
        invalid_indices = random.sample(range(len(assignments)), 5)
        for idx in invalid_indices:
            assignments[idx]['ASSIGNMENT_STATUS_TYPE_ID'] = 999  # Invalid status
        
        return pd.DataFrame(assignments)
    
    def generate_addresses(self, workers_df: pd.DataFrame) -> pd.DataFrame:
        """Generate address data"""
        addresses = []
        for _, worker in workers_df.iterrows():
            # Generate 1-2 addresses per worker
            num_addresses = random.randint(1, 2)
            for i in range(num_addresses):
                start_date = datetime.strptime(worker['EFFECTIVE_START_DATE'], '%Y-%m-%d')
                end_date = datetime.strptime(worker['EFFECTIVE_END_DATE'], '%Y-%m-%d')
                
                # Generate random dates within worker's effective period
                address_start = start_date + timedelta(days=random.randint(0, 365))
                address_end = min(
                    end_date,
                    address_start + timedelta(days=random.randint(365, 1825))
                )
                
                addresses.append({
                    'ADDRESS_ID': len(addresses) + 1,
                    'UNIQUE_ID': str(uuid.uuid4()),  # Add unique identifier
                    'PERSON_ID': worker['PERSON_ID'],
                    'WORKER_UNIQUE_ID': worker['UNIQUE_ID'],  # Link to worker's unique ID
                    'EFFECTIVE_START_DATE': address_start.strftime('%Y-%m-%d'),
                    'EFFECTIVE_END_DATE': address_end.strftime('%Y-%m-%d'),
                    'ADDRESS_TYPE': random.choice(self.address_types),
                    'ADDRESS_LINE1': f'{random.randint(100, 9999)} Main St',
                    'CITY': f'City{random.randint(1, 100)}',
                    'POSTAL_CODE': f'{random.randint(10000, 99999)}',
                    'COUNTRY': random.choice(self.countries)
                })
        
        # Add some invalid data for testing
        invalid_indices = random.sample(range(len(addresses)), 5)
        for idx in invalid_indices:
            if random.random() < 0.5:
                addresses[idx]['ADDRESS_TYPE'] = 'INVALID'  # Invalid address type
            else:
                addresses[idx]['POSTAL_CODE'] = 'INVALID'  # Invalid postal code
        
        return pd.DataFrame(addresses)
    
    def generate_communications(self, workers_df: pd.DataFrame) -> pd.DataFrame:
        """Generate communication data"""
        communications = []
        for _, worker in workers_df.iterrows():
            # Generate 1-3 communication methods per worker
            num_communications = random.randint(1, 3)
            for i in range(num_communications):
                start_date = datetime.strptime(worker['EFFECTIVE_START_DATE'], '%Y-%m-%d')
                end_date = datetime.strptime(worker['EFFECTIVE_END_DATE'], '%Y-%m-%d')
                
                # Generate random dates within worker's effective period
                comm_start = start_date + timedelta(days=random.randint(0, 365))
                comm_end = min(
                    end_date,
                    comm_start + timedelta(days=random.randint(365, 1825))
                )
                
                contact_type = random.choice(self.contact_types)
                if contact_type == 'EMAIL':
                    contact_value = f'worker{worker["PERSON_ID"]}@company.com'
                elif contact_type == 'PHONE':
                    contact_value = f'+1-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}'
                else:  # MOBILE
                    contact_value = f'+1-{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}'
                
                communications.append({
                    'CONTACT_ID': len(communications) + 1,
                    'UNIQUE_ID': str(uuid.uuid4()),  # Add unique identifier
                    'PERSON_ID': worker['PERSON_ID'],
                    'WORKER_UNIQUE_ID': worker['UNIQUE_ID'],  # Link to worker's unique ID
                    'EFFECTIVE_START_DATE': comm_start.strftime('%Y-%m-%d'),
                    'EFFECTIVE_END_DATE': comm_end.strftime('%Y-%m-%d'),
                    'CONTACT_TYPE': contact_type,
                    'CONTACT_VALUE': contact_value,
                    'PRIMARY_FLAG': 'Y' if i == 0 else 'N'
                })
        
        # Add some invalid data for testing
        invalid_indices = random.sample(range(len(communications)), 5)
        for idx in invalid_indices:
            if random.random() < 0.5:
                communications[idx]['CONTACT_TYPE'] = 'INVALID'  # Invalid contact type
            else:
                communications[idx]['CONTACT_VALUE'] = 'invalid@email'  # Invalid email
        
        return pd.DataFrame(communications)
    
    def generate_all_data(self, num_workers: int = 100) -> None:
        """
        Generate all test data and save to files
        
        Args:
            num_workers: Number of workers to generate
        """
        # Generate data
        workers_df = self.generate_workers(num_workers)
        assignments_df = self.generate_assignments(workers_df)
        addresses_df = self.generate_addresses(workers_df)
        communications_df = self.generate_communications(workers_df)
        
        # Save to CSV files
        workers_df.to_csv(os.path.join(self.output_dir, 'workers.csv'), index=False)
        assignments_df.to_csv(os.path.join(self.output_dir, 'assignments.csv'), index=False)
        addresses_df.to_csv(os.path.join(self.output_dir, 'addresses.csv'), index=False)
        communications_df.to_csv(os.path.join(self.output_dir, 'communications.csv'), index=False)
        
        # Generate data documentation
        self.generate_documentation(workers_df, assignments_df, addresses_df, communications_df)
        
        print(f"Generated test data for {num_workers} workers:")
        print(f"- Workers: {len(workers_df)} records")
        print(f"- Assignments: {len(assignments_df)} records")
        print(f"- Addresses: {len(addresses_df)} records")
        print(f"- Communications: {len(communications_df)} records")
        print(f"\nData saved to: {self.output_dir}")
    
    def generate_documentation(self, workers_df: pd.DataFrame, assignments_df: pd.DataFrame,
                            addresses_df: pd.DataFrame, communications_df: pd.DataFrame) -> None:
        """
        Generate data documentation
        
        Args:
            workers_df: Workers DataFrame
            assignments_df: Assignments DataFrame
            addresses_df: Addresses DataFrame
            communications_df: Communications DataFrame
        """
        doc_dir = os.path.join(self.output_dir, 'documentation')
        os.makedirs(doc_dir, exist_ok=True)
        
        # Generate schema documentation
        with open(os.path.join(doc_dir, 'schema.md'), 'w') as f:
            f.write("# HR Core Data Schema Documentation\n\n")
            
            # Document Workers table
            f.write("## Workers Table\n")
            f.write("Primary key: PERSON_ID, UNIQUE_ID\n\n")
            f.write("| Column | Type | Description |\n")
            f.write("|--------|------|-------------|\n")
            f.write("| PERSON_ID | Integer | Primary identifier |\n")
            f.write("| UNIQUE_ID | UUID | Unique identifier for record linking |\n")
            f.write("| EMPLOYEE_NUMBER | String | Employee number |\n")
            f.write("| FIRST_NAME | String | First name |\n")
            f.write("| LAST_NAME | String | Last name |\n")
            f.write("| SEX | String | Gender (M/F/X) |\n")
            f.write("| DATE_OF_BIRTH | Date | Date of birth |\n")
            f.write("| NATIONAL_IDENTIFIER | String | National ID/SSN |\n")
            f.write("| CURRENT_EMPLOYEE_FLAG | String | Current employee indicator |\n")
            f.write("| EFFECTIVE_START_DATE | Date | Record start date |\n")
            f.write("| EFFECTIVE_END_DATE | Date | Record end date |\n\n")
            
            # Document Assignments table
            f.write("## Assignments Table\n")
            f.write("Primary key: ASSIGNMENT_ID, UNIQUE_ID\n")
            f.write("Foreign key: PERSON_ID, WORKER_UNIQUE_ID -> Workers(PERSON_ID, UNIQUE_ID)\n\n")
            f.write("| Column | Type | Description |\n")
            f.write("|--------|------|-------------|\n")
            f.write("| ASSIGNMENT_ID | Integer | Primary identifier |\n")
            f.write("| UNIQUE_ID | UUID | Unique identifier for record linking |\n")
            f.write("| PERSON_ID | Integer | Foreign key to Workers |\n")
            f.write("| WORKER_UNIQUE_ID | UUID | Foreign key to Workers |\n")
            f.write("| EFFECTIVE_START_DATE | Date | Record start date |\n")
            f.write("| EFFECTIVE_END_DATE | Date | Record end date |\n")
            f.write("| ASSIGNMENT_TYPE | String | Type of assignment |\n")
            f.write("| ASSIGNMENT_STATUS_TYPE_ID | Integer | Status identifier |\n")
            f.write("| BUSINESS_GROUP_ID | Integer | Business group identifier |\n")
            f.write("| ORGANIZATION_ID | Integer | Organization identifier |\n")
            f.write("| JOB_ID | Integer | Job identifier |\n")
            f.write("| POSITION_ID | Integer | Position identifier |\n")
            f.write("| GRADE_ID | Integer | Grade identifier |\n\n")
            
            # Document Addresses table
            f.write("## Addresses Table\n")
            f.write("Primary key: ADDRESS_ID, UNIQUE_ID\n")
            f.write("Foreign key: PERSON_ID, WORKER_UNIQUE_ID -> Workers(PERSON_ID, UNIQUE_ID)\n\n")
            f.write("| Column | Type | Description |\n")
            f.write("|--------|------|-------------|\n")
            f.write("| ADDRESS_ID | Integer | Primary identifier |\n")
            f.write("| UNIQUE_ID | UUID | Unique identifier for record linking |\n")
            f.write("| PERSON_ID | Integer | Foreign key to Workers |\n")
            f.write("| WORKER_UNIQUE_ID | UUID | Foreign key to Workers |\n")
            f.write("| EFFECTIVE_START_DATE | Date | Record start date |\n")
            f.write("| EFFECTIVE_END_DATE | Date | Record end date |\n")
            f.write("| ADDRESS_TYPE | String | Type of address |\n")
            f.write("| ADDRESS_LINE1 | String | Street address |\n")
            f.write("| CITY | String | City name |\n")
            f.write("| POSTAL_CODE | String | Postal/ZIP code |\n")
            f.write("| COUNTRY | String | Country code |\n\n")
            
            # Document Communications table
            f.write("## Communications Table\n")
            f.write("Primary key: CONTACT_ID, UNIQUE_ID\n")
            f.write("Foreign key: PERSON_ID, WORKER_UNIQUE_ID -> Workers(PERSON_ID, UNIQUE_ID)\n\n")
            f.write("| Column | Type | Description |\n")
            f.write("|--------|------|-------------|\n")
            f.write("| CONTACT_ID | Integer | Primary identifier |\n")
            f.write("| UNIQUE_ID | UUID | Unique identifier for record linking |\n")
            f.write("| PERSON_ID | Integer | Foreign key to Workers |\n")
            f.write("| WORKER_UNIQUE_ID | UUID | Foreign key to Workers |\n")
            f.write("| EFFECTIVE_START_DATE | Date | Record start date |\n")
            f.write("| EFFECTIVE_END_DATE | Date | Record end date |\n")
            f.write("| CONTACT_TYPE | String | Type of contact |\n")
            f.write("| CONTACT_VALUE | String | Contact information |\n")
            f.write("| PRIMARY_FLAG | String | Primary contact indicator |\n")
        
        # Generate data quality report
        with open(os.path.join(doc_dir, 'data_quality.md'), 'w') as f:
            f.write("# HR Core Data Quality Report\n\n")
            
            # Workers statistics
            f.write("## Workers Table Statistics\n")
            f.write(f"- Total records: {len(workers_df)}\n")
            f.write(f"- Unique PERSON_IDs: {workers_df['PERSON_ID'].nunique()}\n")
            f.write(f"- Unique UNIQUE_IDs: {workers_df['UNIQUE_ID'].nunique()}\n")
            f.write(f"- Current employees: {workers_df['CURRENT_EMPLOYEE_FLAG'].value_counts()['Y']}\n")
            f.write(f"- Gender distribution:\n")
            for sex, count in workers_df['SEX'].value_counts().items():
                f.write(f"  - {sex}: {count}\n")
            f.write("\n")
            
            # Assignments statistics
            f.write("## Assignments Table Statistics\n")
            f.write(f"- Total records: {len(assignments_df)}\n")
            f.write(f"- Unique ASSIGNMENT_IDs: {assignments_df['ASSIGNMENT_ID'].nunique()}\n")
            f.write(f"- Unique UNIQUE_IDs: {assignments_df['UNIQUE_ID'].nunique()}\n")
            f.write(f"- Assignments per worker: {len(assignments_df)/len(workers_df):.2f}\n")
            f.write(f"- Assignment types:\n")
            for type_, count in assignments_df['ASSIGNMENT_TYPE'].value_counts().items():
                f.write(f"  - {type_}: {count}\n")
            f.write("\n")
            
            # Addresses statistics
            f.write("## Addresses Table Statistics\n")
            f.write(f"- Total records: {len(addresses_df)}\n")
            f.write(f"- Unique ADDRESS_IDs: {addresses_df['ADDRESS_ID'].nunique()}\n")
            f.write(f"- Unique UNIQUE_IDs: {addresses_df['UNIQUE_ID'].nunique()}\n")
            f.write(f"- Addresses per worker: {len(addresses_df)/len(workers_df):.2f}\n")
            f.write(f"- Address types:\n")
            for type_, count in addresses_df['ADDRESS_TYPE'].value_counts().items():
                f.write(f"  - {type_}: {count}\n")
            f.write("\n")
            
            # Communications statistics
            f.write("## Communications Table Statistics\n")
            f.write(f"- Total records: {len(communications_df)}\n")
            f.write(f"- Unique CONTACT_IDs: {communications_df['CONTACT_ID'].nunique()}\n")
            f.write(f"- Unique UNIQUE_IDs: {communications_df['UNIQUE_ID'].nunique()}\n")
            f.write(f"- Communications per worker: {len(communications_df)/len(workers_df):.2f}\n")
            f.write(f"- Contact types:\n")
            for type_, count in communications_df['CONTACT_TYPE'].value_counts().items():
                f.write(f"  - {type_}: {count}\n")
        
        print(f"\nDocumentation generated in: {doc_dir}") 