"""
Data generator for HR Core test data
"""

import os
import pandas as pd
import uuid
import random
from datetime import datetime, timedelta
from faker import Faker

def generate_test_data(num_workers: int = 100) -> None:
    """
    Generate test data for HR Core validation
    
    Args:
        num_workers: Number of worker records to generate
    """
    # Create Faker instance
    fake = Faker()
    
    # Create test_data directory if it doesn't exist
    os.makedirs('test_data', exist_ok=True)
    
    # Generate worker data
    workers = []
    for _ in range(num_workers):
        worker = {
            'UNIQUE_ID': str(uuid.uuid4()),
            'PERSON_ID': f"P{fake.random_number(digits=6, fix_len=True)}",
            'EMPLOYEE_NUMBER': f"EMP{fake.random_number(digits=6, fix_len=True)}",
            'FIRST_NAME': fake.first_name(),
            'LAST_NAME': fake.last_name(),
            'BIRTH_DATE': fake.date_of_birth(minimum_age=18, maximum_age=65).strftime('%Y-%m-%d'),
            'SEX': random.choice(['M', 'F', 'O']),
            'MARITAL_STATUS': random.choice(['S', 'M', 'D', 'W']),
            'NATIONALITY': fake.country_code(),
            'EFFECTIVE_FROM': fake.date_between(start_date='-5y').strftime('%Y-%m-%d'),
            'EFFECTIVE_TO': fake.date_between(start_date='+1y', end_date='+5y').strftime('%Y-%m-%d')
        }
        workers.append(worker)
    
    # Generate assignment data
    assignments = []
    for worker in workers:
        num_assignments = random.randint(1, 3)
        for _ in range(num_assignments):
            assignment = {
                'UNIQUE_ID': str(uuid.uuid4()),
                'WORKER_UNIQUE_ID': worker['UNIQUE_ID'],
                'ASSIGNMENT_NUMBER': f"ASG{fake.random_number(digits=3, fix_len=True)}",
                'POSITION_ID': f"POS{fake.random_number(digits=4, fix_len=True)}",
                'DEPARTMENT_ID': f"DEP{fake.random_number(digits=3, fix_len=True)}",
                'LOCATION_ID': f"LOC{fake.random_number(digits=3, fix_len=True)}",
                'EFFECTIVE_FROM': fake.date_between(start_date='-5y').strftime('%Y-%m-%d'),
                'EFFECTIVE_TO': fake.date_between(start_date='+1y', end_date='+5y').strftime('%Y-%m-%d')
            }
            assignments.append(assignment)
    
    # Generate address data
    addresses = []
    for worker in workers:
        num_addresses = random.randint(1, 2)
        for _ in range(num_addresses):
            address = {
                'UNIQUE_ID': str(uuid.uuid4()),
                'WORKER_UNIQUE_ID': worker['UNIQUE_ID'],
                'ADDRESS_TYPE': random.choice(['HOME', 'WORK', 'OTHER']),
                'ADDRESS_LINE1': fake.street_address(),
                'ADDRESS_LINE2': fake.secondary_address() if random.random() > 0.5 else None,
                'CITY': fake.city(),
                'STATE': fake.state(),
                'POSTAL_CODE': str(fake.postcode()),  # Convert to string
                'COUNTRY': fake.country_code(),
                'EFFECTIVE_FROM': fake.date_between(start_date='-5y').strftime('%Y-%m-%d'),
                'EFFECTIVE_TO': fake.date_between(start_date='+1y', end_date='+5y').strftime('%Y-%m-%d')
            }
            addresses.append(address)
    
    # Generate communication data
    communications = []
    for worker in workers:
        num_communications = random.randint(1, 3)
        has_email = False
        has_phone = False
        
        for i in range(num_communications):
            # Ensure at least one email and one phone
            if i == 0:
                contact_type = 'EMAIL'
                contact_value = fake.email()
                has_email = True
            elif i == 1 and not has_phone:
                contact_type = 'PHONE'
                contact_value = f"+1{fake.msisdn()[3:]}"  # Generate phone number in +1XXXXXXXXXX format
                has_phone = True
            else:
                if not has_email:
                    contact_type = 'EMAIL'
                    contact_value = fake.email()
                    has_email = True
                elif not has_phone:
                    contact_type = 'PHONE'
                    contact_value = f"+1{fake.msisdn()[3:]}"
                    has_phone = True
                else:
                    contact_type = random.choice(['EMAIL', 'PHONE'])
                    contact_value = fake.email() if contact_type == 'EMAIL' else f"+1{fake.msisdn()[3:]}"
            
            communication = {
                'UNIQUE_ID': str(uuid.uuid4()),
                'WORKER_UNIQUE_ID': worker['UNIQUE_ID'],
                'CONTACT_TYPE': contact_type,
                'CONTACT_VALUE': contact_value,
                'PRIMARY_FLAG': 'Y' if i == 0 else 'N',
                'EFFECTIVE_FROM': fake.date_between(start_date='-5y').strftime('%Y-%m-%d'),
                'EFFECTIVE_TO': fake.date_between(start_date='+1y', end_date='+5y').strftime('%Y-%m-%d')
            }
            communications.append(communication)
    
    # Convert to DataFrames
    workers_df = pd.DataFrame(workers)
    assignments_df = pd.DataFrame(assignments)
    addresses_df = pd.DataFrame(addresses)
    communications_df = pd.DataFrame(communications)
    
    # Ensure all columns are strings
    for df in [workers_df, assignments_df, addresses_df, communications_df]:
        for col in df.columns:
            df[col] = df[col].astype(str)
    
    # Save to CSV files
    workers_df.to_csv('test_data/workers.csv', index=False)
    assignments_df.to_csv('test_data/assignments.csv', index=False)
    addresses_df.to_csv('test_data/addresses.csv', index=False)
    communications_df.to_csv('test_data/communications.csv', index=False)
    
    print(f"\nGenerated test data for {num_workers} workers:")
    print(f"- Workers: {len(workers)} records")
    print(f"- Assignments: {len(assignments)} records")
    print(f"- Addresses: {len(addresses)} records")
    print(f"- Communications: {len(communications)} records")
    print("\nData saved to: test_data")

if __name__ == '__main__':
    generate_test_data() 