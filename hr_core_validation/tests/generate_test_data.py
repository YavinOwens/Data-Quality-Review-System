"""
Script to generate sample test data for HR Core validation
"""

import pandas as pd
import numpy as np
from faker import Faker
from datetime import datetime, timedelta
import os

def generate_test_data(output_dir: str):
    """Generate sample test data for HR Core validation."""
    fake = Faker()
    
    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)
    
    # Generate workers data
    n_workers = 100
    workers_data = []
    for i in range(n_workers):
        worker_id = f"W{i+1:04d}"
        birth_date = fake.date_of_birth(minimum_age=18, maximum_age=65)
        effective_from = fake.date_between(start_date='-5y', end_date='today')
        effective_to = fake.date_between(start_date=effective_from, end_date='+5y') if np.random.random() > 0.7 else None
        
        workers_data.append({
            'UNIQUE_ID': worker_id,
            'PERSON_ID': f"P{i+1:04d}",
            'FIRST_NAME': fake.first_name(),
            'LAST_NAME': fake.last_name(),
            'BIRTH_DATE': birth_date.strftime('%Y-%m-%d'),
            'NATIONALITY': fake.country_code(),
            'EFFECTIVE_FROM': effective_from.strftime('%Y-%m-%d'),
            'EFFECTIVE_TO': effective_to.strftime('%Y-%m-%d') if effective_to else None
        })
    
    # Generate addresses data
    addresses_data = []
    for worker in workers_data:
        n_addresses = np.random.randint(1, 4)
        for i in range(n_addresses):
            effective_from = fake.date_between(start_date='-5y', end_date='today')
            effective_to = fake.date_between(start_date=effective_from, end_date='+5y') if np.random.random() > 0.7 else None
            
            addresses_data.append({
                'UNIQUE_ID': f"A{len(addresses_data)+1:04d}",
                'WORKER_UNIQUE_ID': worker['UNIQUE_ID'],
                'ADDRESS_TYPE': np.random.choice(['HOME', 'WORK', 'OTHER']),
                'ADDRESS_LINE1': fake.street_address(),
                'ADDRESS_LINE2': fake.secondary_address() if np.random.random() > 0.5 else None,
                'CITY': fake.city(),
                'STATE': fake.state(),
                'POSTAL_CODE': fake.postcode(),
                'EFFECTIVE_FROM': effective_from.strftime('%Y-%m-%d'),
                'EFFECTIVE_TO': effective_to.strftime('%Y-%m-%d') if effective_to else None
            })
    
    # Generate communications data
    communications_data = []
    for worker in workers_data:
        n_contacts = np.random.randint(1, 3)
        for i in range(n_contacts):
            effective_from = fake.date_between(start_date='-5y', end_date='today')
            effective_to = fake.date_between(start_date=effective_from, end_date='+5y') if np.random.random() > 0.7 else None
            
            contact_type = np.random.choice(['EMAIL', 'PHONE'])
            contact_value = fake.email() if contact_type == 'EMAIL' else fake.phone_number()
            
            communications_data.append({
                'UNIQUE_ID': f"C{len(communications_data)+1:04d}",
                'WORKER_UNIQUE_ID': worker['UNIQUE_ID'],
                'CONTACT_TYPE': contact_type,
                'CONTACT_VALUE': contact_value,
                'EFFECTIVE_FROM': effective_from.strftime('%Y-%m-%d'),
                'EFFECTIVE_TO': effective_to.strftime('%Y-%m-%d') if effective_to else None
            })
    
    # Generate assignments data
    assignments_data = []
    positions = ['Manager', 'Developer', 'Analyst', 'Designer', 'Engineer']
    departments = ['IT', 'HR', 'Finance', 'Marketing', 'Operations']
    
    for worker in workers_data:
        n_assignments = np.random.randint(1, 3)
        for i in range(n_assignments):
            effective_from = fake.date_between(start_date='-5y', end_date='today')
            effective_to = fake.date_between(start_date=effective_from, end_date='+5y') if np.random.random() > 0.7 else None
            
            assignments_data.append({
                'UNIQUE_ID': f"AS{len(assignments_data)+1:04d}",
                'WORKER_UNIQUE_ID': worker['UNIQUE_ID'],
                'POSITION': np.random.choice(positions),
                'DEPARTMENT': np.random.choice(departments),
                'EFFECTIVE_FROM': effective_from.strftime('%Y-%m-%d'),
                'EFFECTIVE_TO': effective_to.strftime('%Y-%m-%d') if effective_to else None
            })
    
    # Save data to CSV files
    pd.DataFrame(workers_data).to_csv(os.path.join(output_dir, 'workers.csv'), index=False)
    pd.DataFrame(addresses_data).to_csv(os.path.join(output_dir, 'addresses.csv'), index=False)
    pd.DataFrame(communications_data).to_csv(os.path.join(output_dir, 'communications.csv'), index=False)
    pd.DataFrame(assignments_data).to_csv(os.path.join(output_dir, 'assignments.csv'), index=False)
    
    print(f"Generated test data in {output_dir}:")
    print(f"- {len(workers_data)} workers")
    print(f"- {len(addresses_data)} addresses")
    print(f"- {len(communications_data)} communications")
    print(f"- {len(assignments_data)} assignments")

if __name__ == '__main__':
    generate_test_data('test_data') 