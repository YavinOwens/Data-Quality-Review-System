#%% [markdown]
# Worker Queries
# This script contains various queries related to worker information

#%% [markdown]
## Import required modules
#%%
from db_connection import query_to_dataframe
import pandas as pd

#%% [markdown]
## Basic Worker Information

#%%
def get_all_workers():
    """Get basic information for all workers"""
    query = """
    SELECT 
        w.first_name,
        w.last_name,
        w.employee_number,
        w.birth_date,
        w.nationality
    FROM workers w
    ORDER BY w.last_name, w.first_name;
    """
    return query_to_dataframe(query)

#%% [markdown]
## Worker Contact Information

#%%
def get_worker_contacts():
    """Get contact information for all workers"""
    query = """
    SELECT 
        w.first_name,
        w.last_name,
        c.contact_type,
        c.contact_value,
        c.primary_flag
    FROM workers w
    JOIN communications c ON w.unique_id = c.worker_unique_id
    ORDER BY w.last_name, w.first_name, c.contact_type;
    """
    return query_to_dataframe(query)

#%% [markdown]
## Worker Addresses

#%%
def get_worker_addresses():
    """Get address information for all workers"""
    query = """
    SELECT 
        w.first_name,
        w.last_name,
        a.address_type,
        a.address_line1,
        a.city,
        a.state,
        a.country
    FROM workers w
    JOIN addresses a ON w.unique_id = a.worker_unique_id
    ORDER BY w.last_name, w.first_name, a.address_type;
    """
    return query_to_dataframe(query)

#%% [markdown]
## Worker Assignments

#%%
def get_worker_assignments():
    """Get assignment information for all workers"""
    query = """
    SELECT 
        w.first_name,
        w.last_name,
        a.assignment_number,
        a.position_id,
        a.department_id,
        a.location_id,
        a.effective_from,
        a.effective_to
    FROM workers w
    JOIN assignments a ON w.unique_id = a.worker_unique_id
    ORDER BY w.last_name, w.first_name, a.effective_from;
    """
    return query_to_dataframe(query)

#%% [markdown]
## Example Usage

#%%
if __name__ == "__main__":
    # Get all workers
    workers_df = get_all_workers()
    print("\nWorker Information:")
    print(workers_df.head())
    
    # Get worker contacts
    contacts_df = get_worker_contacts()
    print("\nWorker Contacts:")
    print(contacts_df.head())
    
    # Get worker addresses
    addresses_df = get_worker_addresses()
    print("\nWorker Addresses:")
    print(addresses_df.head())
    
    # Get worker assignments
    assignments_df = get_worker_assignments()
    print("\nWorker Assignments:")
    print(assignments_df.head()) 