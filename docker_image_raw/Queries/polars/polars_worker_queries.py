#%% [markdown]
# Worker Queries using Polars
# This script contains various queries related to worker information using Polars

#%% [markdown]
## Import required modules
#%%
from polars_connection import read_table
import polars as pl

#%% [markdown]
## Load DataFrames
#%%
def load_dataframes():
    """Load all required DataFrames"""
    workers_df = read_table("workers")
    addresses_df = read_table("addresses")
    communications_df = read_table("communications")
    assignments_df = read_table("assignments")
    return workers_df, addresses_df, communications_df, assignments_df

#%% [markdown]
## Basic Worker Information

#%%
def get_all_workers(workers_df):
    """Get basic information for all workers"""
    return (workers_df
            .select([
                "first_name",
                "last_name",
                "employee_number",
                "birth_date",
                "nationality"
            ])
            .sort(["last_name", "first_name"]))

#%% [markdown]
## Worker Contact Information

#%%
def get_worker_contacts(workers_df, communications_df):
    """Get contact information for all workers"""
    return (workers_df
            .join(communications_df, 
                  left_on="unique_id",
                  right_on="worker_unique_id")
            .select([
                "first_name",
                "last_name",
                "contact_type",
                "contact_value",
                "primary_flag"
            ])
            .sort(["last_name", "first_name", "contact_type"]))

#%% [markdown]
## Worker Addresses

#%%
def get_worker_addresses(workers_df, addresses_df):
    """Get address information for all workers"""
    return (workers_df
            .join(addresses_df,
                  left_on="unique_id",
                  right_on="worker_unique_id")
            .select([
                "first_name",
                "last_name",
                "address_type",
                "address_line1",
                "city",
                "state",
                "country"
            ])
            .sort(["last_name", "first_name", "address_type"]))

#%% [markdown]
## Worker Assignments

#%%
def get_worker_assignments(workers_df, assignments_df):
    """Get assignment information for all workers"""
    return (workers_df
            .join(assignments_df,
                  left_on="unique_id",
                  right_on="worker_unique_id")
            .select([
                "first_name",
                "last_name",
                "assignment_number",
                "position_id",
                "department_id",
                "location_id",
                "effective_from",
                "effective_to"
            ])
            .sort(["last_name", "first_name", "effective_from"]))

#%% [markdown]
## Worker Complete Profile

#%%
def get_worker_complete_profile(workers_df, addresses_df, communications_df, assignments_df):
    """Get complete profile for all workers"""
    current_date = pl.col("effective_to") > pl.datetime("now")
    
    return (workers_df
            .join(addresses_df,
                  left_on="unique_id",
                  right_on="worker_unique_id",
                  how="left")
            .join(communications_df,
                  left_on="unique_id",
                  right_on="worker_unique_id",
                  how="left")
            .join(assignments_df,
                  left_on="unique_id",
                  right_on="worker_unique_id",
                  how="left")
            .filter(
                (pl.col("addresses.effective_to") > pl.datetime("now")) &
                (pl.col("communications.effective_to") > pl.datetime("now")) &
                (pl.col("assignments.effective_to") > pl.datetime("now"))
            )
            .select([
                "first_name",
                "last_name",
                "employee_number",
                "birth_date",
                "nationality",
                "sex",
                "marital_status",
                "address_line1",
                "city",
                "state",
                "country",
                "contact_type",
                "contact_value",
                "assignment_number",
                "position_id",
                "department_id"
            ])
            .sort(["last_name", "first_name"]))

#%% [markdown]
## Example Usage

#%%
if __name__ == "__main__":
    try:
        # Load DataFrames
        workers_df, addresses_df, communications_df, assignments_df = load_dataframes()
        
        # Get all workers
        print("\nWorker Information:")
        print(get_all_workers(workers_df).head())
        
        # Get worker contacts
        print("\nWorker Contacts:")
        print(get_worker_contacts(workers_df, communications_df).head())
        
        # Get worker addresses
        print("\nWorker Addresses:")
        print(get_worker_addresses(workers_df, addresses_df).head())
        
        # Get worker assignments
        print("\nWorker Assignments:")
        print(get_worker_assignments(workers_df, assignments_df).head())
        
        # Get complete profiles
        print("\nComplete Profiles:")
        print(get_worker_complete_profile(workers_df, addresses_df, communications_df, assignments_df).head())
        
    except Exception as e:
        print(f"Error executing queries: {e}") 