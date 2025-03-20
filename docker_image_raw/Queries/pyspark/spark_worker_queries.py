#%% [markdown]
# Worker Queries using PySpark
# This script contains various queries related to worker information using PySpark

#%% [markdown]
## Import required modules
#%%
from spark_connection import get_spark_session, read_table
from pyspark.sql.functions import *
from pyspark.sql.types import *

#%% [markdown]
## Initialize Spark Session
#%%
spark = get_spark_session()

#%% [markdown]
## Load DataFrames
#%%
def load_dataframes():
    """Load all required DataFrames"""
    workers_df = read_table(spark, "workers")
    addresses_df = read_table(spark, "addresses")
    communications_df = read_table(spark, "communications")
    assignments_df = read_table(spark, "assignments")
    
    # Cache the DataFrames for better performance
    workers_df.cache()
    addresses_df.cache()
    communications_df.cache()
    assignments_df.cache()
    
    return workers_df, addresses_df, communications_df, assignments_df

#%% [markdown]
## Basic Worker Information

#%%
def get_all_workers(workers_df):
    """Get basic information for all workers"""
    return (workers_df
            .select("first_name", "last_name", "employee_number", 
                   "birth_date", "nationality")
            .orderBy("last_name", "first_name"))

#%% [markdown]
## Worker Contact Information

#%%
def get_worker_contacts(workers_df, communications_df):
    """Get contact information for all workers"""
    return (workers_df
            .join(communications_df, workers_df.unique_id == communications_df.worker_unique_id)
            .select("first_name", "last_name", "contact_type", 
                   "contact_value", "primary_flag")
            .orderBy("last_name", "first_name", "contact_type"))

#%% [markdown]
## Worker Addresses

#%%
def get_worker_addresses(workers_df, addresses_df):
    """Get address information for all workers"""
    return (workers_df
            .join(addresses_df, workers_df.unique_id == addresses_df.worker_unique_id)
            .select("first_name", "last_name", "address_type", "address_line1",
                   "city", "state", "country")
            .orderBy("last_name", "first_name", "address_type"))

#%% [markdown]
## Worker Assignments

#%%
def get_worker_assignments(workers_df, assignments_df):
    """Get assignment information for all workers"""
    return (workers_df
            .join(assignments_df, workers_df.unique_id == assignments_df.worker_unique_id)
            .select("first_name", "last_name", "assignment_number", 
                   "position_id", "department_id", "location_id",
                   "effective_from", "effective_to")
            .orderBy("last_name", "first_name", "effective_from"))

#%% [markdown]
## Worker Complete Profile

#%%
def get_worker_complete_profile(workers_df, addresses_df, communications_df, assignments_df):
    """Get complete profile for all workers"""
    current_date = current_date()
    
    return (workers_df
            .join(addresses_df, workers_df.unique_id == addresses_df.worker_unique_id, "left")
            .join(communications_df, workers_df.unique_id == communications_df.worker_unique_id, "left")
            .join(assignments_df, workers_df.unique_id == assignments_df.worker_unique_id, "left")
            .where((col("addresses.effective_to") > current_date) &
                  (col("communications.effective_to") > current_date) &
                  (col("assignments.effective_to") > current_date))
            .select(
                "first_name", "last_name", "employee_number",
                "birth_date", "nationality", "sex", "marital_status",
                "address_line1", "city", "state", "country",
                "contact_type", "contact_value",
                "assignment_number", "position_id", "department_id"
            )
            .orderBy("last_name", "first_name"))

#%% [markdown]
## Example Usage

#%%
if __name__ == "__main__":
    try:
        # Load DataFrames
        workers_df, addresses_df, communications_df, assignments_df = load_dataframes()
        
        # Get all workers
        print("\nWorker Information:")
        get_all_workers(workers_df).show(5)
        
        # Get worker contacts
        print("\nWorker Contacts:")
        get_worker_contacts(workers_df, communications_df).show(5)
        
        # Get worker addresses
        print("\nWorker Addresses:")
        get_worker_addresses(workers_df, addresses_df).show(5)
        
        # Get worker assignments
        print("\nWorker Assignments:")
        get_worker_assignments(workers_df, assignments_df).show(5)
        
        # Get complete profiles
        print("\nComplete Profiles:")
        get_worker_complete_profile(workers_df, addresses_df, communications_df, assignments_df).show(5)
        
        # Stop Spark session
        spark.stop()
        
    except Exception as e:
        print(f"Error executing queries: {e}")
        spark.stop() 