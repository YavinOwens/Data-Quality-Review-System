#%% [markdown]
# Data Validation using PySpark
# This script contains various data validation checks using PySpark

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
## Check for Missing Required Fields

#%%
def check_missing_fields(workers_df, addresses_df):
    """Check for missing required fields in main tables"""
    
    # Check workers table
    workers_missing = (workers_df
                      .select(
                          lit("workers").alias("table_name"),
                          count("*").alias("total_records"),
                          sum(when(col("first_name").isNull(), 1).otherwise(0)).alias("missing_first_name"),
                          sum(when(col("last_name").isNull(), 1).otherwise(0)).alias("missing_last_name"),
                          sum(when(col("employee_number").isNull(), 1).otherwise(0)).alias("missing_employee_number")
                      ))
    
    # Check addresses table
    addresses_missing = (addresses_df
                        .select(
                            lit("addresses").alias("table_name"),
                            count("*").alias("total_records"),
                            sum(when(col("address_line1").isNull(), 1).otherwise(0)).alias("missing_address_line1"),
                            sum(when(col("city").isNull(), 1).otherwise(0)).alias("missing_city"),
                            sum(when(col("country").isNull(), 1).otherwise(0)).alias("missing_country")
                        ))
    
    return workers_missing.union(addresses_missing)

#%% [markdown]
## Check for Date Range Validity

#%%
def check_date_ranges(workers_df, addresses_df, assignments_df, communications_df):
    """Check for invalid date ranges in all tables"""
    
    def check_dates(df, table_name):
        return (df
                .select(
                    lit(table_name).alias("table_name"),
                    count("*").alias("total_records"),
                    sum(when(col("effective_from") > col("effective_to"), 1).otherwise(0)).alias("invalid_date_range"),
                    sum(when(col("effective_from") > current_date(), 1).otherwise(0)).alias("future_start_date"),
                    sum(when(col("effective_to") < current_date(), 1).otherwise(0)).alias("expired_records")
                ))
    
    workers_dates = check_dates(workers_df, "workers")
    addresses_dates = check_dates(addresses_df, "addresses")
    assignments_dates = check_dates(assignments_df, "assignments")
    communications_dates = check_dates(communications_df, "communications")
    
    return workers_dates.union(addresses_dates).union(assignments_dates).union(communications_dates)

#%% [markdown]
## Check for Duplicate Records

#%%
def check_duplicates(workers_df, assignments_df):
    """Check for duplicate records based on business keys"""
    
    # Check duplicate workers
    duplicate_workers = (workers_df
                        .groupBy("employee_number")
                        .agg(count("*").alias("record_count"))
                        .where(col("record_count") > 1)
                        .select(
                            lit("duplicate_workers").alias("check_type"),
                            col("employee_number"),
                            col("record_count")
                        ))
    
    # Check duplicate assignments
    duplicate_assignments = (assignments_df
                           .groupBy("assignment_number")
                           .agg(count("*").alias("record_count"))
                           .where(col("record_count") > 1)
                           .select(
                               lit("duplicate_assignments").alias("check_type"),
                               col("assignment_number"),
                               col("record_count")
                           ))
    
    return duplicate_workers.union(duplicate_assignments)

#%% [markdown]
## Check for Orphaned Records

#%%
def check_orphaned_records(workers_df, addresses_df, assignments_df, communications_df):
    """Check for records without corresponding worker records"""
    
    def check_orphans(df, table_name):
        return (df
                .join(workers_df, df.worker_unique_id == workers_df.unique_id, "left")
                .where(workers_df.unique_id.isNull())
                .select(
                    lit(f"orphaned_{table_name}").alias("check_type"),
                    count("*").alias("orphaned_count")
                ))
    
    addresses_orphans = check_orphans(addresses_df, "addresses")
    assignments_orphans = check_orphans(assignments_df, "assignments")
    communications_orphans = check_orphans(communications_df, "communications")
    
    return addresses_orphans.union(assignments_orphans).union(communications_orphans)

#%% [markdown]
## Check for Data Consistency

#%%
def check_data_consistency(workers_df, addresses_df, communications_df, assignments_df):
    """Check for workers missing required related records"""
    return (workers_df
            .join(addresses_df, workers_df.unique_id == addresses_df.worker_unique_id, "left")
            .join(communications_df, workers_df.unique_id == communications_df.worker_unique_id, "left")
            .join(assignments_df, workers_df.unique_id == assignments_df.worker_unique_id, "left")
            .groupBy("workers.unique_id", "first_name", "last_name")
            .agg(
                countDistinct("addresses.address_type").alias("address_count"),
                countDistinct("communications.contact_type").alias("contact_count"),
                countDistinct("assignments.assignment_number").alias("assignment_count")
            )
            .where(
                (col("address_count") == 0) |
                (col("contact_count") == 0) |
                (col("assignment_count") == 0)
            ))

#%% [markdown]
## Check Primary Contact Flags

#%%
def check_primary_contacts(communications_df):
    """Check for multiple primary contacts of the same type per worker"""
    return (communications_df
            .groupBy("worker_unique_id", "contact_type")
            .agg(
                count("*").alias("total_contacts"),
                sum(when(col("primary_flag") == "Y", 1).otherwise(0)).alias("primary_contacts")
            )
            .where(col("primary_contacts") > 1))

#%% [markdown]
## Example Usage

#%%
if __name__ == "__main__":
    try:
        # Load DataFrames
        workers_df, addresses_df, communications_df, assignments_df = load_dataframes()
        
        # Check missing fields
        print("\nMissing Required Fields:")
        missing_fields_df = check_missing_fields(workers_df, addresses_df)
        missing_fields_df.show()
        
        # Check date ranges
        print("\nDate Range Validation:")
        date_ranges_df = check_date_ranges(workers_df, addresses_df, assignments_df, communications_df)
        date_ranges_df.show()
        
        # Check duplicates
        print("\nDuplicate Records:")
        duplicates_df = check_duplicates(workers_df, assignments_df)
        duplicates_df.show()
        
        # Check orphaned records
        print("\nOrphaned Records:")
        orphaned_df = check_orphaned_records(workers_df, addresses_df, assignments_df, communications_df)
        orphaned_df.show()
        
        # Check data consistency
        print("\nData Consistency Issues:")
        consistency_df = check_data_consistency(workers_df, addresses_df, communications_df, assignments_df)
        consistency_df.show()
        
        # Check primary contacts
        print("\nMultiple Primary Contacts:")
        primary_contacts_df = check_primary_contacts(communications_df)
        primary_contacts_df.show()
        
        # Stop Spark session
        spark.stop()
        
    except Exception as e:
        print(f"Error executing validation checks: {e}")
        spark.stop() 