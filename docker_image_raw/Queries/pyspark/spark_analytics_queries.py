#%% [markdown]
# Analytics Queries using PySpark
# This script contains various analytical queries for workforce data using PySpark

#%% [markdown]
## Import required modules
#%%
from spark_connection import get_spark_session, read_table
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import matplotlib.pyplot as plt
import pandas as pd

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
## Worker Demographics

#%%
def get_worker_demographics(workers_df):
    """Get demographic statistics about workers"""
    return (workers_df
            .groupBy("nationality", "sex", "marital_status")
            .agg(
                count("*").alias("worker_count"),
                avg(months_between(current_date(), col("birth_date"))/12).alias("avg_age")
            )
            .orderBy(desc("worker_count")))

#%% [markdown]
## Assignment Analysis

#%%
def get_assignment_statistics(assignments_df):
    """Get statistics about worker assignments"""
    return (assignments_df
            .groupBy("department_id")
            .agg(
                countDistinct("worker_unique_id").alias("worker_count"),
                count("*").alias("total_assignments"),
                avg(datediff(col("effective_to"), col("effective_from"))).alias("avg_assignment_duration_days")
            )
            .orderBy(desc("worker_count")))

#%% [markdown]
## Contact Method Analysis

#%%
def get_contact_method_stats(communications_df):
    """Analyze preferred contact methods"""
    return (communications_df
            .groupBy("contact_type")
            .agg(
                count("*").alias("total_contacts"),
                sum(when(col("primary_flag") == "Y", 1).otherwise(0)).alias("primary_contacts"),
                round(avg(when(col("primary_flag") == "Y", 100).otherwise(0)), 2).alias("primary_percentage")
            )
            .orderBy(desc("total_contacts")))

#%% [markdown]
## Geographic Distribution

#%%
def get_geographic_distribution(addresses_df):
    """Analyze worker distribution by location"""
    return (addresses_df
            .groupBy("country", "state")
            .agg(
                countDistinct("worker_unique_id").alias("worker_count"),
                count("*").alias("address_count")
            )
            .orderBy(desc("worker_count")))

#%% [markdown]
## Advanced Analytics

#%%
def get_department_age_distribution(workers_df, assignments_df):
    """Analyze age distribution by department"""
    return (workers_df
            .join(assignments_df, workers_df.unique_id == assignments_df.worker_unique_id)
            .where(col("assignments.effective_to") > current_date())
            .groupBy("department_id")
            .agg(
                countDistinct("workers.unique_id").alias("worker_count"),
                avg(months_between(current_date(), col("birth_date"))/12).alias("avg_age"),
                min(months_between(current_date(), col("birth_date"))/12).alias("min_age"),
                max(months_between(current_date(), col("birth_date"))/12).alias("max_age")
            )
            .orderBy(desc("worker_count")))

#%%
def get_nationality_distribution_by_department(workers_df, assignments_df):
    """Analyze nationality distribution by department"""
    window_spec = Window.partitionBy("department_id")
    
    return (workers_df
            .join(assignments_df, workers_df.unique_id == assignments_df.worker_unique_id)
            .where(col("assignments.effective_to") > current_date())
            .groupBy("department_id", "nationality")
            .agg(count("*").alias("worker_count"))
            .withColumn("percentage", 
                       round(col("worker_count") * 100 / sum("worker_count").over(window_spec), 2))
            .orderBy("department_id", desc("worker_count")))

#%% [markdown]
## Visualization Functions

#%%
def plot_worker_demographics(demographics_df):
    """Create visualizations for worker demographics"""
    # Convert Spark DataFrame to Pandas for plotting
    pandas_df = demographics_df.toPandas()
    
    plt.figure(figsize=(12, 6))
    
    # Plot nationality distribution
    plt.subplot(1, 2, 1)
    nationality_counts = pandas_df.groupby('nationality')['worker_count'].sum()
    nationality_counts.plot(kind='bar')
    plt.title('Workers by Nationality')
    plt.xlabel('Nationality')
    plt.ylabel('Count')
    
    # Plot age distribution
    plt.subplot(1, 2, 2)
    pandas_df.boxplot(column='avg_age')
    plt.title('Age Distribution')
    
    plt.tight_layout()
    plt.show()

#%% [markdown]
## Example Usage

#%%
if __name__ == "__main__":
    try:
        # Load DataFrames
        workers_df, addresses_df, communications_df, assignments_df = load_dataframes()
        
        # Get demographic data
        print("\nWorker Demographics:")
        demographics_df = get_worker_demographics(workers_df)
        demographics_df.show(5)
        
        # Get assignment statistics
        print("\nAssignment Statistics:")
        assignments_stats_df = get_assignment_statistics(assignments_df)
        assignments_stats_df.show(5)
        
        # Get contact method statistics
        print("\nContact Method Statistics:")
        contact_stats_df = get_contact_method_stats(communications_df)
        contact_stats_df.show()
        
        # Get geographic distribution
        print("\nGeographic Distribution:")
        geo_df = get_geographic_distribution(addresses_df)
        geo_df.show(5)
        
        # Get department age distribution
        print("\nDepartment Age Distribution:")
        age_dist_df = get_department_age_distribution(workers_df, assignments_df)
        age_dist_df.show(5)
        
        # Get nationality distribution by department
        print("\nNationality Distribution by Department:")
        nat_dist_df = get_nationality_distribution_by_department(workers_df, assignments_df)
        nat_dist_df.show(5)
        
        # Create visualizations
        plot_worker_demographics(demographics_df)
        
        # Stop Spark session
        spark.stop()
        
    except Exception as e:
        print(f"Error executing queries: {e}")
        spark.stop() 