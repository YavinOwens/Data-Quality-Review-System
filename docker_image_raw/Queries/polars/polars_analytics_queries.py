#%% [markdown]
# Analytics Queries using Polars
# This script contains various analytical queries for workforce data using Polars

#%% [markdown]
## Import required modules
#%%
from polars_connection import read_table
import polars as pl
import matplotlib.pyplot as plt

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
## Worker Demographics

#%%
def get_worker_demographics(workers_df):
    """Get demographic statistics about workers"""
    return (workers_df
            .groupby(["nationality", "sex", "marital_status"])
            .agg([
                pl.count("*").alias("worker_count"),
                pl.col("birth_date")
                  .map_elements(lambda x: (pl.datetime("now") - x).days / 365.25)
                  .mean()
                  .alias("avg_age")
            ])
            .sort("worker_count", descending=True))

#%% [markdown]
## Assignment Analysis

#%%
def get_assignment_statistics(assignments_df):
    """Get statistics about worker assignments"""
    return (assignments_df
            .groupby("department_id")
            .agg([
                pl.col("worker_unique_id").n_unique().alias("worker_count"),
                pl.count("*").alias("total_assignments"),
                pl.col("effective_to")
                  .sub(pl.col("effective_from"))
                  .map_elements(lambda x: x.days)
                  .mean()
                  .alias("avg_assignment_duration_days")
            ])
            .sort("worker_count", descending=True))

#%% [markdown]
## Contact Method Analysis

#%%
def get_contact_method_stats(communications_df):
    """Analyze preferred contact methods"""
    return (communications_df
            .groupby("contact_type")
            .agg([
                pl.count("*").alias("total_contacts"),
                pl.col("primary_flag")
                  .filter(pl.col("primary_flag") == "Y")
                  .count()
                  .alias("primary_contacts"),
                (pl.col("primary_flag")
                  .filter(pl.col("primary_flag") == "Y")
                  .count() * 100.0 / pl.count("*"))
                  .round(2)
                  .alias("primary_percentage")
            ])
            .sort("total_contacts", descending=True))

#%% [markdown]
## Geographic Distribution

#%%
def get_geographic_distribution(addresses_df):
    """Analyze worker distribution by location"""
    return (addresses_df
            .groupby(["country", "state"])
            .agg([
                pl.col("worker_unique_id").n_unique().alias("worker_count"),
                pl.count("*").alias("address_count")
            ])
            .sort("worker_count", descending=True))

#%% [markdown]
## Advanced Analytics

#%%
def get_department_age_distribution(workers_df, assignments_df):
    """Analyze age distribution by department"""
    return (workers_df
            .join(assignments_df,
                  left_on="unique_id",
                  right_on="worker_unique_id")
            .filter(pl.col("assignments.effective_to") > pl.datetime("now"))
            .groupby("department_id")
            .agg([
                pl.col("workers.unique_id").n_unique().alias("worker_count"),
                pl.col("birth_date")
                  .map_elements(lambda x: (pl.datetime("now") - x).days / 365.25)
                  .mean()
                  .alias("avg_age"),
                pl.col("birth_date")
                  .map_elements(lambda x: (pl.datetime("now") - x).days / 365.25)
                  .min()
                  .alias("min_age"),
                pl.col("birth_date")
                  .map_elements(lambda x: (pl.datetime("now") - x).days / 365.25)
                  .max()
                  .alias("max_age")
            ])
            .sort("worker_count", descending=True))

#%%
def get_nationality_distribution_by_department(workers_df, assignments_df):
    """Analyze nationality distribution by department"""
    # First get counts by department and nationality
    dept_nat_counts = (workers_df
                      .join(assignments_df,
                            left_on="unique_id",
                            right_on="worker_unique_id")
                      .filter(pl.col("assignments.effective_to") > pl.datetime("now"))
                      .groupby(["department_id", "nationality"])
                      .agg(pl.count("*").alias("worker_count")))
    
    # Calculate total workers per department for percentage
    dept_totals = (dept_nat_counts
                  .groupby("department_id")
                  .agg(pl.col("worker_count").sum().alias("total_workers")))
    
    # Join and calculate percentages
    return (dept_nat_counts
            .join(dept_totals, on="department_id")
            .with_columns([
                (pl.col("worker_count") * 100.0 / pl.col("total_workers"))
                .round(2)
                .alias("percentage")
            ])
            .sort(["department_id", "worker_count"], descending=[False, True]))

#%% [markdown]
## Visualization Functions

#%%
def plot_worker_demographics(demographics_df):
    """Create visualizations for worker demographics"""
    # Convert Polars DataFrame to Pandas for plotting
    pandas_df = demographics_df.to_pandas()
    
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
        print(demographics_df.head())
        
        # Get assignment statistics
        print("\nAssignment Statistics:")
        assignments_stats_df = get_assignment_statistics(assignments_df)
        print(assignments_stats_df.head())
        
        # Get contact method statistics
        print("\nContact Method Statistics:")
        contact_stats_df = get_contact_method_stats(communications_df)
        print(contact_stats_df)
        
        # Get geographic distribution
        print("\nGeographic Distribution:")
        geo_df = get_geographic_distribution(addresses_df)
        print(geo_df.head())
        
        # Get department age distribution
        print("\nDepartment Age Distribution:")
        age_dist_df = get_department_age_distribution(workers_df, assignments_df)
        print(age_dist_df.head())
        
        # Get nationality distribution by department
        print("\nNationality Distribution by Department:")
        nat_dist_df = get_nationality_distribution_by_department(workers_df, assignments_df)
        print(nat_dist_df.head())
        
        # Create visualizations
        plot_worker_demographics(demographics_df)
        
    except Exception as e:
        print(f"Error executing queries: {e}") 