#%% [markdown]
# Analytics Queries
# This script contains various analytical queries for workforce data

#%% [markdown]
## Import required modules
#%%
from db_connection import query_to_dataframe
import pandas as pd
import matplotlib.pyplot as plt

#%% [markdown]
## Worker Demographics

#%%
def get_worker_demographics():
    """Get demographic statistics about workers"""
    query = """
    SELECT 
        nationality,
        COUNT(*) as worker_count,
        AVG(EXTRACT(YEAR FROM AGE(CURRENT_DATE, birth_date))) as avg_age,
        sex,
        marital_status
    FROM workers
    GROUP BY nationality, sex, marital_status
    ORDER BY worker_count DESC;
    """
    return query_to_dataframe(query)

#%% [markdown]
## Assignment Analysis

#%%
def get_assignment_statistics():
    """Get statistics about worker assignments"""
    query = """
    SELECT 
        department_id,
        COUNT(DISTINCT worker_unique_id) as worker_count,
        COUNT(*) as total_assignments,
        AVG(EXTRACT(DAYS FROM (effective_to - effective_from))) as avg_assignment_duration_days
    FROM assignments
    GROUP BY department_id
    ORDER BY worker_count DESC;
    """
    return query_to_dataframe(query)

#%% [markdown]
## Contact Method Analysis

#%%
def get_contact_method_stats():
    """Analyze preferred contact methods"""
    query = """
    SELECT 
        contact_type,
        COUNT(*) as total_contacts,
        SUM(CASE WHEN primary_flag = 'Y' THEN 1 ELSE 0 END) as primary_contacts,
        ROUND(AVG(CASE WHEN primary_flag = 'Y' THEN 100.0 ELSE 0 END), 2) as primary_percentage
    FROM communications
    GROUP BY contact_type
    ORDER BY total_contacts DESC;
    """
    return query_to_dataframe(query)

#%% [markdown]
## Geographic Distribution

#%%
def get_geographic_distribution():
    """Analyze worker distribution by location"""
    query = """
    SELECT 
        a.country,
        a.state,
        COUNT(DISTINCT a.worker_unique_id) as worker_count,
        COUNT(*) as address_count
    FROM addresses a
    GROUP BY a.country, a.state
    ORDER BY worker_count DESC;
    """
    return query_to_dataframe(query)

#%% [markdown]
## Visualization Examples

#%%
def plot_worker_demographics(df):
    """Create visualizations for worker demographics"""
    plt.figure(figsize=(12, 6))
    
    # Plot nationality distribution
    plt.subplot(1, 2, 1)
    nationality_counts = df.groupby('nationality')['worker_count'].sum()
    nationality_counts.plot(kind='bar')
    plt.title('Workers by Nationality')
    plt.xlabel('Nationality')
    plt.ylabel('Count')
    
    # Plot age distribution
    plt.subplot(1, 2, 2)
    df.boxplot(column='avg_age')
    plt.title('Age Distribution')
    
    plt.tight_layout()
    plt.show()

#%% [markdown]
## Example Usage

#%%
if __name__ == "__main__":
    # Get demographic data
    demographics_df = get_worker_demographics()
    print("\nWorker Demographics:")
    print(demographics_df.head())
    
    # Get assignment statistics
    assignments_df = get_assignment_statistics()
    print("\nAssignment Statistics:")
    print(assignments_df.head())
    
    # Get contact method statistics
    contacts_df = get_contact_method_stats()
    print("\nContact Method Statistics:")
    print(contacts_df)
    
    # Get geographic distribution
    geo_df = get_geographic_distribution()
    print("\nGeographic Distribution:")
    print(geo_df.head())
    
    # Create visualizations
    plot_worker_demographics(demographics_df) 