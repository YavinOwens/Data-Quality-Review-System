# %% [markdown]
# # SQL Helper Functions Example Usage
# 
# This notebook demonstrates how to use the SQL helper functions to interact with your PostgreSQL database running in Docker.

# %% [markdown]
# ## Setup
# First, let's import the necessary helper functions

# %%
from db_connection import DatabaseConnection, query_to_df
from sql_queries import sql
import pandas as pd

# %% [markdown]
# ## 1. Connecting to the Database
# 
# First, let's establish a connection to the database. You can use the default connection or create a custom one.

# %%
# Using default connection
db = DatabaseConnection()

# Test the connection
if db.test_connection():
    print("Successfully connected to the database!")
else:
    print("Connection failed. Please check your database settings.")

# %% [markdown]
# ## 2. Basic Table Information
# 
# Let's explore the structure of our tables.

# %%
# Get information about the workers table
workers_info = sql.get_table_info('workers')
print("\nTable Structure:")
print(workers_info)

# %% [markdown]
# ## 3. Data Exploration
# 
# Let's look at some sample data and basic statistics.

# %%
# Get a sample of workers
sample_workers = sql.get_sample_data('workers', limit=5)
print("\nSample Workers Data:")
print(sample_workers)

# Get row count
total_workers = sql.get_row_count('workers')
print(f"\nTotal number of workers: {total_workers}")

# %% [markdown]
# ## 4. Column Analysis
# 
# Let's analyze specific columns in our tables.

# %%
# Get statistics for age column
age_stats = sql.get_column_stats('workers', 'age')
print("\nAge Statistics:")
print(age_stats)

# Get distinct values in department
departments = sql.get_distinct_values('workers', 'department')
print("\nDepartment Distribution:")
print(departments)

# %% [markdown]
# ## 5. Data Quality Check
# 
# Let's check for missing values in our data.

# %%
# Check for NULL values
null_counts = sql.get_null_counts('workers')
print("\nNull Value Analysis:")
print(null_counts)

# %% [markdown]
# ## 6. Custom Queries
# 
# You can also run custom SQL queries easily.

# %%
# Example of a custom query
custom_query = """
SELECT 
    department,
    COUNT(*) as worker_count,
    AVG(age) as avg_age
FROM workers
GROUP BY department
ORDER BY worker_count DESC
"""

result = query_to_df(custom_query)
print("\nDepartment Analysis:")
print(result)

# %% [markdown]
# ## 7. Data Visualization Example
# 
# Let's create a simple visualization of our data using pandas plotting capabilities.

# %%
import matplotlib.pyplot as plt

# Get age distribution by department
age_by_dept = query_to_df("""
    SELECT 
        department,
        AVG(age) as avg_age,
        COUNT(*) as count
    FROM workers
    GROUP BY department
    ORDER BY count DESC
    LIMIT 10
""")

# Create a bar plot
plt.figure(figsize=(12, 6))
plt.bar(age_by_dept['department'], age_by_dept['avg_age'])
plt.xticks(rotation=45)
plt.title('Average Age by Department')
plt.xlabel('Department')
plt.ylabel('Average Age')
plt.tight_layout()
plt.show()

# %% [markdown]
# ## 8. Error Handling Example
# 
# The helper functions include built-in error handling. Let's see how it works:

# %%
# Try to query a non-existent table
invalid_query = sql.get_sample_data('nonexistent_table')
print("\nTrying to query non-existent table:")
print(invalid_query)  # Will return empty DataFrame instead of raising error

# %% [markdown]
# ## 9. Using Parameters in Queries
# 
# Demonstrate how to use parameterized queries for safety:

# %%
# Example of a parameterized query
param_query = """
SELECT *
FROM workers
WHERE department = %(dept)s
AND age > %(min_age)s
LIMIT 5
"""

params = {
    'dept': 'IT',
    'min_age': 25
}

filtered_workers = query_to_df(param_query, params)
print("\nFiltered Workers:")
print(filtered_workers) 