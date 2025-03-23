# %% [markdown]
# # Pandas Basics with PostgreSQL Database
# 
# This notebook demonstrates fundamental pandas operations using data from our PostgreSQL database.
# We'll cover:
# 1. Database connection and data loading
# 2. Basic DataFrame operations
# 3. Data manipulation and cleaning
# 4. Data sampling and analysis
# 5. Data modeling basics

# %% [markdown]
# ## 1. Setup and Database Connection
# First, let's import the necessary libraries and connect to our database.

# %%
import pandas as pd
import numpy as np
from helpers.db_connection import DatabaseConnection, query_to_df
import matplotlib.pyplot as plt
import seaborn as sns

# Set display options for better readability
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_rows', 100)

# %% [markdown]
# ## 2. Exploring Database Tables
# Let's first see what tables are available in our database.

# %%
# Query to list all tables in the database
tables_query = """
SELECT table_name 
FROM information_schema.tables 
WHERE table_schema = 'public'
ORDER BY table_name;
"""

# Get list of tables
tables_df = query_to_df(tables_query)
print("Available tables in the database:")
print(tables_df)

# %% [markdown]
# ## 3. Exploring Table Structures
# Let's examine the structure of each table.

# %%
# Function to get column information for a table
def get_table_structure(table_name):
    query = f"""
    SELECT column_name, data_type, character_maximum_length
    FROM information_schema.columns
    WHERE table_name = '{table_name}'
    ORDER BY ordinal_position;
    """
    return query_to_df(query)

# Get structure for each table
for table in tables_df['table_name']:
    print(f"\nStructure of table '{table}':")
    print(get_table_structure(table))

# %% [markdown]
# ## 4. Loading Sample Data
# Now that we know the table structure, let's load some sample data.

# %%
# Example query to get data from the first table
sample_query = f"""
SELECT *
FROM {tables_df['table_name'].iloc[0]}
LIMIT 5;
"""

# Load data into a pandas DataFrame
df = query_to_df(sample_query)

# Display basic information about the DataFrame
print("DataFrame Info:")
print(df.info())

print("\nDataFrame Shape:")
print(df.shape)

print("\nDataFrame Columns:")
print(df.columns)

print("\nFirst few rows of data:")
print(df.head())

# %% [markdown]
# ## 3. Basic DataFrame Operations
# Let's explore the basic operations we can perform on our DataFrame.

# %%
if 'df' in locals():
    # Display basic information about the DataFrame
    print("DataFrame Info:")
    print(df.info())

    print("\nDataFrame Shape:")
    print(df.shape)

    print("\nDataFrame Columns:")
    print(df.columns)

    # %% [markdown]
    # ### Viewing Data
    # Different ways to view the data in our DataFrame.

    # %%
    # Display first 5 rows
    print("First 5 rows:")
    print(df.head())

    # Display last 5 rows
    print("\nLast 5 rows:")
    print(df.tail())

    # Display middle 5 rows
    print("\nMiddle 5 rows:")
    middle_index = len(df) // 2
    print(df.iloc[middle_index-2:middle_index+3])

    # %% [markdown]
    # ### Indexing and Selection
    # Different ways to select and filter data.

    # %%
    # Select specific columns
    print("Selected columns:")
    print(df[['first_name', 'last_name', 'salary']].head())

    # Filter rows based on condition
    print("\nHigh salary employees:")
    high_salary = df[df['salary'] > 80000]
    print(high_salary.head())

    # Multiple conditions
    print("\nHigh salary employees in specific departments:")
    high_salary_dept = df[(df['salary'] > 80000) & (df['dept_name'].isin(['Sales', 'Marketing']))]
    print(high_salary_dept.head())
else:
    print("DataFrame not loaded. Please check the database connection and query above.")

# %% [markdown]
# ### Column Operations
# How to manipulate columns in the DataFrame.

# %%
# Rename columns
df_renamed = df.rename(columns={
    'emp_no': 'employee_id',
    'first_name': 'first',
    'last_name': 'last'
})
print("Renamed columns:")
print(df_renamed.columns)

# Drop columns
df_dropped = df.drop(columns=['emp_no'])
print("\nDropped columns:")
print(df_dropped.columns)

# %% [markdown]
# ### Row Operations
# How to manipulate rows in the DataFrame.

# %%
# Remove rows with missing values
df_clean = df.dropna()
print("Shape after removing missing values:", df_clean.shape)

# Remove duplicate rows
df_unique = df.drop_duplicates()
print("Shape after removing duplicates:", df_unique.shape)

# %% [markdown]
# ## 4. Data Sampling and Analysis
# Different ways to sample and analyze the data.

# %%
# Random sampling
print("Random sample of 5 rows:")
print(df.sample(n=5))

# Stratified sampling by department
print("\nStratified sample by department:")
stratified_sample = df.groupby('dept_name', group_keys=False).apply(
    lambda x: x.sample(n=min(3, len(x)))
)
print(stratified_sample)

# %% [markdown]
# ### Basic Statistics
# Calculate basic statistics on numerical columns.

# %%
# Numerical columns statistics
print("Numerical columns statistics:")
print(df.describe())

# Group by operations
print("\nAverage salary by department:")
print(df.groupby('dept_name')['salary'].mean().sort_values(ascending=False))

# %% [markdown]
# ## 5. Data Modeling Basics
# Simple data modeling examples.

# %%
# Create a simple feature
df['salary_category'] = pd.cut(
    df['salary'],
    bins=[0, 50000, 80000, 120000, float('inf')],
    labels=['Low', 'Medium', 'High', 'Very High']
)

# Count salary categories
print("Salary categories distribution:")
print(df['salary_category'].value_counts())

# %% [markdown]
# ### Visualization
# Basic visualizations of our data.

# %%
# Set the style for better visualizations
plt.style.use('seaborn')

# Create a figure with multiple subplots
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(15, 5))

# Salary distribution by department
sns.boxplot(data=df, x='dept_name', y='salary', ax=ax1)
ax1.set_title('Salary Distribution by Department')
ax1.set_xticklabels(ax1.get_xticklabels(), rotation=45)

# Gender distribution
df['gender'].value_counts().plot(kind='pie', autopct='%1.1f%%', ax=ax2)
ax2.set_title('Gender Distribution')

plt.tight_layout()
plt.show()

# %% [markdown]
# ## 6. Advanced Operations
# Some more advanced pandas operations.

# %%
# Pivot table
print("Average salary by department and gender:")
pivot_table = pd.pivot_table(
    df,
    values='salary',
    index='dept_name',
    columns='gender',
    aggfunc='mean'
)
print(pivot_table)

# %% [markdown]
# ### Time Series Operations
# Working with date columns.

# %%
# Convert hire_date to datetime
df['hire_date'] = pd.to_datetime(df['hire_date'])

# Calculate years of service
df['years_of_service'] = (pd.Timestamp.now() - df['hire_date']).dt.years

print("Years of service statistics:")
print(df['years_of_service'].describe())

# %% [markdown]
# ## 7. Data Export
# How to export our processed data.

# %%
# Export to CSV
df.to_csv('processed_employee_data.csv', index=False)

# Export to Excel
df.to_excel('processed_employee_data.xlsx', index=False) 