# %% [markdown]
# # Polars Basics with PostgreSQL Database
# 
# This notebook demonstrates fundamental Polars operations using data from our PostgreSQL database.
# Polars is a fast DataFrame library written in Rust, offering better performance than Pandas.
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
import polars as pl
import numpy as np
from helpers.db_connection import DatabaseConnection, query_to_df
import matplotlib.pyplot as plt
import seaborn as sns

# Set display options for better readability
pl.Config.set_fmt_str_lengths(50)
pl.Config.set_tbl_rows(10)
pl.Config.set_tbl_cols(10)

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
LIMIT 1000;
"""

# Load data into a Polars DataFrame
df = pl.DataFrame(query_to_df(sample_query))

# Display basic information about the DataFrame
print("DataFrame Info:")
print(df.describe())

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
print(df.slice(middle_index-2, 5))

# %% [markdown]
# ### Indexing and Selection
# Different ways to select and filter data using Polars expressions.

# %%
# Select specific columns (if they exist)
available_columns = df.columns
print("Available columns for selection:")
print(available_columns)

# Example of column selection and filtering
numeric_cols = [col for col in available_columns if df[col].dtype in [pl.Float64, pl.Int64]]
if numeric_cols:
    print(f"\nNumeric columns statistics:")
    print(df.select(numeric_cols).describe())

# %% [markdown]
# ### Column Operations
# How to manipulate columns in the DataFrame using Polars expressions.

# %%
# Rename columns example
df_renamed = df.rename({col: f"col_{i}" for i, col in enumerate(df.columns)})
print("Renamed columns:")
print(df_renamed.columns)

# Drop first column example
df_dropped = df.drop(df.columns[0])
print("\nDropped first column:")
print(df_dropped.columns)

# %% [markdown]
# ### Row Operations
# How to manipulate rows in the DataFrame.

# %%
# Remove rows with missing values
df_clean = df.drop_nulls()
print("Shape after removing missing values:", df_clean.shape)

# Remove duplicate rows
df_unique = df.unique()
print("Shape after removing duplicates:", df_unique.shape)

# %% [markdown]
# ## 5. Data Analysis
# Different ways to analyze the data.

# %%
# Basic statistics for numeric columns
print("Basic statistics for numeric columns:")
print(df.describe())

# Group by example (if categorical columns exist)
categorical_cols = [col for col in available_columns if df[col].dtype == pl.Utf8]
if categorical_cols and numeric_cols:
    print(f"\nGroup by example using {categorical_cols[0]} and {numeric_cols[0]}:")
    print(
        df.groupby(categorical_cols[0])
        .agg(pl.col(numeric_cols[0]).mean().alias('mean_value'))
        .sort('mean_value', descending=True)
    )

# %% [markdown]
# ## 6. Data Export
# How to export our processed data.

# %%
# Export to CSV
df.write_csv('processed_data_polars.csv')

# Export to Parquet (Polars' preferred format)
df.write_parquet('processed_data_polars.parquet')

# %% [markdown]
# ## 7. Performance Comparison
# Let's compare the performance of some operations between Pandas and Polars.

# %%
import time
import pandas as pd

# Create a large dataset for comparison
large_df = pl.DataFrame({
    'A': np.random.randn(1000000),
    'B': np.random.randn(1000000),
    'C': np.random.randn(1000000)
})

# Convert to Pandas for comparison
pandas_df = large_df.to_pandas()

# Test groupby operation
start_time = time.time()
polars_result = large_df.groupby('A').agg(pl.col('B').mean())
polars_time = time.time() - start_time

start_time = time.time()
pandas_result = pandas_df.groupby('A')['B'].mean()
pandas_time = time.time() - start_time

print(f"Polars groupby time: {polars_time:.4f} seconds")
print(f"Pandas groupby time: {pandas_time:.4f} seconds")
print(f"Polars is {pandas_time/polars_time:.2f}x faster") 