# %% [markdown]
# # Data Validation Basics with Pandera
# 
# This notebook demonstrates data validation techniques using Pandera with different DataFrame libraries
# (Pandas, Polars, and PySpark). We'll also compare Pandera with Great Expectations.
# 
# ## What is Pandera?
# 
# Pandera is a statistical data validation library for pandas DataFrames. It provides a flexible and
# declarative way to validate data at runtime, ensuring data quality and consistency.
# 
# ### Key Benefits of Pandera:
# 1. **Lightweight and Fast**: Pandera is designed to be lightweight and performant, making it suitable
#    for both development and production environments.
# 2. **Type Hints and Schema Validation**: Provides Python type hints and runtime schema validation.
# 3. **Integration with Multiple Libraries**: Works with Pandas, Polars, and PySpark.
# 4. **Declarative API**: Easy to read and maintain validation rules.
# 5. **Custom Validation Rules**: Supports custom validation functions and complex validation logic.
# 
# ### Pandera vs Great Expectations
# 
# While Pandera is excellent for many use cases, Great Expectations is often preferred in production
# data pipelines for several reasons:
# 
# **Great Expectations Advantages:**
# 1. **Production-Ready**: Built specifically for data pipeline validation in production environments.
# 2. **Data Documentation**: Generates comprehensive data documentation and profiling.
# 3. **Data Quality Reports**: Creates detailed data quality reports and dashboards.
# 4. **Integration with Data Platforms**: Better integration with data platforms and ETL tools.
# 5. **Data Profiling**: Built-in data profiling capabilities.
# 6. **Community and Support**: Larger community and more enterprise support.
# 
# **When to Use Each:**
# - **Pandera**: Use for lightweight validation in development, testing, or smaller projects.
# - **Great Expectations**: Use for production data pipelines, large-scale projects, or when you need
#   comprehensive data quality reporting.
# 


# %% [markdown]
# ## 1. Setup and Imports
# First, let's import the necessary libraries and set up our environment.

# %%
import pandas as pd
import polars as pl
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import pandera as pa
from pandera.typing import Series, DataFrame
from typing import Optional
import numpy as np
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns
from helpers.db_connection import DatabaseConnection, query_to_df

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
# ## 4. Define Data Schema Based on Available Tables
# Let's create a schema that matches our actual database structure.

# %%
# Example query to get data from the first table
sample_query = f"""
SELECT *
FROM {tables_df['table_name'].iloc[0]}
LIMIT 5;
"""

# Load sample data to understand the structure
sample_df = query_to_df(sample_query)
print("Sample data structure:")
print(sample_df.info())

# Define the schema based on actual columns
class DataSchema(pa.SchemaModel):
    """Schema for data validation."""
    
    # We'll dynamically add fields based on the actual columns
    class Config:
        """Schema configuration."""
        strict = True
        coerce = True

# %% [markdown]
# ## 5. Load and Validate Data with Pandas

# %%
# Load data from the first available table
query = f"""
SELECT *
FROM {tables_df['table_name'].iloc[0]}
LIMIT 1000;
"""

# Load into Pandas DataFrame
pandas_df = query_to_df(query)
print("\nLoaded data info:")
print(pandas_df.info())

# %% [markdown]
# ## 6. Load and Validate Data with Polars

# %%
# Convert to Polars DataFrame
polars_df = pl.DataFrame(pandas_df)
print("\nPolars DataFrame info:")
print(polars_df.describe())

# %% [markdown]
# ## 7. Load and Validate Data with PySpark

# %%
# Create Spark session
spark = SparkSession.builder \
    .appName("Data Validation") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

# Convert to Spark DataFrame
spark_df = spark.createDataFrame(pandas_df)
print("\nSpark DataFrame schema:")
spark_df.printSchema()

# %% [markdown]
# ## 8. Data Quality Metrics

# %%
def calculate_data_quality(df):
    """Calculate data quality metrics."""
    metrics = {
        "total_rows": len(df),
        "missing_values": df.isnull().sum().to_dict(),
        "unique_values": df.nunique().to_dict(),
        "data_types": df.dtypes.to_dict()
    }
    return metrics

# Calculate metrics for Pandas DataFrame
print("Data Quality Metrics:")
print(calculate_data_quality(pandas_df))

# %% [markdown]
# ## 9. Cleanup
# Clean up resources and stop the Spark session.

# %%
# Stop Spark session
spark.stop() 