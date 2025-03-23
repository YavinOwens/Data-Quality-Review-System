# %% [markdown]
# # PySpark Basics with PostgreSQL Database
# 
# This notebook demonstrates fundamental PySpark operations using data from our PostgreSQL database.
# PySpark is Apache Spark's Python API, offering distributed computing capabilities for big data processing.
# We'll cover:
# 1. Spark session setup and database connection
# 2. Basic DataFrame operations
# 3. Data manipulation and cleaning
# 4. Data sampling and analysis
# 5. Data modeling basics
# 6. Performance optimization techniques


# %% [markdown]
# ## 1. Setup and Spark Session Configuration
# First, let's set up our Spark session and configure it for optimal performance.
#


# %%
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, count, when, lit, round, desc, asc
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, DoubleType
from pyspark.sql.window import Window
import time
from helpers.db_connection import DatabaseConnection, query_to_df
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

# Create Spark session with optimized configurations
spark = SparkSession.builder \
    .appName("PostgreSQL Analysis") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "4g") \
    .config("spark.sql.shuffle.partitions", "2") \
    .config("spark.default.parallelism", "2") \
    .getOrCreate()

# Set display options
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
spark.conf.set("spark.sql.repl.eagerEval.maxNumRows", 10)
spark.conf.set("spark.sql.repl.eagerEval.truncate", False)


# %% [markdown]
# ## 2. Exploring Database Tables
# Let's first see what tables are available in our database.
#


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
#


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
#


# %%
# Example query to get data from the first table
sample_query = f"""
SELECT *
FROM {tables_df['table_name'].iloc[0]}
LIMIT 1000;
"""

# Load data into a Spark DataFrame
df = spark.createDataFrame(query_to_df(sample_query))

# Display basic information about the DataFrame
print("DataFrame Schema:")
df.printSchema()

print("\nDataFrame Summary Statistics:")
df.describe().show()

print("\nDataFrame Columns:")
print(df.columns)


# %% [markdown]
# ### Viewing Data
# Different ways to view the data in our Spark DataFrame.
#


# %%
# Display first 5 rows
print("First 5 rows:")
df.show(5)

# Display last 5 rows (using orderBy and limit)
print("\nLast 5 rows:")
df.orderBy(df.columns[0].desc()).limit(5).show()


# %% [markdown]
# ### Basic Operations
# Let's explore some basic Spark DataFrame operations.
#


# %%
# Select all columns
print("All columns:")
df.select("*").show(5)

# Select specific columns
numeric_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, (IntegerType, DoubleType))]
if numeric_columns:
    print("\nNumeric columns:")
    df.select(numeric_columns).show(5)

# Basic filtering
if numeric_columns:
    print(f"\nFiltered data (where {numeric_columns[0]} > 0):")
    df.filter(col(numeric_columns[0]) > 0).show(5)


# %% [markdown]
# ### Aggregations and Grouping
# Demonstrate aggregation operations if we have appropriate columns.
#


# %%
# Get categorical columns (string type)
categorical_columns = [field.name for field in df.schema.fields if isinstance(field.dataType, StringType)]

# If we have both numeric and categorical columns, show some aggregations
if numeric_columns and categorical_columns:
    print(f"Aggregations grouped by {categorical_columns[0]}:")
    df.groupBy(categorical_columns[0]) \
      .agg(avg(numeric_columns[0]).alias("average")) \
      .orderBy("average", ascending=False) \
      .show()


# %% [markdown]
# ## 5. Data Export
# How to export our processed data.
#


# %%
# Export to CSV
df.write.mode("overwrite").csv("processed_data_spark", header=True)

# Export to Parquet (Spark's preferred format)
df.write.mode("overwrite").parquet("processed_data_spark")


# %% [markdown]
# ## 6. Cleanup
# Clean up resources and stop the Spark session.
#


# %%
# Stop Spark session
spark.stop() 