#%% [markdown]
# Polars Connection Utility
# This script provides connection functionality to PostgreSQL using Polars

#%% [markdown]
## Import required libraries

#%%
import polars as pl
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd

#%% [markdown]
## Define connection parameters and functions

#%%
def get_connection():
    """
    Create a connection to the PostgreSQL database
    Returns: database connection
    """
    return psycopg2.connect(
        host="localhost",
        database="postgres",
        user="postgres",
        password="mysecretpassword",
        port="5432"
    )

#%%
def query_to_polars(query):
    """
    Execute a query and return results as a Polars DataFrame
    Args:
        query: SQL query string
    Returns: Polars DataFrame with query results
    """
    with get_connection() as conn:
        # First get as pandas then convert to polars for better performance
        pandas_df = pd.read_sql_query(query, conn)
        return pl.from_pandas(pandas_df)

#%%
def read_table(table_name):
    """
    Read a table from PostgreSQL into a Polars DataFrame
    Args:
        table_name: Name of the table to read
    Returns: Polars DataFrame
    """
    query = f"SELECT * FROM {table_name}"
    return query_to_polars(query)

#%% [markdown]
## Test connection (run this cell to verify connection works)

#%%
if __name__ == "__main__":
    try:
        df = read_table("workers")
        print("Successfully connected to the database!")
        print(f"Worker count: {df.height}")
    except Exception as e:
        print(f"Error connecting to the database: {e}") 