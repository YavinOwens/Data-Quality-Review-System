#%% [markdown]
# Database Connection Utility
# This script provides connection functionality to the PostgreSQL database

#%% [markdown]
## Import required libraries

#%%
import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd

#%% [markdown]
## Define connection parameters and function

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
def query_to_dataframe(query):
    """
    Execute a query and return results as a pandas DataFrame
    Args:
        query: SQL query string
    Returns: pandas DataFrame with query results
    """
    with get_connection() as conn:
        return pd.read_sql_query(query, conn)

#%%
def execute_query(query):
    """
    Execute a query and return results as a dictionary
    Args:
        query: SQL query string
    Returns: List of dictionary results
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(query)
            return cur.fetchall()

#%% [markdown]
## Test connection (run this cell to verify connection works)

#%%
if __name__ == "__main__":
    try:
        with get_connection() as conn:
            print("Successfully connected to the database!")
    except Exception as e:
        print(f"Error connecting to the database: {e}") 