#%% [markdown]
# PySpark Connection Utility
# This script provides connection functionality to PostgreSQL using PySpark

#%% [markdown]
## Import required libraries

#%%
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import os

#%% [markdown]
## Create Spark Session with PostgreSQL JDBC driver

#%%
def get_spark_session():
    """
    Create a Spark session with PostgreSQL JDBC driver
    Returns: SparkSession
    """
    return (SparkSession.builder
            .appName("WorkerAnalytics")
            .config("spark.jars", "postgresql-42.7.2.jar")  # Make sure to download this JAR
            .config("spark.driver.extraClassPath", "postgresql-42.7.2.jar")
            .getOrCreate())

#%%
def read_table(spark, table_name):
    """
    Read a table from PostgreSQL into a Spark DataFrame
    Args:
        spark: SparkSession
        table_name: Name of the table to read
    Returns: Spark DataFrame
    """
    return (spark.read
            .format("jdbc")
            .option("url", "jdbc:postgresql://localhost:5432/postgres")
            .option("driver", "org.postgresql.Driver")
            .option("dbtable", table_name)
            .option("user", "postgres")
            .option("password", "mysecretpassword")
            .load())

#%%
def write_table(df, table_name, mode="overwrite"):
    """
    Write a Spark DataFrame to PostgreSQL
    Args:
        df: Spark DataFrame
        table_name: Target table name
        mode: Write mode (overwrite/append)
    """
    (df.write
     .format("jdbc")
     .option("url", "jdbc:postgresql://localhost:5432/postgres")
     .option("driver", "org.postgresql.Driver")
     .option("dbtable", table_name)
     .option("user", "postgres")
     .option("password", "mysecretpassword")
     .mode(mode)
     .save())

#%% [markdown]
## Test connection (run this cell to verify connection works)

#%%
if __name__ == "__main__":
    try:
        spark = get_spark_session()
        df = read_table(spark, "workers")
        print("Successfully connected to the database!")
        print(f"Worker count: {df.count()}")
        spark.stop()
    except Exception as e:
        print(f"Error connecting to the database: {e}") 