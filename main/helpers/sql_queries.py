"""
SQL query helper functions for common database operations.
Provides pre-built queries and functions for common data analysis tasks.
"""

from typing import Optional, Dict, Any
import pandas as pd
from db_connection import query_to_df, DatabaseConnection

class SQLQueries:
    @staticmethod
    def get_table_info(table_name: str) -> pd.DataFrame:
        """
        Get information about a table's structure.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            pd.DataFrame: Table structure information
        """
        query = """
        SELECT 
            column_name,
            data_type,
            character_maximum_length,
            column_default,
            is_nullable
        FROM information_schema.columns
        WHERE table_name = %(table_name)s
        ORDER BY ordinal_position;
        """
        return query_to_df(query, {"table_name": table_name})

    @staticmethod
    def get_row_count(table_name: str) -> int:
        """
        Get the number of rows in a table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            int: Number of rows
        """
        query = "SELECT COUNT(*) as count FROM %(table_name)s"
        result = query_to_df(query, {"table_name": table_name})
        return result.iloc[0]["count"] if not result.empty else 0

    @staticmethod
    def get_sample_data(table_name: str, limit: int = 5) -> pd.DataFrame:
        """
        Get a sample of rows from a table.
        
        Args:
            table_name (str): Name of the table
            limit (int): Number of rows to return
            
        Returns:
            pd.DataFrame: Sample data
        """
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        return query_to_df(query)

    @staticmethod
    def get_column_stats(table_name: str, column_name: str) -> pd.DataFrame:
        """
        Get basic statistics for a numeric column.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column
            
        Returns:
            pd.DataFrame: Column statistics
        """
        query = f"""
        SELECT 
            COUNT(*) as count,
            AVG({column_name}) as mean,
            MIN({column_name}) as min,
            MAX({column_name}) as max,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY {column_name}) as median
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        """
        return query_to_df(query)

    @staticmethod
    def get_null_counts(table_name: str) -> pd.DataFrame:
        """
        Get count of NULL values for each column in a table.
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            pd.DataFrame: NULL counts by column
        """
        query = f"""
        SELECT 
            column_name,
            COUNT(*) - COUNT(column_name) as null_count,
            ROUND(100.0 * (COUNT(*) - COUNT(column_name)) / COUNT(*), 2) as null_percentage
        FROM information_schema.columns
        CROSS JOIN {table_name}
        WHERE table_name = '{table_name}'
        GROUP BY column_name
        ORDER BY null_count DESC;
        """
        return query_to_df(query)

    @staticmethod
    def get_distinct_values(table_name: str, column_name: str, limit: int = 10) -> pd.DataFrame:
        """
        Get distinct values and their counts for a column.
        
        Args:
            table_name (str): Name of the table
            column_name (str): Name of the column
            limit (int): Maximum number of distinct values to return
            
        Returns:
            pd.DataFrame: Distinct values and their counts
        """
        query = f"""
        SELECT 
            {column_name},
            COUNT(*) as count,
            ROUND(100.0 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) as percentage
        FROM {table_name}
        WHERE {column_name} IS NOT NULL
        GROUP BY {column_name}
        ORDER BY count DESC
        LIMIT {limit}
        """
        return query_to_df(query)

# Create a default instance
sql = SQLQueries() 