import streamlit as st
import pandas as pd
from sqlalchemy import create_engine, inspect
from db_connection import get_connection, DatabaseConnection

def get_table_counts(connection):
    """Get the count of records for each table in the database."""
    try:
        # Debug: Show all available tables
        debug_query = """
            SELECT table_name 
            FROM user_tables 
            ORDER BY table_name
        """
        debug_df = pd.read_sql(debug_query, connection.engine)
        st.write("Debug: All available tables in the database:")
        st.dataframe(debug_df)
        
        # Get list of tables (Oracle specific)
        tables_query = """
            SELECT table_name 
            FROM user_tables 
            WHERE table_name IN ('ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS', 'WORKERS')
            ORDER BY table_name
        """
        tables_df = pd.read_sql(tables_query, connection.engine)
        table_counts = {}
        
        for table_name in tables_df['table_name']:
            try:
                # Get count for each table
                count_query = f"SELECT COUNT(*) as count FROM {table_name}"
                count_df = pd.read_sql(count_query, connection.engine)
                table_counts[table_name] = count_df['count'].iloc[0]
            except Exception as e:
                st.warning(f"Could not get count for table {table_name}: {str(e)}")
                table_counts[table_name] = 0
        
        return table_counts
    except Exception as e:
        st.error(f"Error getting table list: {str(e)}")
        return {}

def get_table_preview(connection, table_name, limit=5):
    """Get a preview of the selected table."""
    try:
        query = f"SELECT * FROM {table_name} WHERE ROWNUM <= {limit}"
        return pd.read_sql(query, connection.engine)
    except Exception as e:
        st.error(f"Error fetching preview for table {table_name}: {str(e)}")
        return None

def get_column_info(connection, table_name):
    """Get column information for a table."""
    try:
        query = """
            SELECT column_name, data_type 
            FROM user_tab_columns 
            WHERE table_name = :table_name
            ORDER BY column_id
        """
        return pd.read_sql(query, connection.engine, params={'table_name': table_name})
    except Exception as e:
        st.error(f"Error getting column information: {str(e)}")
        return None

def main():
    st.title("Database Table Explorer")
    
    try:
        # Get database connection using existing parameters
        connection = get_connection()
        
        # Get table counts
        table_counts = get_table_counts(connection)
        
        # Display table counts
        st.header("Table Counts")
        counts_df = pd.DataFrame(list(table_counts.items()), columns=['Table Name', 'Record Count'])
        st.dataframe(counts_df)
        
        # Table selection
        st.header("Table Preview")
        selected_table = st.selectbox("Select a table to preview:", list(table_counts.keys()))
        
        if selected_table:
            preview_df = get_table_preview(connection, selected_table)
            if preview_df is not None:
                st.write(f"Preview of {selected_table} (first 5 rows):")
                st.dataframe(preview_df)
                
                # Show table information
                columns_df = get_column_info(connection, selected_table)
                if columns_df is not None:
                    st.write("Column Information:")
                    for _, row in columns_df.iterrows():
                        st.write(f"- {row['column_name']}: {row['data_type']}")
        
    except Exception as e:
        st.error(f"Error connecting to database: {str(e)}")

if __name__ == "__main__":
    main()
