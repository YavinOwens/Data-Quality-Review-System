import streamlit as st
import pandas as pd
from db_connection import DatabaseConnection
import contextlib

# Cache the database connection
@st.cache_resource
def get_database_connection():
    """Create and cache the database connection."""
    return DatabaseConnection()

@contextlib.contextmanager
def managed_db_connection():
    """Context manager for database connections."""
    db = get_database_connection()
    try:
        connection = db.get_connection()
        yield connection
    except Exception as e:
        st.error(f"Database connection error: {str(e)}")
        raise
    finally:
        try:
            connection.close()
        except:
            pass

# Cache the table counts query
@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_table_counts(connection):
    """Get record counts for all tables."""
    try:
        query = """
            SELECT table_name, 
                   (SELECT COUNT(*) FROM user_tables t2 WHERE t2.table_name = t1.table_name) as record_count
            FROM user_tables t1
            WHERE table_name IN ('ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS', 'WORKERS')
            ORDER BY table_name
        """
        return pd.read_sql(query, connection.engine)
    except Exception as e:
        st.error(f"Error getting table counts: {str(e)}")
        return pd.DataFrame()

# Cache the table preview query
@st.cache_data(ttl=60)  # Cache for 1 minute
def get_table_preview(connection, table_name, limit=5):
    """Get a preview of data from a specific table."""
    try:
        query = f"SELECT * FROM {table_name} WHERE ROWNUM <= {limit}"
        return pd.read_sql(query, connection.engine)
    except Exception as e:
        st.error(f"Error getting table preview: {str(e)}")
        return pd.DataFrame()

def get_column_info(connection, table_name):
    """Get information about columns in a table."""
    try:
        query = f"""
            SELECT column_name, data_type, nullable, data_length, data_precision, data_scale
            FROM user_tab_columns
            WHERE table_name = '{table_name}'
            ORDER BY column_id
        """
        return pd.read_sql(query, connection.engine)
    except Exception as e:
        st.error(f"Error getting column info: {str(e)}")
        return pd.DataFrame()

def main():
    st.title("Database Explorer")
    
    try:
        with managed_db_connection() as connection:
            # Sidebar navigation
            st.sidebar.title("Navigation")
            view_mode = st.sidebar.radio(
                "Select View",
                ["Quick Overview", "Data Explorer", "Advanced Analysis"]
            )
            
            if view_mode == "Quick Overview":
                st.header("Database Overview")
                
                # Show table counts with metrics
                table_counts = get_table_counts(connection)
                if not table_counts.empty:
                    cols = st.columns(len(table_counts))
                    for idx, (_, row) in enumerate(table_counts.iterrows()):
                        with cols[idx]:
                            st.metric(
                                row['table_name'], 
                                row['record_count'],
                                help=f"Total records in {row['table_name']}"
                            )
                
                # Quick preview of all tables
                st.header("Quick Data Preview")
                for table_name in ['WORKERS', 'ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS']:
                    with st.expander(f"Preview: {table_name}"):
                        preview_df = get_table_preview(connection, table_name, 3)
                        if not preview_df.empty:
                            st.dataframe(preview_df, use_container_width=True)
            
            elif view_mode == "Data Explorer":
                st.header("Data Explorer")
                
                # Table selection
                selected_table = st.selectbox(
                    "Select Table",
                    ['WORKERS', 'ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS']
                )
                
                if selected_table:
                    # Show table preview with row count selector
                    preview_rows = st.slider("Number of rows to preview", 5, 50, 10)
                    preview_df = get_table_preview(connection, selected_table, preview_rows)
                    if not preview_df.empty:
                        st.dataframe(preview_df, use_container_width=True)
                    
                    # Column information
                    with st.expander("Column Details"):
                        column_info = get_column_info(connection, selected_table)
                        if not column_info.empty:
                            st.dataframe(column_info, use_container_width=True)
                    
                    # Search functionality
                    st.subheader("Search Data")
                    search_col1, search_col2 = st.columns([1, 2])
                    with search_col1:
                        search_column = st.selectbox(
                            "Search Column",
                            preview_df.columns.tolist()
                        )
                    with search_col2:
                        search_term = st.text_input("Search Term")
                    
                    if search_term:
                        search_query = f"""
                            SELECT *
                            FROM {selected_table}
                            WHERE LOWER({search_column}) LIKE LOWER('%{search_term}%')
                            AND ROWNUM <= 50
                        """
                        search_results = pd.read_sql(search_query, connection.engine)
                        if not search_results.empty:
                            st.dataframe(search_results, use_container_width=True)
                        else:
                            st.info("No results found.")
            
            else:  # Advanced Analysis
                st.header("Advanced Analysis")
                
                # Table selection for analysis
                selected_table = st.selectbox(
                    "Select Table for Analysis",
                    ['WORKERS', 'ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS']
                )
                
                if selected_table:
                    # Get table statistics
                    stats_query = f"""
                        SELECT 
                            COUNT(*) as total_records,
                            COUNT(DISTINCT worker_unique_id) as unique_workers
                        FROM {selected_table}
                    """
                    stats_df = pd.read_sql(stats_query, connection.engine)
                    
                    # Display statistics
                    col1, col2 = st.columns(2)
                    with col1:
                        st.metric("Total Records", stats_df['total_records'].iloc[0])
                    with col2:
                        st.metric("Unique Workers", stats_df['unique_workers'].iloc[0])
                    
                    # Table-specific visualizations
                    if selected_table == 'WORKERS':
                        st.subheader("Gender Distribution")
                        gender_query = "SELECT sex, COUNT(*) as count FROM workers GROUP BY sex"
                        gender_df = pd.read_sql(gender_query, connection.engine)
                        st.bar_chart(gender_df.set_index('sex'))
                    
                    elif selected_table == 'ADDRESSES':
                        st.subheader("Country Distribution")
                        country_query = "SELECT country, COUNT(*) as count FROM addresses GROUP BY country"
                        country_df = pd.read_sql(country_query, connection.engine)
                        st.bar_chart(country_df.set_index('country'))
                    
                    elif selected_table == 'COMMUNICATIONS':
                        st.subheader("Communication Types")
                        type_query = "SELECT contact_type, COUNT(*) as count FROM communications GROUP BY contact_type"
                        type_df = pd.read_sql(type_query, connection.engine)
                        st.bar_chart(type_df.set_index('contact_type'))
            
            # Export functionality in sidebar
            st.sidebar.header("Export Options")
            if st.sidebar.button("Export Current View"):
                try:
                    if 'selected_table' in locals():
                        query = f"SELECT * FROM {selected_table}"
                        export_df = pd.read_sql(query, connection.engine)
                        csv = export_df.to_csv(index=False)
                        st.sidebar.download_button(
                            label="Download CSV",
                            data=csv,
                            file_name=f"{selected_table.lower()}_export.csv",
                            mime="text/csv"
                        )
                except Exception as e:
                    st.sidebar.error(f"Error exporting data: {str(e)}")
            
            # Help information in sidebar
            with st.sidebar.expander("Table Information"):
                st.markdown("""
                ### Available Tables:
                - **WORKERS**: Worker personal information
                - **ADDRESSES**: Worker address information
                - **ASSIGNMENTS**: Worker assignment details
                - **COMMUNICATIONS**: Worker contact information
                """)
    
    except Exception as e:
        st.error(f"Error: {str(e)}")

if __name__ == "__main__":
    main() 