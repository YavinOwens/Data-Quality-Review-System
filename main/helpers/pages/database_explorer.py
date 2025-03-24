import streamlit as st
import pandas as pd
from db_connection import DatabaseConnection

# Cache the database connection
@st.cache_resource
def get_database_connection():
    """Create and cache the database connection."""
    return DatabaseConnection()

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

# Cache the column info query
@st.cache_data(ttl=300)  # Cache for 5 minutes
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

def database_explorer_page():
    """Main function for the database explorer page."""
    st.title("Database Explorer")
    
    try:
        # Get cached database connection
        db = get_database_connection()
        connection = db.get_connection()
        
        # Initialize session state for selected table if not exists
        if 'selected_table' not in st.session_state:
            st.session_state.selected_table = 'WORKERS'
        
        # Sidebar navigation
        st.sidebar.title("Navigation")
        page = st.sidebar.radio(
            "Select View",
            ["Table Overview", "Data Explorer", "Search & Analytics"]
        )
        
        if page == "Table Overview":
            st.header("Table Overview")
            table_counts = get_table_counts(connection)
            if not table_counts.empty:
                # Format the display with a more appealing layout
                for _, row in table_counts.iterrows():
                    col1, col2 = st.columns([2, 1])
                    with col1:
                        st.subheader(row['table_name'])
                    with col2:
                        st.metric("Records", row['record_count'])
            
        elif page == "Data Explorer":
            st.header("Data Explorer")
            
            # Table selection in sidebar
            st.session_state.selected_table = st.sidebar.selectbox(
                "Select Table",
                ['WORKERS', 'ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS'],
                index=['WORKERS', 'ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS'].index(st.session_state.selected_table)
            )
            
            # Show table preview with expanded view option
            st.subheader(f"Preview: {st.session_state.selected_table}")
            preview_rows = st.slider("Number of rows to preview", 5, 50, 5)
            preview_df = get_table_preview(connection, st.session_state.selected_table, preview_rows)
            if not preview_df.empty:
                st.dataframe(preview_df, use_container_width=True)
            
            # Column information in expander
            with st.expander("View Column Information"):
                column_info = get_column_info(connection, st.session_state.selected_table)
                if not column_info.empty:
                    st.dataframe(column_info, use_container_width=True)
            
            # Statistics in columns
            st.subheader("Statistics")
            stats_query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT worker_unique_id) as unique_workers
                FROM {st.session_state.selected_table}
            """
            stats_df = pd.read_sql(stats_query, connection.engine)
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Records", stats_df['total_records'].iloc[0])
            with col2:
                st.metric("Unique Workers", stats_df['unique_workers'].iloc[0])
            
        else:  # Search & Analytics
            st.header("Search & Analytics")
            
            # Table selection for search
            selected_table = st.selectbox(
                "Select Table to Search",
                ['WORKERS', 'ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS']
            )
            
            # Get columns for selected table
            preview_df = get_table_preview(connection, selected_table)
            
            # Advanced search options
            col1, col2 = st.columns(2)
            with col1:
                search_column = st.selectbox(
                    "Select Column",
                    preview_df.columns.tolist()
                )
            with col2:
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
            
            # Visualizations based on table
            if selected_table == 'WORKERS':
                with st.expander("Gender Distribution"):
                    gender_query = "SELECT sex, COUNT(*) as count FROM workers GROUP BY sex"
                    gender_df = pd.read_sql(gender_query, connection.engine)
                    st.bar_chart(gender_df.set_index('sex'))
            
            elif selected_table == 'ADDRESSES':
                with st.expander("Country Distribution"):
                    country_query = "SELECT country, COUNT(*) as count FROM addresses GROUP BY country"
                    country_df = pd.read_sql(country_query, connection.engine)
                    st.bar_chart(country_df.set_index('country'))
    
    except Exception as e:
        st.error(f"Error connecting to database: {str(e)}")
    
    # Export functionality in sidebar
    st.sidebar.header("Export Options")
    if st.sidebar.button("Export Current Table"):
        try:
            query = f"SELECT * FROM {st.session_state.selected_table}"
            export_df = pd.read_sql(query, connection.engine)
            csv = export_df.to_csv(index=False)
            st.sidebar.download_button(
                label="Download CSV",
                data=csv,
                file_name=f"{st.session_state.selected_table.lower()}_export.csv",
                mime="text/csv"
            )
        except Exception as e:
            st.sidebar.error(f"Error exporting data: {str(e)}")
    
    # Table information in sidebar
    with st.sidebar.expander("Table Information"):
        st.markdown("""
        ### Available Tables:
        - **WORKERS**: Worker personal information
        - **ADDRESSES**: Worker address information
        - **ASSIGNMENTS**: Worker assignment details
        - **COMMUNICATIONS**: Worker contact information
        """) 