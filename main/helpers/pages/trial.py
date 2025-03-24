import streamlit as st
import pandas as pd
from db_connection import get_connection

def trial_page():
    st.title("Try It Out")
    
    # Database Connection
    try:
        connection = get_connection()
        
        # Table Selection
        st.header("Explore Your Data")
        tables_query = """
            SELECT table_name 
            FROM user_tables 
            WHERE table_name IN ('ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS', 'WORKERS')
            ORDER BY table_name
        """
        tables_df = pd.read_sql(tables_query, connection.engine)
        selected_table = st.selectbox("Select a table to explore:", tables_df['table_name'])
        
        if selected_table:
            # Get table preview
            preview_query = f"SELECT * FROM {selected_table} WHERE ROWNUM <= 5"
            preview_df = pd.read_sql(preview_query, connection.engine)
            
            st.subheader("Data Preview")
            st.dataframe(preview_df)
            
            # Get table statistics
            stats_query = f"""
                SELECT 
                    COUNT(*) as total_records,
                    COUNT(DISTINCT worker_unique_id) as unique_workers
                FROM {selected_table}
            """
            stats_df = pd.read_sql(stats_query, connection.engine)
            
            st.subheader("Table Statistics")
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Total Records", stats_df['total_records'].iloc[0])
            with col2:
                st.metric("Unique Workers", stats_df['unique_workers'].iloc[0])
            
            # Data Visualization
            st.subheader("Data Visualization")
            if selected_table == 'WORKERS':
                # Gender distribution
                gender_query = "SELECT sex, COUNT(*) as count FROM workers GROUP BY sex"
                gender_df = pd.read_sql(gender_query, connection.engine)
                st.bar_chart(gender_df.set_index('sex'))
            
            elif selected_table == 'ADDRESSES':
                # Country distribution
                country_query = "SELECT country, COUNT(*) as count FROM addresses GROUP BY country"
                country_df = pd.read_sql(country_query, connection.engine)
                st.bar_chart(country_df.set_index('country'))
    
    except Exception as e:
        st.error(f"Error connecting to database: {str(e)}")
    
    # Feature Demo
    st.header("Feature Demo")
    with st.expander("Data Filtering"):
        st.write("""
        Try our advanced filtering capabilities:
        - Filter by date ranges
        - Search by specific values
        - Combine multiple conditions
        """)
    
    with st.expander("Data Export"):
        st.write("""
        Export your data in various formats:
        - CSV
        - Excel
        - PDF reports
        """)
    
    with st.expander("Custom Queries"):
        st.write("""
        Create and save custom queries:
        - Build complex queries
        - Save for future use
        - Share with team members
        """) 