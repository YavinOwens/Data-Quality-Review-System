import streamlit as st
import pandas as pd
from db_connection import get_connection

def get_table_info(table_name, connection):
    """Get information about a specific table."""
    try:
        # Get column information
        columns_query = f"""
            SELECT column_name, data_type, nullable
            FROM user_tab_columns
            WHERE table_name = '{table_name}'
            ORDER BY column_id
        """
        columns_df = pd.read_sql(columns_query, connection.engine)
        
        # Get row count
        count_query = f"SELECT COUNT(*) as count FROM {table_name}"
        count_df = pd.read_sql(count_query, connection.engine)
        
        return {
            'columns': columns_df,
            'row_count': count_df['count'].iloc[0]
        }
    except Exception as e:
        return f"Error getting table info: {str(e)}"

def get_table_data(table_name, connection, limit=5):
    """Get sample data from a table."""
    try:
        query = f"SELECT * FROM {table_name} WHERE ROWNUM <= {limit}"
        return pd.read_sql(query, connection.engine)
    except Exception as e:
        return f"Error getting table data: {str(e)}"

def chatbot_page():
    st.title("EBS Data Assistant")
    
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask me about your EBS data..."):
        # Add user message to chat history
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        # Get database connection
        try:
            connection = get_connection()
            
            # Process the user's question
            if "table" in prompt.lower():
                # Extract table name from prompt
                table_name = None
                for table in ['ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS', 'WORKERS']:
                    if table.lower() in prompt.lower():
                        table_name = table
                        break
                
                if table_name:
                    # Get table information
                    table_info = get_table_info(table_name, connection)
                    if isinstance(table_info, dict):
                        response = f"Here's information about the {table_name} table:\n\n"
                        response += f"Total rows: {table_info['row_count']}\n\n"
                        response += "Columns:\n"
                        for _, row in table_info['columns'].iterrows():
                            response += f"- {row['column_name']} ({row['data_type']})\n"
                    else:
                        response = table_info
                else:
                    response = "I couldn't identify which table you're asking about. Please specify the table name (ADDRESSES, ASSIGNMENTS, COMMUNICATIONS, or WORKERS)."
            
            elif "sample" in prompt.lower() or "show" in prompt.lower():
                # Extract table name from prompt
                table_name = None
                for table in ['ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS', 'WORKERS']:
                    if table.lower() in prompt.lower():
                        table_name = table
                        break
                
                if table_name:
                    sample_data = get_table_data(table_name, connection)
                    if isinstance(sample_data, pd.DataFrame):
                        response = f"Here's a sample of data from the {table_name} table:\n\n"
                        response += sample_data.to_string()
                    else:
                        response = sample_data
                else:
                    response = "I couldn't identify which table you're asking about. Please specify the table name (ADDRESSES, ASSIGNMENTS, COMMUNICATIONS, or WORKERS)."
            
            else:
                response = "I can help you with:\n1. Table information (e.g., 'Tell me about the WORKERS table')\n2. Sample data (e.g., 'Show me sample data from ADDRESSES')\n\nPlease try asking about specific tables or data."
            
        except Exception as e:
            response = f"I encountered an error: {str(e)}"
        
        # Add assistant response to chat history
        st.session_state.messages.append({"role": "assistant", "content": response})
        with st.chat_message("assistant"):
            st.markdown(response)
    
    # Add some helpful suggestions
    st.sidebar.header("Try asking about:")
    st.sidebar.markdown("""
    - Tell me about the WORKERS table
    - Show me sample data from ADDRESSES
    - What columns are in the COMMUNICATIONS table?
    - How many records are in ASSIGNMENTS?
    """) 