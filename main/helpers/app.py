import streamlit as st
from streamlit_option_menu import option_menu
import pandas as pd
from db_connection import DatabaseConnection
import contextlib

# Page config
st.set_page_config(
    page_title="EBS Data Explorer",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# Custom CSS
st.markdown("""
<style>
    /* Hide sidebar */
    [data-testid="stSidebar"] {
        display: none;
    }
    /* Full width container */
    .stApp {
        max-width: 100%;
    }
    .main > div {
        padding: 1rem;
    }
    /* Improve option menu styling */
    .stOptionMenu {
        background-color: #262730;
        border-radius: 0.5rem;
        padding: 0.5rem;
        margin-bottom: 1rem;
    }
    /* Style metrics */
    [data-testid="stMetricValue"] {
        font-size: 2rem;
    }
    /* Style dataframes */
    .stDataFrame {
        border: 1px solid #4a4a4a;
        border-radius: 0.5rem;
    }
    /* Improve header spacing */
    h1 {
        margin-bottom: 1rem;
    }
    h2 {
        margin: 1rem 0;
    }
    /* Improve expander styling */
    .streamlit-expanderHeader {
        font-size: 1rem;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)

def main():
    st.title("Database Explorer")
    
    # Main navigation
    explorer_view = option_menu(
        menu_title=None,
        options=["Quick Overview", "Data Explorer", "Advanced Analysis"],
        icons=["speedometer2", "table", "graph-up"],
        orientation="horizontal",
        styles={
            "container": {"padding": "0!important", "background-color": "#262730"},
            "icon": {"color": "#fafafa", "font-size": "16px"},
            "nav-link": {
                "font-size": "16px",
                "text-align": "center",
                "margin": "0px",
                "--hover-color": "#363945",
                "padding": "1rem",
            },
            "nav-link-selected": {"background-color": "#1f77b4"},
        }
    )
    
    # Create database connection
    db = DatabaseConnection()
    
    if explorer_view == "Quick Overview":
        try:
            with db.get_connection() as conn:
                # Show table counts with metrics
                table_counts = []
                for table_name in ['ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS', 'WORKERS']:
                    query = f"SELECT COUNT(*) as count FROM {table_name}"
                    cursor = conn.cursor()
                    cursor.execute(query)
                    count = cursor.fetchone()[0]
                    cursor.close()
                    table_counts.append({
                        'table_name': table_name,
                        'record_count': count
                    })
                
                table_counts = pd.DataFrame(table_counts)
                
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
                st.subheader("Quick Data Preview")
                for table_name in ['WORKERS', 'ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS']:
                    with st.expander(f"Preview: {table_name}", expanded=False):
                        query = f"SELECT * FROM {table_name} WHERE ROWNUM <= 3"
                        preview_df = pd.read_sql(query, conn)
                        if not preview_df.empty:
                            st.dataframe(
                                preview_df,
                                use_container_width=True,
                                hide_index=True
                            )
        except Exception as e:
            st.error(f"Error accessing database: {str(e)}")
    
    elif explorer_view == "Data Explorer":
        col1, col2 = st.columns([1, 3])
        with col1:
            selected_table = st.selectbox(
                "Select Table",
                ['WORKERS', 'ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS']
            )
            preview_rows = st.slider("Number of rows", 5, 50, 10)
            
            if selected_table:
                try:
                    with db.get_connection() as conn:
                        # Get column information
                        column_query = f"""
                            SELECT column_name, data_type, nullable
                            FROM user_tab_columns
                            WHERE table_name = '{selected_table}'
                            ORDER BY column_id
                        """
                        column_info = pd.read_sql(column_query, conn)
                        
                        with st.expander("Column Details", expanded=True):
                            if not column_info.empty:
                                st.dataframe(
                                    column_info,
                                    use_container_width=True,
                                    hide_index=True
                                )
                except Exception as e:
                    st.error(f"Error getting column information: {str(e)}")
        
        with col2:
            if selected_table:
                try:
                    with db.get_connection() as conn:
                        # Get preview data
                        preview_query = f"SELECT * FROM {selected_table} WHERE ROWNUM <= {preview_rows}"
                        preview_df = pd.read_sql(preview_query, conn)
                        
                        if not preview_df.empty:
                            st.dataframe(
                                preview_df,
                                use_container_width=True,
                                hide_index=True
                            )
                        
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
                            search_results = pd.read_sql(search_query, conn)
                            if not search_results.empty:
                                st.dataframe(
                                    search_results,
                                    use_container_width=True,
                                    hide_index=True
                                )
                            else:
                                st.info("No results found.")
                except Exception as e:
                    st.error(f"Error accessing data: {str(e)}")
    
    else:  # Advanced Analysis
        col1, col2 = st.columns([1, 3])
        with col1:
            selected_table = st.selectbox(
                "Select Table for Analysis",
                ['WORKERS', 'ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS']
            )
        
        if selected_table:
            with col2:
                try:
                    with db.get_connection() as conn:
                        # Get statistics
                        stats_query = f"SELECT COUNT(*) FROM {selected_table}"
                        cursor = conn.cursor()
                        cursor.execute(stats_query)
                        total_records = cursor.fetchone()[0]
                        cursor.close()
                        
                        st.metric("Total Records", total_records)
                        
                        # Table-specific visualizations
                        if selected_table == 'WORKERS':
                            st.subheader("Gender Distribution")
                            gender_query = "SELECT sex, COUNT(*) as count FROM workers GROUP BY sex"
                            cursor = conn.cursor()
                            cursor.execute(gender_query)
                            results = cursor.fetchall()
                            cursor.close()
                            
                            gender_data = {
                                'sex': [r[0] for r in results],
                                'count': [r[1] for r in results]
                            }
                            gender_df = pd.DataFrame(gender_data)
                            st.bar_chart(gender_df.set_index('sex'))
                        
                        elif selected_table == 'ADDRESSES':
                            st.subheader("Country Distribution")
                            country_query = "SELECT country, COUNT(*) as count FROM addresses GROUP BY country"
                            cursor = conn.cursor()
                            cursor.execute(country_query)
                            results = cursor.fetchall()
                            cursor.close()
                            
                            country_data = {
                                'country': [r[0] for r in results],
                                'count': [r[1] for r in results]
                            }
                            country_df = pd.DataFrame(country_data)
                            st.bar_chart(country_df.set_index('country'))
                        
                        elif selected_table == 'COMMUNICATIONS':
                            st.subheader("Communication Types")
                            type_query = "SELECT contact_type, COUNT(*) as count FROM communications GROUP BY contact_type"
                            cursor = conn.cursor()
                            cursor.execute(type_query)
                            results = cursor.fetchall()
                            cursor.close()
                            
                            type_data = {
                                'contact_type': [r[0] for r in results],
                                'count': [r[1] for r in results]
                            }
                            type_df = pd.DataFrame(type_data)
                            st.bar_chart(type_df.set_index('contact_type'))
                except Exception as e:
                    st.error(f"Error analyzing data: {str(e)}")

def show_data_assistant():
    st.title("Data Assistant")
    
    # Initialize chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    # Display chat messages
    for message in st.session_state.messages:
        with st.chat_message(message["role"]):
            st.markdown(message["content"])
    
    # Chat input
    if prompt := st.chat_input("Ask me about your EBS data..."):
        st.session_state.messages.append({"role": "user", "content": prompt})
        with st.chat_message("user"):
            st.markdown(prompt)
        
        db = DatabaseConnection()
        with db.get_connection() as conn:
            if "table" in prompt.lower():
                table_name = None
                for table in ['ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS', 'WORKERS']:
                    if table.lower() in prompt.lower():
                        table_name = table
                        break
                
                if table_name:
                    column_query = f"""
                        SELECT column_name, data_type, nullable
                        FROM user_tab_columns
                        WHERE table_name = '{table_name}'
                        ORDER BY column_id
                    """
                    table_info = pd.read_sql(column_query, conn)
                    if not table_info.empty:
                        response = f"Here's information about the {table_name} table:\n\n"
                        response += "Columns:\n"
                        for _, row in table_info.iterrows():
                            response += f"- {row['column_name']} ({row['data_type']})\n"
                    else:
                        response = f"Error getting information about {table_name}"
                else:
                    response = "I couldn't identify which table you're asking about. Please specify one of: ADDRESSES, ASSIGNMENTS, COMMUNICATIONS, or WORKERS."
            
            elif "sample" in prompt.lower() or "show" in prompt.lower():
                table_name = None
                for table in ['ADDRESSES', 'ASSIGNMENTS', 'COMMUNICATIONS', 'WORKERS']:
                    if table.lower() in prompt.lower():
                        table_name = table
                        break
                
                if table_name:
                    preview_query = f"SELECT * FROM {table_name} WHERE ROWNUM <= 5"
                    sample_data = pd.read_sql(preview_query, conn)
                    if not sample_data.empty:
                        response = f"Here's a sample of data from the {table_name} table:\n\n"
                        response += sample_data.to_string()
                    else:
                        response = f"Error getting sample data from {table_name}"
                else:
                    response = "I couldn't identify which table you're asking about. Please specify one of: ADDRESSES, ASSIGNMENTS, COMMUNICATIONS, or WORKERS."
            
            else:
                response = "I can help you with:\n1. Table information (e.g., 'Tell me about the WORKERS table')\n2. Sample data (e.g., 'Show me sample data from ADDRESSES')\n\nPlease try asking about specific tables or data."
        
        st.session_state.messages.append({"role": "assistant", "content": response})
        with st.chat_message("assistant"):
            st.markdown(response)
    
    # Add helpful suggestions in the sidebar
    st.sidebar.header("Try asking about:")
    st.sidebar.markdown("""
    - Tell me about the WORKERS table
    - Show me sample data from ADDRESSES
    - What columns are in the COMMUNICATIONS table?
    - How many records are in ASSIGNMENTS?
    """)

def show_account_management():
    st.title("Account Management")
    
    profile_col, settings_col = st.columns(2)
    
    db = DatabaseConnection()
    with profile_col:
        st.subheader("Profile Details")
        
        with db.get_connection() as conn:
            current_username = st.session_state.get('username', 'default_user')
            user_query = f"SELECT * FROM users WHERE username = '{current_username}'"
            user_profile = pd.read_sql(user_query, conn)
            
            if not user_profile.empty:
                profile = user_profile.iloc[0]
                first_name = st.text_input("First Name", value=profile.get('first_name', ''))
                last_name = st.text_input("Last Name", value=profile.get('last_name', ''))
                email = st.text_input("Email", value=profile.get('email', ''))
                username = st.text_input("Username", value=current_username, disabled=True)
            else:
                first_name = st.text_input("First Name")
                last_name = st.text_input("Last Name")
                email = st.text_input("Email")
                username = st.text_input("Username", value=current_username, disabled=True)
    
    with settings_col:
        st.subheader("Account Settings")
        current_password = st.text_input("Current Password", type="password")
        new_password = st.text_input("New Password", type="password")
        confirm_password = st.text_input("Confirm New Password", type="password")
    
    st.subheader("Preferences")
    email_notifications = st.checkbox("Receive Email Notifications")
    newsletter = st.checkbox("Subscribe to Newsletter")
    
    if st.button("Save Changes"):
        if new_password and new_password != confirm_password:
            st.error("New passwords do not match!")
        else:
            with db.get_connection() as conn:
                try:
                    update_query = """
                        UPDATE users 
                        SET first_name = :1,
                            last_name = :2,
                            email = :3,
                            email_notifications = :4,
                            newsletter = :5
                        WHERE username = :6
                    """
                    params = [
                        first_name,
                        last_name,
                        email,
                        email_notifications,
                        newsletter,
                        current_username
                    ]
                    
                    if new_password:
                        update_query = update_query.replace("newsletter = :5", "newsletter = :5, password = :7")
                        params.append(new_password)
                    
                    with conn.cursor() as cursor:
                        cursor.execute(update_query, params)
                        conn.commit()
                    
                    st.success("Changes saved successfully!")
                except Exception as e:
                    st.error(f"Failed to save changes: {str(e)}")

if __name__ == "__main__":
    main() 