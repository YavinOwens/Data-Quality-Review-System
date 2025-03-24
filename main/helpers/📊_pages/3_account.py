import streamlit as st
import pandas as pd
from db_connection import DatabaseConnection
import contextlib

@contextlib.contextmanager
def managed_db_connection():
    """Context manager for database connections."""
    db = DatabaseConnection()
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

def get_user_profile(connection, username):
    """Get user profile from database."""
    try:
        query = "SELECT * FROM USERS WHERE username = :username"
        return pd.read_sql(query, connection.engine, params={'username': username})
    except Exception as e:
        st.error(f"Error fetching user profile: {str(e)}")
        return None

def update_user_profile(connection, username, updates):
    """Update user profile in database."""
    try:
        # Construct UPDATE query based on provided updates
        set_clauses = [f"{key} = :{key}" for key in updates.keys()]
        query = f"UPDATE USERS SET {', '.join(set_clauses)} WHERE username = :username"
        params = {**updates, 'username': username}
        
        with connection.engine.begin() as trans:
            trans.execute(query, params)
        return True
    except Exception as e:
        st.error(f"Error updating profile: {str(e)}")
        return False

st.title("Account Management")

# Create two columns for the layout
profile_col, settings_col = st.columns(2)

with profile_col:
    st.subheader("Profile Details")
    
    # Use managed database connection to get user profile
    with managed_db_connection() as connection:
        # Assuming we have the current username in session state
        current_username = st.session_state.get('username', 'default_user')
        user_profile = get_user_profile(connection, current_username)
        
        if user_profile is not None and not user_profile.empty:
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
        with managed_db_connection() as connection:
            updates = {
                'first_name': first_name,
                'last_name': last_name,
                'email': email,
                'email_notifications': email_notifications,
                'newsletter': newsletter
            }
            
            if new_password:
                # In a real app, you would verify the current password
                # and hash the new password before storing
                updates['password'] = new_password
            
            if update_user_profile(connection, current_username, updates):
                st.success("Changes saved successfully!")
            else:
                st.error("Failed to save changes. Please try again.") 